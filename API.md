# API Documentation

Complete reference for the CrepuscularCluster WebSocket protocol.

## Connection

All clients connect to the WebSocket server:

```
ws://hostname:8765
```

Default: `ws://localhost:8765`

## Client Types

Three client types can connect to the server:

1. **Worker** - Executes batch jobs
2. **Generator** - Submits batches and receives results
3. **Monitor** - Observes system state

## Registration Protocol

Upon connection, clients must immediately send a registration message identifying their type.

### Worker Registration

```json
{
  "worker": "workload_name",
  "cores": 4
}
```

**Fields:**
- `worker` (string, required): Workload identifier
- `cores` (integer, required): Number of cores available

**Server Response:**
- Server sends job assignments as they become available
- No immediate response to registration

### Generator Registration

```json
{
  "generator": true,
  "workload": "workload_name",
  "batches": [
    ["arg1", "arg2"],
    ["arg3", "arg4", "arg5"],
    ["arg6"]
  ]
}
```

**Fields:**
- `generator` (boolean, required): Must be `true`
- `workload` (string, required): Workload identifier
- `batches` (array, required): List of argument arrays

**Server Response:**
- Server immediately queues all batches
- Results are pushed to generator as jobs complete
- Each batch generates one result message

### Monitor Registration

```json
{
  "monitor": true,
  "workload": "workload_name"
}
```

**Fields:**
- `monitor` (boolean, required): Must be `true`
- `workload` (string, required): Workload to monitor (currently unused)

**Server Response:**
- Server immediately sends current state
- State updates sent whenever system state changes

## Message Formats

### Job Assignment (Server → Worker)

```json
{
  "batch": ["arg1", "arg2", "arg3"]
}
```

**Fields:**
- `batch` (array): Arguments to pass to worker's executable

**Worker Action:**
- Execute: `<executable> arg1 arg2 arg3`
- Send result when complete

### Job Result (Worker → Server)

```json
{
  "stdout": "output text",
  "stderr": "error text",
  "exitcode": 0
}
```

**Fields:**
- `stdout` (string, required): Standard output from job
- `stderr` (string, required): Standard error from job
- `exitcode` (integer, required): Exit code (0 = success)

**Server Action:**
- Routes result to generator that submitted the batch
- Assigns new work to worker if available

### Batch Result (Server → Generator)

```json
{
  "stdout": "output text",
  "stderr": "error text",
  "exitcode": 0
}
```

**Fields:**
- Same as job result format
- Pushed when job completes
- One message per batch submitted

### System State (Server → Monitor)

```json
{
  "workers": [
    {
      "worker_id": 0,
      "cores": 4,
      "leases": {
        "cores_used": 2,
        "jobs_assigned": 2
      }
    },
    {
      "worker_id": 1,
      "cores": 8,
      "leases": {
        "cores_used": 0,
        "jobs_assigned": 0
      }
    }
  ],
  "jobs_waiting": 42
}
```

**Fields:**
- `workers` (array): Currently connected workers
  - `worker_id` (integer): Unique worker identifier
  - `cores` (integer): Total cores declared by worker
  - `leases` (object): Current assignments
    - `cores_used` (integer): Cores currently in use
    - `jobs_assigned` (integer): Number of active jobs
- `jobs_waiting` (integer): Total jobs in queue across all workloads

## Connection Lifecycle

### Worker Lifecycle

```
1. Connect to server
2. Send worker registration
3. Loop:
   a. Receive job assignment
   b. Execute job
   c. Send result
4. Disconnect (server reassigns incomplete jobs)
```

### Generator Lifecycle

```
1. Connect to server
2. Send generator registration with batches
3. Loop:
   a. Receive result (pushed from server)
   b. Process result
4. Disconnect when all results received
```

### Monitor Lifecycle

```
1. Connect to server
2. Send monitor registration
3. Receive initial state
4. Loop:
   a. Receive state updates
   b. Update UI/metrics
5. Disconnect when done monitoring
```

## Error Handling

### Connection Errors

**Client disconnection:**
- Workers: Incomplete jobs are requeued as "new"
- Generators: Results can no longer be delivered
- Monitors: Removed from notification list

**No error messages sent** - clients should handle:
- Connection timeouts
- Network failures
- Server restarts

### Job Failures

**Non-zero exit codes:**
- Still delivered to generator
- Generator decides how to handle failures
- No automatic retry by server

## Workload Matching

Workers and generators are matched by workload string:

```
Worker:    {"worker": "image_processing", ...}
Generator: {"workload": "image_processing", ...}
            ↑ These must match exactly
```

**Rules:**
- Case-sensitive matching
- Multiple workers per workload supported
- Multiple generators per workload supported
- Results routed to correct generator (by batch tracking)

## Core Management

Workers can declare multiple cores and receive parallel assignments:

### Single-core jobs

```python
# Worker declares 4 cores
{"worker": "workload", "cores": 4}

# Can receive up to 4 jobs simultaneously
# Each batch implicitly requires 1 core
```

### Multi-core jobs (future enhancement)

The `BatchInput` dataclass supports `cores_required` field:

```python
batch = BatchInput(args=["arg1"], cores_required=2)
```

Currently, all batches default to `cores_required=1`. Multi-core batch submission is not yet exposed in the client API but is supported by the server architecture.

## Best Practices

### Connection Management

**Use ping/pong:**
```python
websockets.connect(
    uri,
    ping_interval=20,  # Send ping every 20 seconds
    ping_timeout=10    # Timeout if no pong in 10 seconds
)
```

**Handle disconnections:**
```python
try:
    async with websockets.connect(uri) as ws:
        # ... use connection ...
except websockets.exceptions.ConnectionClosed:
    # Reconnect with exponential backoff
```

### Message Size

**Default limit:** 1 MB per message

**Large outputs:**
```python
websockets.connect(uri, max_size=10_000_000)  # 10 MB limit
```

**Recommendation:** For very large results, write to shared storage and return a reference:

```json
{
  "stdout": "s3://bucket/results/job123.json",
  "stderr": "",
  "exitcode": 0
}
```

### Batch Size

**Optimal batch submission:**
- Submit 100-1000 batches per connection
- For more batches, consider chunking:

```python
# Instead of one generator with 10,000 batches
# Use multiple connections with 1,000 batches each

async def submit_chunk(batches):
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({
            "generator": True,
            "workload": "my_workload",
            "batches": batches
        }))
        results = []
        for _ in range(len(batches)):
            results.append(json.loads(await ws.recv()))
        return results

# Process in chunks
all_results = []
chunk_size = 1000
for i in range(0, len(all_batches), chunk_size):
    chunk = all_batches[i:i+chunk_size]
    results = await submit_chunk(chunk)
    all_results.extend(results)
```

### JSON Encoding

**Arguments must be strings:**

```python
# Correct
batches = [["1", "100"], ["101", "200"]]

# Incorrect - will cause issues
batches = [[1, 100], [101, 200]]
```

**Complex data:** Encode as JSON strings:

```python
import json

config = {"resize": True, "width": 800}
batches = [
    ["image1.jpg", json.dumps(config)],
    ["image2.jpg", json.dumps(config)]
]
```

## Example Protocol Flows

### Successful Job Flow

```
Generator          Server          Worker
   |                 |                |
   |--register------>|                |
   |  (with batches) |                |
   |                 |<---register----|
   |                 |                |
   |                 |--assign job--->|
   |                 |                |
   |                 |<--job result---|
   |<--batch result--|                |
   |                 |                |
```

### Worker Disconnect Flow

```
Generator          Server          Worker
   |                 |                |
   |                 |--assign job--->|
   |                 |                |
   |                 |       X  (disconnects)
   |                 |
   |                 | (requeue job)
   |                 |
   |                 |<---register----| Worker 2
   |                 |--assign job--->|
   |<--batch result--|<--job result---|
   |                 |                |
```

## See Also

- [Integration Guide](INTEGRATION.md) - How to build custom clients
- [Architecture](ARCHITECTURE.md) - Design overview
