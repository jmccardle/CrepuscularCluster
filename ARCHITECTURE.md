# Architecture Overview

Deep-dive into CrepuscularCluster's design, concurrency model, and implementation details.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Cluster Server                          │
│                      (cluster_server.py)                        │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │                      WorkServer                           │ │
│  │                                                           │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │ │
│  │  │   Workers    │  │  Generators  │  │   Monitors   │   │ │
│  │  │ Dict[int,    │  │ Dict[int,    │  │ List[        │   │ │
│  │  │ ClientWorker]│  │ WebSocket]   │  │ WebSocket]   │   │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘   │ │
│  │                                                           │ │
│  │  ┌──────────────────────────────────────────────────┐    │ │
│  │  │          Queues: Dict[str, Queue]                │    │ │
│  │  │  "workload_1" → [Batch, Batch, Batch, ...]      │    │ │
│  │  │  "workload_2" → [Batch, Batch, ...]             │    │ │
│  │  └──────────────────────────────────────────────────┘    │ │
│  │                                                           │ │
│  │  ┌──────────────────────────────────────────────────┐    │ │
│  │  │         asyncio.Lock (protects all state)        │    │ │
│  │  └──────────────────────────────────────────────────┘    │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ▲
                              │ WebSocket connections
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
         ▼                    ▼                    ▼
    ┌────────┐          ┌──────────┐         ┌─────────┐
    │ Worker │          │Generator │         │ Monitor │
    │ Client │          │  Client  │         │ Client  │
    └────────┘          └──────────┘         └─────────┘
```

## Component Details

### WorkServer

Central coordinator managing all connections and state.

**Responsibilities:**
- Accept WebSocket connections
- Route messages to appropriate handlers
- Manage job queues per workload
- Track worker capacity and assignments
- Route results to correct generators
- Notify monitors of state changes

**Key Data Structures:**
```python
self.workers: Dict[int, ClientWorker]      # Active workers by ID
self.generators: Dict[int, WebSocket]      # Active generators by ID
self.monitors: List[WebSocket]             # Monitor connections
self.queues: Dict[str, queue.Queue]        # Job queues by workload
self.lock: asyncio.Lock                    # Protects all mutable state
```

### ClientWorker

Represents a connected worker client.

```python
@dataclass
class ClientWorker:
    worker_id: int              # Unique identifier
    cores: int                  # Total cores available
    websocket: WebSocket        # Connection handle
    workload: str              # Workload type
    leases: List[BatchInput]   # Currently assigned jobs
```

**Core Tracking:**
```python
def has_capacity(self, cores_needed: int) -> bool:
    used_cores = sum(batch.cores_required for batch in self.leases)
    return (used_cores + cores_needed) <= self.cores
```

### BatchInput

Represents a single job in the system.

```python
@dataclass
class BatchInput:
    args: List[str]              # Job arguments
    lease_state: str             # "new" → "leased" → "done"
    assigned_worker: Optional[int]  # Worker ID if leased
    cores_required: int          # Cores needed (default: 1)
    timestamp: float             # Last state change time
    generator_id: Optional[int]  # Which generator submitted this
```

**State Machine:**
```
    new ──────► leased ──────► done
     ▲            │
     │            │ (worker disconnect)
     └────────────┘
```

## Concurrency Model

### Async/Await Architecture

CrepuscularCluster uses Python's `asyncio` for concurrent I/O operations.

**Key Principles:**
1. **Single Event Loop** - All async operations run on one thread
2. **Non-blocking I/O** - WebSocket operations never block the loop
3. **Cooperative Multitasking** - Coroutines yield control at `await` points

**Example Flow:**
```python
async def register_worker(self, websocket, client_info):
    # Runs concurrently with other coroutines
    async with self.lock:  # Only one coroutine in critical section
        worker_id = self._worker_id
        self._worker_id += 1

    # I/O operations don't block other coroutines
    while True:
        message = await websocket.recv()  # Yields control while waiting
        # Process message...
```

### Lock Strategy

All shared state modifications are protected by a single `asyncio.Lock`:

```python
self.lock = asyncio.Lock()

# Pattern used throughout:
async with self.lock:
    # Critical section - exclusive access to:
    # - self.workers
    # - self.generators
    # - self.queues
    # - Worker.leases
```

**Why a single lock?**
- Prevents race conditions across data structures
- Simplifies reasoning about state consistency
- Fine-grained locking complexity not needed for this scale
- Lock is only held during fast in-memory operations

### Thread Safety in Workers

Worker clients use threads for subprocess execution but bridge to async safely:

```python
# worker_client.py
result_queue = Queue()  # Thread-safe queue

# Thread: Execute subprocess
def process_batch(args):
    proc = subprocess.Popen(...)
    stdout, stderr = proc.communicate()
    result_queue.put(result)  # Thread-safe

# Async: Send results
async def result_sender(websocket):
    while True:
        if not result_queue.empty():
            result = result_queue.get()
            await websocket.send(json.dumps(result))
        await asyncio.sleep(0.01)
```

**Why threads in workers?**
- `subprocess.Popen().communicate()` is blocking
- Keeps worker simple - no need for `asyncio.subprocess`
- Queue provides thread-safe bridge to async loop

## Data Flow

### Job Submission Flow

```
Generator                Server                   Worker
    │                      │                         │
    │ 1. Submit batches    │                         │
    ├─────────────────────>│                         │
    │  {generator: true,   │                         │
    │   batches: [...]}    │                         │
    │                      │                         │
    │                      │ 2. Queue batches        │
    │                      │    (with generator_id)  │
    │                      │                         │
    │                      │ 3. Distribute work      │
    │                      │                         │
    │                      │ 4. Check worker capacity│
    │                      ├────────────────────────>│
    │                      │   {batch: ["arg1"]}     │
    │                      │                         │
    │                      │ 5. Execute subprocess   │
    │                      │                         │
    │                      │ 6. Return result        │
    │                      │<────────────────────────┤
    │                      │   {stdout, stderr, ...} │
    │                      │                         │
    │ 7. Route to correct  │                         │
    │    generator         │                         │
    │<─────────────────────┤                         │
    │  {stdout, stderr...} │                         │
```

### Worker Capacity Management

```python
# Server assigns work based on available cores:

Worker has 4 cores:
  ┌─────┬─────┬─────┬─────┐
  │  1  │  2  │  3  │  4  │
  └─────┴─────┴─────┴─────┘

Assigned 2 single-core jobs:
  ┌─────┬─────┬─────┬─────┐
  │ Job1│ Job2│     │     │
  └─────┴─────┴─────┴─────┘
  Used: 2, Available: 2

Can accept another 1-core job:
  ┌─────┬─────┬─────┬─────┐
  │ Job1│ Job2│ Job3│     │
  └─────┴─────┴─────┴─────┘
  Used: 3, Available: 1

Cannot accept a 2-core job (only 1 available)
```

**Implementation:**
```python
def has_capacity(self, cores_needed: int) -> bool:
    used_cores = sum(batch.cores_required for batch in self.leases)
    return (used_cores + cores_needed) <= self.cores

# Assignment logic:
async def assign_work(self, worker_id):
    async with self.lock:
        while not queue_obj.empty():
            batch = queue_obj.get_nowait()
            if worker.has_capacity(batch.cores_required):
                # Assign batch
                worker.leases.append(batch)
                await worker.websocket.send(...)
            else:
                # Requeue for another worker
                requeue_list.append(batch)
```

## Fault Tolerance

### Worker Disconnection

When a worker disconnects, incomplete jobs are recovered:

```python
async def cleanup_disconnected_worker(self, worker_id):
    async with self.lock:
        worker = self.workers.pop(worker_id)

        # Requeue all assigned jobs
        for batch in worker.leases:
            batch.lease_state = "new"  # Reset state
            batch.assigned_worker = None
            self.queues[worker.workload].put(batch)
```

**Recovery Flow:**
```
Worker 1 disconnects with 3 jobs assigned
         ↓
Jobs marked as "new" and requeued
         ↓
Other workers receive the jobs
         ↓
Generator eventually receives all results
```

### Generator Disconnection

If a generator disconnects before receiving all results:

```python
# Server attempts to send result:
if generator_id in self.generators:
    await self.generators[generator_id].send(result)
else:
    # Generator disconnected - result is lost
    print(f"Generator {generator_id} disconnected")
```

**Current Limitation:**
- Results are not persisted
- If generator disconnects, results are lost
- Consider adding result persistence for production use

### Connection Monitoring

Monitors can track system health:

```python
# Monitor receives state updates:
{
    "workers": [
        {"worker_id": 0, "cores": 4, "leases": {...}},
        {"worker_id": 1, "cores": 8, "leases": {...}}
    ],
    "jobs_waiting": 42  # Jobs in queue
}

# Use for:
# - Alerting on worker failures
# - Scaling decisions
# - Job queue backlog monitoring
```

## Design Decisions

### Why WebSockets?

**Advantages:**
- Persistent bidirectional connections
- Push-based result delivery (no polling)
- Wide language support for clients
- Built-in ping/pong for connection health

**Alternatives Considered:**
- HTTP/REST: Requires polling for results
- Message Queue (RabbitMQ/Redis): Additional infrastructure
- gRPC: More complexity for simple use case

### Why Per-Workload Queues?

```python
self.queues: Dict[str, queue.Queue]
```

**Benefits:**
- Workload isolation
- Easy to add workload-specific policies
- Simple matching logic (string equality)

**Tradeoff:**
- Can't prioritize across workloads
- No global fair scheduling

### Why Single Lock?

**Simplicity over performance:**
- Critical sections are very short (microseconds)
- Contention is low (I/O bound, not CPU bound)
- Code is easier to reason about
- Premature optimization avoided

**When to reconsider:**
- Thousands of concurrent workers
- High job submission rate (>10k/sec)
- Lock contention shows up in profiling

### Why Track Generator ID?

Previous implementation had broadcast problem:

```python
# OLD: All generators got all results
while True:
    if workload in self.results:
        result = self.results[workload].pop(0)
        await websocket.send(result)  # Wrong generator!
```

**New: Track and route:**
```python
# NEW: Route to correct generator
batch.generator_id = generator_id
# ... later ...
await self.generators[batch.generator_id].send(result)
```

## Performance Characteristics

### Scalability Limits

**Workers:**
- Tested: ~100 concurrent workers
- Limited by: Network bandwidth, server CPU
- Bottleneck: Lock contention at very high scale

**Jobs:**
- Queue size: Limited by memory
- Job rate: ~1000 jobs/sec on modern hardware
- Bottleneck: WebSocket send/recv overhead

**Results:**
- Stored in memory temporarily
- Large results should use external storage

### Optimization Opportunities

**Current:**
- Single-threaded async server
- In-memory job queue
- No job persistence

**Future Improvements:**
1. **Job Persistence:** Save batches to disk/database
2. **Result Persistence:** Don't lose results on generator disconnect
3. **Multi-process Server:** Scale across CPU cores
4. **Job Prioritization:** Priority queues per workload
5. **Resource Limits:** Max queue size, result TTL
6. **Authentication:** Secure production deployments

## Code Organization

```
cluster_server.py
├── BatchInput (dataclass)         # Job representation
├── BatchOutput (dataclass)        # Result representation
├── ClientWorker (class)           # Worker state
├── WorkServer (class)
│   ├── __init__()                 # Initialize state
│   ├── handler()                  # WebSocket entry point
│   ├── register_worker()          # Worker lifecycle
│   ├── handle_generator()         # Generator lifecycle
│   ├── register_monitor()         # Monitor lifecycle
│   ├── submit_batches()           # Queue jobs
│   ├── distribute_work()          # Trigger assignment
│   ├── assign_work()              # Assign job to worker
│   ├── process_completed_job()    # Handle result, route to generator
│   ├── cleanup_disconnected_worker() # Handle worker failure
│   └── notify_monitors()          # Push state updates
└── main()                         # Server entry point
```

## Testing Considerations

**Unit Testing:**
```python
# Mock WebSocket connections
# Test state transitions
# Verify lock usage
# Test capacity calculations
```

**Integration Testing:**
```python
# Start server
# Connect multiple workers
# Submit batches via generator
# Verify correct routing
# Test disconnection scenarios
```

**Load Testing:**
```python
# 100+ workers
# 10k+ batches
# Measure throughput
# Check for race conditions
```

## See Also

- [API Documentation](API.md) - Protocol reference
- [Integration Guide](INTEGRATION.md) - Building custom clients
- [Examples](examples/) - Practical implementations
