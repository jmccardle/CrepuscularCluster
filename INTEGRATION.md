# Integration Guide

This guide shows how to integrate CrepuscularCluster into your own applications for distributed batch processing.

## Overview

CrepuscularCluster provides a flexible WebSocket-based architecture for distributing batch jobs. You can integrate it into your applications in several ways:

1. **Custom Generators** - Submit batches from your application logic
2. **Custom Workers** - Execute Python functions or custom logic instead of shell commands
3. **Embedded Server** - Run the cluster server within your application
4. **Monitor Integration** - Track job progress in your application

## Custom Generator Integration

### Minimal Example

Here's the simplest way to create a custom generator:

```python
import asyncio
import websockets
import json

async def my_generator(workload_name, batches):
    """
    Connect to CrepuscularCluster and submit batches.

    Args:
        workload_name: String identifier for this workload type
        batches: List of argument lists, e.g. [["arg1", "arg2"], ["arg3", "arg4"]]
    """
    uri = "ws://localhost:8765"

    async with websockets.connect(uri) as websocket:
        # Register and submit batches
        registration = {
            "generator": True,
            "workload": workload_name,
            "batches": batches
        }
        await websocket.send(json.dumps(registration))
        print(f"Submitted {len(batches)} batches to workload '{workload_name}'")

        # Receive results (pushed from server)
        results = []
        for i in range(len(batches)):
            response = json.loads(await websocket.recv())
            results.append(response)
            print(f"Received result {i+1}/{len(batches)}: exit code {response['exitcode']}")

        return results

# Usage
if __name__ == "__main__":
    batches = [
        ["input1.txt", "output1.txt"],
        ["input2.txt", "output2.txt"],
        ["input3.txt", "output3.txt"],
    ]

    results = asyncio.run(my_generator("data_processing", batches))

    # Process results
    for result in results:
        if result["exitcode"] == 0:
            print(f"Success: {result['stdout']}")
        else:
            print(f"Error: {result['stderr']}")
```

### Real-World Example: Batch Image Processing

```python
import asyncio
import websockets
import json
from pathlib import Path

class ImageBatchGenerator:
    """Distribute image processing tasks across workers."""

    def __init__(self, server_uri="ws://localhost:8765"):
        self.server_uri = server_uri

    async def process_images(self, image_dir, operation="resize", **kwargs):
        """
        Process all images in a directory using distributed workers.

        Args:
            image_dir: Path to directory containing images
            operation: Operation to perform (resize, convert, filter)
            **kwargs: Operation-specific parameters
        """
        # Build batch list from images
        image_path = Path(image_dir)
        batches = []

        for img_file in image_path.glob("*.jpg"):
            # Each batch is the arguments for the worker script
            batch_args = [
                operation,
                str(img_file),
                str(img_file.with_suffix('.processed.jpg')),
                json.dumps(kwargs)  # Pass parameters as JSON
            ]
            batches.append(batch_args)

        if not batches:
            print(f"No images found in {image_dir}")
            return []

        # Connect and submit
        async with websockets.connect(self.server_uri) as websocket:
            registration = {
                "generator": True,
                "workload": "image_processing",
                "batches": batches
            }
            await websocket.send(json.dumps(registration))
            print(f"Submitted {len(batches)} images for {operation}")

            # Collect results with progress tracking
            results = []
            failed_images = []

            for i in range(len(batches)):
                response = json.loads(await websocket.recv())
                results.append(response)

                if response["exitcode"] != 0:
                    failed_images.append(batches[i][1])
                    print(f"✗ Failed: {batches[i][1]} - {response['stderr']}")
                else:
                    print(f"✓ Processed ({i+1}/{len(batches)}): {batches[i][1]}")

            if failed_images:
                print(f"\n⚠ {len(failed_images)} images failed processing")

            return results

# Usage
async def main():
    generator = ImageBatchGenerator()

    # Resize all images to 800x600
    results = await generator.process_images(
        "/path/to/images",
        operation="resize",
        width=800,
        height=600
    )

    print(f"\nProcessed {len(results)} images")

if __name__ == "__main__":
    asyncio.run(main())
```

### Integrating into Existing Applications

If you have an existing async Python application, you can integrate the generator directly:

```python
class MyApplication:
    def __init__(self):
        self.cluster_uri = "ws://localhost:8765"

    async def distribute_work(self, job_data):
        """Submit work to cluster and await results."""
        # Convert your job data into batches
        batches = self._prepare_batches(job_data)

        # Submit to cluster
        async with websockets.connect(self.cluster_uri) as ws:
            await ws.send(json.dumps({
                "generator": True,
                "workload": "my_app_jobs",
                "batches": batches
            }))

            # Process results as they arrive
            results = []
            async for _ in range(len(batches)):
                result = json.loads(await ws.recv())
                await self._handle_result(result)
                results.append(result)

            return results

    def _prepare_batches(self, job_data):
        """Convert application data to batch format."""
        return [[str(job_id), json.dumps(job_params)]
                for job_id, job_params in job_data.items()]

    async def _handle_result(self, result):
        """Process individual results as they arrive."""
        # Update database, send notifications, etc.
        pass
```

## Custom Worker Integration

### Minimal Example

Create a worker that executes Python functions instead of subprocess commands:

```python
import asyncio
import websockets
import json
import sys

WORKLOAD_NAME = sys.argv[1] if len(sys.argv) > 1 else "python_functions"
NUM_CORES = 1

async def worker_client(handler_function):
    """
    Connect as a worker and execute a Python function for each batch.

    Args:
        handler_function: Callable that takes batch args and returns result dict
    """
    uri = "ws://localhost:8765"

    async with websockets.connect(uri) as websocket:
        # Register as worker
        registration = {"worker": WORKLOAD_NAME, "cores": NUM_CORES}
        await websocket.send(json.dumps(registration))
        print(f"Registered as worker for {WORKLOAD_NAME}")

        while True:
            # Receive batch assignment
            message = json.loads(await websocket.recv())

            if "batch" in message:
                args = message["batch"]
                print(f"Processing: {args}")

                try:
                    # Execute handler function
                    result = await handler_function(args)

                    # Send result back
                    response = {
                        "stdout": str(result.get("output", "")),
                        "stderr": str(result.get("error", "")),
                        "exitcode": result.get("exitcode", 0)
                    }
                except Exception as e:
                    response = {
                        "stdout": "",
                        "stderr": str(e),
                        "exitcode": 1
                    }

                await websocket.send(json.dumps(response))
                print(f"Completed: {args}")

# Example handler function
async def my_batch_handler(args):
    """Process a batch - replace with your logic."""
    # args is a list of strings from the generator
    input_file, output_file = args[0], args[1]

    # Do some work...
    await asyncio.sleep(0.1)  # Simulate processing

    return {
        "output": f"Processed {input_file} -> {output_file}",
        "error": "",
        "exitcode": 0
    }

if __name__ == "__main__":
    asyncio.run(worker_client(my_batch_handler))
```

### Advanced: Function Worker with Dynamic Imports

Execute arbitrary Python functions by importing them from module paths:

```python
import asyncio
import websockets
import json
import importlib
import sys
from queue import Queue
import threading

class FunctionWorker:
    """Worker that executes Python functions from module paths."""

    def __init__(self, workload_name, num_cores=4):
        self.workload_name = workload_name
        self.num_cores = num_cores
        self.result_queue = Queue()

    async def connect(self, server_uri="ws://localhost:8765"):
        """Connect to cluster and process batches."""
        async with websockets.connect(server_uri) as websocket:
            # Register
            await websocket.send(json.dumps({
                "worker": self.workload_name,
                "cores": self.num_cores
            }))
            print(f"Function worker registered: {self.num_cores} cores")

            # Start result sender task
            sender = asyncio.create_task(self._result_sender(websocket))

            try:
                while True:
                    message = json.loads(await websocket.recv())

                    if "batch" in message:
                        args = message["batch"]
                        # Execute in thread pool to avoid blocking
                        threading.Thread(
                            target=self._execute_function,
                            args=(args,),
                            daemon=True
                        ).start()
            finally:
                sender.cancel()

    async def _result_sender(self, websocket):
        """Send results from queue."""
        while True:
            if not self.result_queue.empty():
                result = self.result_queue.get()
                await websocket.send(json.dumps(result))
            await asyncio.sleep(0.01)

    def _execute_function(self, args):
        """Execute function from module path in thread."""
        try:
            # Args: [module_path, function_name, arg1, arg2, ...]
            module_path, func_name, *func_args = args

            # Import module and get function
            module = importlib.import_module(module_path)
            func = getattr(module, func_name)

            # Execute
            result = func(*func_args)

            self.result_queue.put({
                "stdout": json.dumps(result),
                "stderr": "",
                "exitcode": 0
            })

        except Exception as e:
            self.result_queue.put({
                "stdout": "",
                "stderr": str(e),
                "exitcode": 1
            })

# Usage
if __name__ == "__main__":
    worker = FunctionWorker("python_functions", num_cores=4)
    asyncio.run(worker.connect())
```

## Embedded Server

You can embed the cluster server in your application:

```python
import asyncio
from cluster_server import WorkServer
import websockets

class MyApp:
    def __init__(self):
        self.cluster = WorkServer()

    async def start(self):
        """Start application with embedded cluster server."""
        # Start cluster server
        server_task = asyncio.create_task(self._run_cluster())

        # Start your application logic
        app_task = asyncio.create_task(self._run_app())

        await asyncio.gather(server_task, app_task)

    async def _run_cluster(self):
        """Run embedded cluster server."""
        async with websockets.serve(self.cluster.handler, "localhost", 8765):
            print("Cluster server running on ws://localhost:8765")
            await asyncio.Future()  # Run forever

    async def _run_app(self):
        """Your application logic."""
        # Wait for server to start
        await asyncio.sleep(1)

        # Now you can submit jobs to your own embedded server
        # ... your app logic ...
        pass

if __name__ == "__main__":
    app = MyApp()
    asyncio.run(app.start())
```

## Best Practices

### Error Handling

Always handle connection errors and implement retry logic:

```python
async def robust_generator(batches, max_retries=3):
    """Generator with connection retry logic."""
    for attempt in range(max_retries):
        try:
            async with websockets.connect(
                "ws://localhost:8765",
                ping_interval=20,
                ping_timeout=10
            ) as ws:
                # ... submit and process ...
                return results

        except websockets.exceptions.ConnectionClosed:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # Exponential backoff
                print(f"Connection lost, retrying in {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise
```

### Resource Management

For workers with limited resources:

```python
# Declare cores appropriately
registration = {
    "worker": "heavy_computation",
    "cores": 8  # Reserve 8 cores for parallel processing
}

# Or use system detection
import os
cores = os.cpu_count() or 1
registration = {"worker": workload, "cores": cores}
```

### Connection Lifecycle

Keep connections alive for long-running generators:

```python
async with websockets.connect(
    uri,
    ping_interval=30,  # Send ping every 30s
    ping_timeout=10,   # Timeout after 10s
    max_size=10_000_000  # Increase if large results expected
) as websocket:
    # ... your code ...
```

### Monitoring Progress

Use the monitor client to track progress from your application:

```python
async def monitor_jobs(workload_name):
    """Monitor job progress."""
    async with websockets.connect("ws://localhost:8765") as ws:
        await ws.send(json.dumps({
            "monitor": True,
            "workload": workload_name
        }))

        while True:
            state = json.loads(await ws.recv())
            workers = state["workers"]
            waiting = state["jobs_waiting"]

            print(f"Workers: {len(workers)}, Jobs waiting: {waiting}")

            # Update your UI, metrics, etc.
```

## See Also

- [API Documentation](API.md) - WebSocket protocol reference
- [Architecture Overview](ARCHITECTURE.md) - Design deep-dive
- [Examples](examples/) - More integration examples
