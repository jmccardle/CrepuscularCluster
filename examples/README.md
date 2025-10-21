# Examples

Practical examples demonstrating how to integrate CrepuscularCluster into your applications.

## Quick Start

1. Start the cluster server:
   ```bash
   python cluster_server.py
   ```

2. Run an example worker (in another terminal):
   ```bash
   python examples/function_worker.py
   ```

3. Run an example generator (in another terminal):
   ```bash
   python -c "
   import asyncio
   import websockets
   import json

   async def test():
       async with websockets.connect('ws://localhost:8765') as ws:
           await ws.send(json.dumps({
               'generator': True,
               'workload': 'python_functions',
               'batches': [
                   ['add', '10', '20'],
                   ['multiply', '5', '3'],
                   ['word_count', 'hello world from cluster']
               ]
           }))
           for _ in range(3):
               result = json.loads(await ws.recv())
               print(result)

   asyncio.run(test())
   "
   ```

## Example Files

### function_worker.py

A worker that executes Python functions instead of shell commands.

**Features:**
- Function registry with decorator-based registration
- JSON result serialization
- Error handling with stack traces
- Thread-based execution (non-blocking)

**Usage:**
```bash
python examples/function_worker.py [workload_name] [num_cores]
```

**Adding Custom Functions:**
```python
from examples.function_worker import register_function

@register_function("my_function")
def my_function(arg1: str, arg2: str):
    # Your logic here
    return {"result": "success"}
```

**Generator Integration:**
```python
import asyncio
import websockets
import json

async def call_functions():
    async with websockets.connect('ws://localhost:8765') as ws:
        await ws.send(json.dumps({
            'generator': True,
            'workload': 'python_functions',
            'batches': [
                ['add', '100', '200'],
                ['multiply', '7', '8'],
                ['process_text', 'Hello World', 'upper'],
                ['word_count', 'The quick brown fox jumps over the lazy dog']
            ]
        }))

        results = []
        for _ in range(4):
            result = json.loads(await ws.recv())
            results.append(json.loads(result['stdout']))

        return results

results = asyncio.run(call_functions())
for result in results:
    print(result)
```

### image_batch_generator.py

A generator for batch image processing tasks.

**Features:**
- Directory scanning for images
- Progress tracking
- Failure reporting
- Extensible operation types

**Usage:**
```bash
python examples/image_batch_generator.py <image_dir> <operation> [args...]
```

**Examples:**
```bash
# Resize all images
python examples/image_batch_generator.py ./photos resize 800 600

# Convert to grayscale
python examples/image_batch_generator.py ./photos grayscale

# Rotate images
python examples/image_batch_generator.py ./photos rotate 90
```

**Required Worker:**
You need an image processing worker script. Example:

```python
#!/usr/bin/env python3
# image_worker.py
import sys
from PIL import Image

operation = sys.argv[1]
input_file = sys.argv[2]
output_file = sys.argv[3]

img = Image.open(input_file)

if operation == "resize":
    width, height = int(sys.argv[4]), int(sys.argv[5])
    img = img.resize((width, height))
elif operation == "grayscale":
    img = img.convert('L')
elif operation == "rotate":
    angle = int(sys.argv[4])
    img = img.rotate(angle)

img.save(output_file)
print(f"Processed: {input_file} -> {output_file}")
```

Then run the worker:
```bash
python worker_client.py image_processing image_worker.py 4
```

## Creating Your Own Examples

### Custom Generator Template

```python
import asyncio
import websockets
import json

class MyGenerator:
    async def submit_jobs(self, jobs):
        async with websockets.connect('ws://localhost:8765') as ws:
            # Convert your jobs to batches
            batches = [[str(j.id), j.data] for j in jobs]

            # Submit
            await ws.send(json.dumps({
                'generator': True,
                'workload': 'my_workload',
                'batches': batches
            }))

            # Collect results
            results = []
            for _ in range(len(batches)):
                result = json.loads(await ws.recv())
                results.append(result)

            return results
```

### Custom Worker Template

```python
import asyncio
import websockets
import json
from queue import Queue
import threading

class MyWorker:
    def __init__(self):
        self.result_queue = Queue()

    async def connect(self):
        async with websockets.connect('ws://localhost:8765') as ws:
            await ws.send(json.dumps({
                'worker': 'my_workload',
                'cores': 4
            }))

            sender = asyncio.create_task(self._send_results(ws))

            try:
                while True:
                    msg = json.loads(await ws.recv())
                    if 'batch' in msg:
                        threading.Thread(
                            target=self._process,
                            args=(msg['batch'],),
                            daemon=True
                        ).start()
            finally:
                sender.cancel()

    async def _send_results(self, ws):
        while True:
            if not self.result_queue.empty():
                await ws.send(json.dumps(self.result_queue.get()))
            await asyncio.sleep(0.01)

    def _process(self, batch):
        try:
            # Your processing logic
            result = self.do_work(batch)
            self.result_queue.put({
                'stdout': str(result),
                'stderr': '',
                'exitcode': 0
            })
        except Exception as e:
            self.result_queue.put({
                'stdout': '',
                'stderr': str(e),
                'exitcode': 1
            })

    def do_work(self, batch):
        # Implement your logic
        pass
```

## More Examples

Looking for more integration patterns? See:

- [INTEGRATION.md](../INTEGRATION.md) - Comprehensive integration guide
- [API.md](../API.md) - WebSocket protocol details
- [ARCHITECTURE.md](../ARCHITECTURE.md) - Design overview

## Contributing Examples

Have a useful example? Please contribute!

1. Create your example file in this directory
2. Add documentation in this README
3. Submit a pull request

Good example candidates:
- Data processing pipelines
- Machine learning batch inference
- Video processing workflows
- Database batch operations
- API batch requests
