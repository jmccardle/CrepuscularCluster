import asyncio
import websockets
import json
import subprocess
import sys
import threading
from queue import Queue

# Read command-line arguments
if len(sys.argv) != 4:
    print("Usage: python worker_client.py <workload> <executable> <num_cores>")
    sys.exit(1)

WORKLOAD_NAME = sys.argv[1]
EXECUTABLE = sys.argv[2]
NUM_CORES = int(sys.argv[3])

result_queue = Queue()  # Bridge between threads and async loop

async def worker_client():
    uri = "ws://localhost:8765"

    async with websockets.connect(uri) as websocket:
        # Register as a worker
        registration = {"worker": WORKLOAD_NAME, "cores": NUM_CORES}
        await websocket.send(json.dumps(registration))
        print(f"Registered as Worker for {WORKLOAD_NAME} with {NUM_CORES} cores.")

        # Create task to send results from queue
        send_task = asyncio.create_task(result_sender(websocket))

        try:
            while True:
                # Wait for a job assignment
                response = json.loads(await websocket.recv())

                if "batch" in response:
                    args = response["batch"]
                    print(f"Received batch: {args}")
                    threading.Thread(target=process_batch, args=(args,), daemon=True).start()
                else:
                    await asyncio.sleep(0.1)  # No work available, wait
        finally:
            send_task.cancel()

async def result_sender(websocket):
    """Async task that sends results from the queue."""
    while True:
        # Check queue periodically
        if not result_queue.empty():
            result = result_queue.get()
            await websocket.send(json.dumps(result))
            print(f"Sent result: Exit {result['exitcode']}")
        await asyncio.sleep(0.01)  # Small delay to prevent busy-waiting

def process_batch(args):
    """Executes the given batch in a subprocess (runs in thread)."""
    cmd = [sys.executable, EXECUTABLE] + args
    print(f"Executing: {' '.join(cmd)}")

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    result = {
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "exitcode": proc.returncode
    }

    result_queue.put(result)  # Thread-safe queue.put()
    print(f"Completed batch {args}: Exit {proc.returncode}")

if __name__ == "__main__":
    asyncio.run(worker_client())
