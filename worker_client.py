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

task_queue = Queue()  # Holds jobs to be executed
active_threads = set()  # Track running threads

async def worker_client():
    uri = "ws://localhost:8765"

    async with websockets.connect(uri) as websocket:
        # Register as a worker
        registration = {"worker": WORKLOAD_NAME, "cores": NUM_CORES}
        await websocket.send(json.dumps(registration))
        print(f"Registered as Worker for {WORKLOAD_NAME} with {NUM_CORES} cores.")

        while True:
            # If we have available cores, request new work
            #if task_queue.qsize() < NUM_CORES:
            #    await websocket.send(json.dumps({"request_work": True}))

            # Wait for a job assignment
            response = json.loads(await websocket.recv())

            if "batch" in response:
                args = response["batch"]
                print(f"Received batch: {args}")
                task_queue.put(args)
                threading.Thread(target=process_batch, args=(websocket, args), daemon=True).start()
            else:
                await asyncio.sleep(0.1)  # No work available, wait

def process_batch(websocket, args):
    """Executes the given batch in a subprocess and sends the result."""
    global active_threads

    cmd = [sys.executable, EXECUTABLE] + args
    print(f"Executing: {' '.join(cmd)}")

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = proc.communicate()

    result = {
        "stdout": stdout.strip(),
        "stderr": stderr.strip(),
        "exitcode": proc.returncode
    }

    asyncio.run(send_result(websocket, result))
    print(f"Completed batch {args}: Exit {proc.returncode}")

    active_threads.discard(threading.current_thread())

async def send_result(websocket, result):
    """Sends the completed batch result back to the server."""
    await websocket.send(json.dumps(result))

if __name__ == "__main__":
    asyncio.run(worker_client())
