import asyncio
import websockets
import json

BATCH_SIZE = 100
WORKLOAD_NAME = "SoE"

async def generator_client():
    uri = "ws://localhost:8765"
    
    async with websockets.connect(uri) as websocket:
        # Generate batches of work
        batches = [[str(start), str(min(start + BATCH_SIZE - 1, 1000))] 
                   for start in range(1, 1001, BATCH_SIZE)]

        # Send registration and job batches
        registration = {"generator": True, "workload": WORKLOAD_NAME, "batches": batches}
        await websocket.send(json.dumps(registration))
        print(f"Submitted {len(batches)} batch jobs.")

        # Track remaining jobs
        remaining_batches = len(batches)
        completed_batches = 0

        # Listen for results (now pushed from server)
        while remaining_batches > 0:
            response = json.loads(await websocket.recv())
            completed_batches += 1
            print(f"Result ({completed_batches}/{len(batches)}): {response}")
            remaining_batches -= 1

        print("All batches completed. Exiting.")

if __name__ == "__main__":
    asyncio.run(generator_client())
