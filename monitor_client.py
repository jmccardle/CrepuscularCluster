import asyncio
import websockets
import json

WORKLOAD_NAME = "SoE"

async def monitor_client():
    uri = "ws://localhost:8765"
    
    async with websockets.connect(uri) as websocket:
        # Send registration
        registration = {"monitor": True, "workload": WORKLOAD_NAME}
        await websocket.send(json.dumps(registration))
        print(f"Monitoring.")

        # Listen for results
        while True:
            response = json.loads(await websocket.recv())
            print(f"{response}")

if __name__ == "__main__":
    asyncio.run(monitor_client())
