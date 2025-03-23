import asyncio
import websockets
import json
import threading
import queue
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional

@dataclass
class BatchInput:
    args: List[str]
    lease_state: str = "new"  # "new" -> "offered" -> "leased" -> "done"
    assigned_worker: Optional[int] = None
    cores_required: int = 1
    timestamp: float = field(default_factory=time.time)

@dataclass
class BatchOutput:
    stdout: str
    stderr: str
    exitcode: int

class ClientWorker:
    def __init__(self, worker_id: int, cores: int, websocket, workload: str):
        self.worker_id = worker_id
        self.cores = cores
        self.websocket = websocket
        self.workload = workload
        self.leases: List[BatchInput] = []

    def has_capacity(self, cores_needed: int) -> bool:
        """Check if the worker has enough free cores."""
        used_cores = sum(batch.cores_required for batch in self.leases)
        return (used_cores + cores_needed) <= self.cores

class WorkServer:
    def __init__(self):
        self.queues: Dict[str, queue.Queue] = {}
        self.results: Dict[str, List[BatchOutput]] = {}
        self.workers: Dict[int, ClientWorker] = {}
        self.monitors: List[websockets.WebSocketServerProtocol] = []
        self.lock = threading.Lock()
        self._worker_id = 0

    async def handler(self, websocket, path=None):
        if path: print(f"handler arg: {path}")
        """Handles incoming WebSocket connections."""
        data = await websocket.recv()
        client_info = json.loads(data)

        if "worker" in client_info:
            await self.register_worker(websocket, client_info)
        elif "generator" in client_info:
            await self.handle_generator(websocket, client_info)
        elif "monitor" in client_info:
            await self.register_monitor(websocket, client_info["monitor"])

    async def register_worker(self, websocket, client_info):
        """Registers a new worker and assigns jobs."""
        with self.lock:
            worker_id = self._worker_id
            self._worker_id += 1

        workload = client_info["worker"]
        cores = client_info["cores"]

        client = ClientWorker(worker_id, cores, websocket, workload)
        self.workers[worker_id] = client
        print(f"Worker {worker_id} registered for workload '{workload}' with {cores} cores.")
        await self.notify_monitors()

        try:
            while True:
                message = await websocket.recv()
                response = json.loads(message)

                if "stdout" in response:
                    self.process_completed_job(worker_id, response)
                    await self.notify_monitors()
                    self.assign_work(worker_id)

        except websockets.exceptions.ConnectionClosed:
            self.cleanup_disconnected_worker(worker_id)

    async def handle_generator(self, websocket, client_info):
        """Handles job submissions from a generator."""
        workload = client_info["workload"]
        batch_list = client_info["batches"]
        self.submit_batches(workload, batch_list)
        await self.notify_monitors()

        try:
            while True:
                if workload in self.results and self.results[workload]:
                    result = self.results[workload].pop(0)
                    await websocket.send(json.dumps(result.__dict__))
                else:
                    await asyncio.sleep(1)

        except websockets.exceptions.ConnectionClosed:
            print(f"Generator for {workload} disconnected.")

    async def register_monitor(self, websocket, workload):
        """Adds a monitor client that listens for worker/job state changes."""
        self.monitors.append(websocket)
        print(f"Monitor connected for workload '{workload}'.")

        try:
            while True:
                await asyncio.sleep(10)  # Keep connection alive

        except websockets.exceptions.ConnectionClosed:
            self.monitors.remove(websocket)
            print("Monitor disconnected.")

    def submit_batches(self, workload, batch_list):
        """Adds new jobs to the queue and notifies monitors."""
        if workload not in self.queues:
            self.queues[workload] = queue.Queue()
        
        for args in batch_list:
            batch = BatchInput(args=args)
            self.queues[workload].put(batch)

        print(f"Added {len(batch_list)} batches to workload '{workload}'.")
        self.distribute_work(workload)

    def distribute_work(self, workload = None):
        """Assigns jobs from the queue to available workers."""
        if not workload:
            for wl in self.queues.keys():
                self.distribute_work(wl)
            return
        if workload not in self.queues or self.queues[workload].empty():
            return
        
        for worker in list(self.workers.values()):
            if worker.workload == workload:
                self.assign_work(worker.worker_id)

    def assign_work(self, worker_id):
        """Finds available work and sends it to a worker."""
        worker = self.workers.get(worker_id)
        if not worker:
            return
        
        if worker.workload in self.queues:
            queue_obj = self.queues[worker.workload]
            requeue_list = []

            while not queue_obj.empty():
                batch = queue_obj.get_nowait()

                if batch.lease_state == "new" and worker.has_capacity(batch.cores_required):
                    batch.lease_state = "leased"
                    batch.assigned_worker = worker.worker_id
                    batch.timestamp = time.time()
                    worker.leases.append(batch)

                    try:
                        asyncio.create_task(worker.websocket.send(json.dumps({"batch": batch.args})))
                        print(f"Assigned {batch.args} to Worker {worker.worker_id}.")
                        asyncio.create_task(self.notify_monitors())
                    except websockets.exceptions.ConnectionClosed:
                        self.cleanup_disconnected_worker(worker.worker_id)
                        return
                else: requeue_list.append(batch)
            for batch in requeue_list:
                queue_obj.put(batch)

    def process_completed_job(self, worker_id, response):
        """Handles completed jobs and notifies monitors."""
        worker = self.workers.get(worker_id)
        if not worker:
            return

        # Find and remove the completed batch
        for batch in worker.leases:
            if batch.lease_state == "leased":
                worker.leases.remove(batch)
                batch.lease_state = "done"
                break

        output = BatchOutput(**response)
        workload = worker.workload
        if workload not in self.results:
            self.results[workload] = []
        self.results[workload].append(output)

        print(f"Worker {worker_id} completed {batch.args}, exit code {response['exitcode']}.")
        asyncio.create_task(self.notify_monitors())

    def cleanup_disconnected_worker(self, worker_id):
        """Handles worker disconnections and reassigns their work."""
        with self.lock:
            worker = self.workers.pop(worker_id, None)
            if not worker:
                return
            
            print(f"Worker {worker_id} disconnected. Reassigning work.")

            for batch in worker.leases:
                batch.lease_state = "new"
                batch.assigned_worker = None
                self.queues[worker.workload].put(batch)

            worker.leases.clear()
            asyncio.create_task(self.notify_monitors())

    async def notify_monitors(self):
        """Sends updated worker/job state to all connected monitors."""
        state = {
            "workers": [
                {
                    "worker_id": worker.worker_id,
                    "cores": worker.cores,
                    "leases": {"cores_used": sum([batch.cores_required for batch in worker.leases]), "jobs_assigned": len(worker.leases)}
                }
                for worker in self.workers.values()
            ],
            "jobs_waiting": sum(q.qsize() for q in self.queues.values())
        }

        for monitor in list(self.monitors):
            try:
                await monitor.send(json.dumps(state))
            except websockets.exceptions.ConnectionClosed:
                self.monitors.remove(monitor)

async def main():
    """Starts the WebSocket server and job monitoring."""
    server = WorkServer()
    #def feed_workers():
    #    time.sleep(10)
    #    while True:
    #        server.distribute_work()
    #        time.sleep(1)

    #server_thread = threading.Thread(target=feed_workers, daemon=True)
    #server_thread.start()

    async with websockets.serve(server.handler, "localhost", 8765):
        print("WebSocket server started on ws://localhost:8765")
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())

