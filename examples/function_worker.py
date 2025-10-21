#!/usr/bin/env python3
"""
Function Worker Example

Demonstrates a worker that executes Python functions instead of subprocess commands.

This worker:
- Receives function names and arguments from generators
- Executes functions from a registry
- Returns results via JSON serialization
- Handles errors gracefully

Usage:
    python function_worker.py [workload_name] [num_cores]

Example:
    python function_worker.py data_processing 4
"""

import asyncio
import websockets
import json
import sys
import traceback
from queue import Queue
import threading
from typing import Callable, Dict, Any

# Function registry - add your functions here
FUNCTION_REGISTRY: Dict[str, Callable] = {}


def register_function(name: str):
    """Decorator to register functions for remote execution."""
    def decorator(func: Callable):
        FUNCTION_REGISTRY[name] = func
        return func
    return decorator


# Example registered functions
@register_function("add")
def add_numbers(a: str, b: str) -> Dict[str, Any]:
    """Add two numbers."""
    result = float(a) + float(b)
    return {"result": result, "operation": "add"}


@register_function("multiply")
def multiply_numbers(a: str, b: str) -> Dict[str, Any]:
    """Multiply two numbers."""
    result = float(a) * float(b)
    return {"result": result, "operation": "multiply"}


@register_function("process_text")
def process_text(text: str, operation: str) -> Dict[str, Any]:
    """Process text with various operations."""
    operations = {
        "upper": text.upper(),
        "lower": text.lower(),
        "reverse": text[::-1],
        "length": len(text)
    }

    if operation not in operations:
        raise ValueError(f"Unknown operation: {operation}")

    return {
        "input": text,
        "operation": operation,
        "result": operations[operation]
    }


@register_function("word_count")
def word_count(text: str) -> Dict[str, Any]:
    """Count words in text."""
    words = text.split()
    return {
        "text_length": len(text),
        "word_count": len(words),
        "unique_words": len(set(words))
    }


class FunctionWorker:
    """Worker that executes Python functions from a registry."""

    def __init__(self, workload_name: str, num_cores: int = 1):
        self.workload_name = workload_name
        self.num_cores = num_cores
        self.result_queue = Queue()

    async def connect(self, server_uri="ws://localhost:8765"):
        """Connect to cluster and process function calls."""
        print(f"Function Worker starting...")
        print(f"Workload: {self.workload_name}")
        print(f"Cores: {self.num_cores}")
        print(f"Registered functions: {', '.join(FUNCTION_REGISTRY.keys())}")
        print()

        async with websockets.connect(server_uri) as websocket:
            # Register as worker
            registration = {
                "worker": self.workload_name,
                "cores": self.num_cores
            }
            await websocket.send(json.dumps(registration))
            print(f"Connected to {server_uri}")

            # Start result sender
            sender = asyncio.create_task(self._result_sender(websocket))

            try:
                while True:
                    message = json.loads(await websocket.recv())

                    if "batch" in message:
                        args = message["batch"]
                        print(f"Received: {args}")

                        # Execute in thread pool to avoid blocking
                        threading.Thread(
                            target=self._execute_function,
                            args=(args,),
                            daemon=True
                        ).start()

            finally:
                sender.cancel()

    async def _result_sender(self, websocket):
        """Send results from queue to server."""
        while True:
            if not self.result_queue.empty():
                result = self.result_queue.get()
                await websocket.send(json.dumps(result))
                print(f"Sent result: {result.get('stdout', '')[:50]}...")
            await asyncio.sleep(0.01)

    def _execute_function(self, args: list):
        """Execute function call in thread."""
        try:
            # Parse arguments: [function_name, arg1, arg2, ...]
            if not args:
                raise ValueError("No function name provided")

            function_name = args[0]
            function_args = args[1:]

            # Look up function
            if function_name not in FUNCTION_REGISTRY:
                raise ValueError(
                    f"Unknown function: {function_name}. "
                    f"Available: {', '.join(FUNCTION_REGISTRY.keys())}"
                )

            # Execute function
            func = FUNCTION_REGISTRY[function_name]
            result = func(*function_args)

            # Format response
            self.result_queue.put({
                "stdout": json.dumps(result, indent=2),
                "stderr": "",
                "exitcode": 0
            })

            print(f"✓ Completed: {function_name}({', '.join(function_args)})")

        except Exception as e:
            # Handle errors
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

            self.result_queue.put({
                "stdout": "",
                "stderr": error_msg,
                "exitcode": 1
            })

            print(f"✗ Error: {function_name} - {e}")


def print_usage():
    """Print usage information."""
    print("Function Worker - Execute Python functions via CrepuscularCluster")
    print()
    print("Usage: python function_worker.py [workload_name] [num_cores]")
    print()
    print("Arguments:")
    print("  workload_name    Workload identifier (default: python_functions)")
    print("  num_cores        Number of cores to declare (default: 1)")
    print()
    print("Registered Functions:")
    for name, func in FUNCTION_REGISTRY.items():
        print(f"  {name:20} {func.__doc__ or ''}")
    print()
    print("Example:")
    print("  python function_worker.py data_processing 4")
    print()
    print("Generator Example:")
    print("  # In your generator, submit batches like:")
    print('  batches = [')
    print('      ["add", "10", "20"],')
    print('      ["multiply", "5", "3"],')
    print('      ["word_count", "hello world"],')
    print('  ]')


async def main():
    """Main entry point."""
    # Parse arguments
    workload_name = sys.argv[1] if len(sys.argv) > 1 else "python_functions"
    num_cores = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    if "--help" in sys.argv or "-h" in sys.argv:
        print_usage()
        sys.exit(0)

    # Create and start worker
    worker = FunctionWorker(workload_name, num_cores)

    try:
        await worker.connect()
    except KeyboardInterrupt:
        print("\nShutting down...")
    except websockets.exceptions.WebSocketException as e:
        print(f"\nError: Could not connect to cluster server")
        print(f"Make sure the server is running at ws://localhost:8765")
        print(f"Details: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
