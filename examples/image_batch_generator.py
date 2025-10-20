#!/usr/bin/env python3
"""
Image Batch Generator Example

Demonstrates how to build a custom generator that:
- Scans a directory for images
- Submits batch processing jobs
- Tracks progress and handles failures
- Works with the example image_worker.py

Usage:
    python image_batch_generator.py /path/to/images resize 800 600
"""

import asyncio
import websockets
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

class ImageBatchGenerator:
    """Custom generator for batch image processing."""

    def __init__(self, server_uri="ws://localhost:8765", workload="image_processing"):
        self.server_uri = server_uri
        self.workload = workload

    async def process_directory(
        self,
        image_dir: str,
        operation: str,
        *args
    ) -> Dict[str, Any]:
        """
        Process all images in a directory.

        Args:
            image_dir: Path to directory containing images
            operation: Operation to perform (resize, grayscale, rotate, etc.)
            *args: Operation-specific arguments

        Returns:
            Dictionary with processing statistics
        """
        # Find all images
        image_path = Path(image_dir)
        image_extensions = {".jpg", ".jpeg", ".png", ".gif", ".bmp"}

        image_files = [
            f for f in image_path.iterdir()
            if f.suffix.lower() in image_extensions
        ]

        if not image_files:
            print(f"No images found in {image_dir}")
            return {"total": 0, "success": 0, "failed": 0}

        # Build batches
        batches = []
        for img_file in image_files:
            output_file = img_file.parent / f"{img_file.stem}_processed{img_file.suffix}"
            batch_args = [operation, str(img_file), str(output_file)] + list(args)
            batches.append(batch_args)

        print(f"Submitting {len(batches)} images for {operation} processing...")

        # Connect and submit
        stats = await self._submit_and_collect(batches, image_files)

        return stats

    async def _submit_and_collect(
        self,
        batches: List[List[str]],
        image_files: List[Path]
    ) -> Dict[str, Any]:
        """Submit batches and collect results with progress tracking."""

        stats = {
            "total": len(batches),
            "success": 0,
            "failed": 0,
            "failed_images": []
        }

        async with websockets.connect(self.server_uri) as websocket:
            # Register and submit
            registration = {
                "generator": True,
                "workload": self.workload,
                "batches": batches
            }
            await websocket.send(json.dumps(registration))

            # Collect results
            for i in range(len(batches)):
                response = json.loads(await websocket.recv())

                if response["exitcode"] == 0:
                    stats["success"] += 1
                    print(f"✓ [{i+1}/{stats['total']}] {image_files[i].name}")
                else:
                    stats["failed"] += 1
                    stats["failed_images"].append(str(image_files[i]))
                    print(f"✗ [{i+1}/{stats['total']}] {image_files[i].name}")
                    if response["stderr"]:
                        print(f"   Error: {response['stderr']}")

        return stats


def print_stats(stats: Dict[str, Any]):
    """Print processing statistics."""
    print("\n" + "="*60)
    print("Processing Complete")
    print("="*60)
    print(f"Total images:    {stats['total']}")
    print(f"Successful:      {stats['success']}")
    print(f"Failed:          {stats['failed']}")

    if stats['failed_images']:
        print("\nFailed images:")
        for img in stats['failed_images']:
            print(f"  - {img}")


async def main():
    """Main entry point."""
    if len(sys.argv) < 3:
        print("Usage: python image_batch_generator.py <image_dir> <operation> [args...]")
        print("\nExamples:")
        print("  python image_batch_generator.py ./photos resize 800 600")
        print("  python image_batch_generator.py ./photos grayscale")
        print("  python image_batch_generator.py ./photos rotate 90")
        sys.exit(1)

    image_dir = sys.argv[1]
    operation = sys.argv[2]
    operation_args = sys.argv[3:]

    # Create generator and process
    generator = ImageBatchGenerator()

    print(f"Image Batch Generator")
    print(f"Directory: {image_dir}")
    print(f"Operation: {operation} {' '.join(operation_args)}")
    print()

    try:
        stats = await generator.process_directory(image_dir, operation, *operation_args)
        print_stats(stats)

    except FileNotFoundError:
        print(f"Error: Directory '{image_dir}' not found")
        sys.exit(1)
    except websockets.exceptions.WebSocketException as e:
        print(f"Error: Could not connect to cluster server")
        print(f"Make sure the server is running at ws://localhost:8765")
        print(f"Details: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
