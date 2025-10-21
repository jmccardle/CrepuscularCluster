# CrepuscularCluster

A creepy, crawly, batch job executor over websockets.

[Watch a full demo on YouTube](https://www.youtube.com/watch?v=EbcG25HSExI)

## Architecture

Built on Python's `asyncio` and WebSockets for efficient concurrent job distribution.

## Workloads

A string keyword that matches Generators and Workers together. Multiple generators can connect for the same workload, with results routed correctly to each generator.

## Generators

submit batches to be worked. A single batch is the command line arguments to some other application.

## Workers

receive batches to be worked. A worker declares how many cores it has, and will receive jobs whenever one is available requiring <= its available cores.

# Run It

Server:
```
pip install websockets
python cluster_server.py
```

Edit `worker_client.py` to have the hostname or IP for where your server will be accessible, and `generator_client.py` to create the sets of arguments, and process the responses received.

At least one worker:
```
python worker_client.py <workload_string> <command> <cores>
```

and the generator:

```
python generator_client.py
```

Your batches should be executed by the clients!

## Documentation

### Getting Started
- **[Quick Start](#run-it)** - Get up and running quickly
- **[Examples](examples/)** - Practical integration examples

### Integration Guides
- **[Integration Guide](INTEGRATION.md)** - How to integrate CrepuscularCluster into your applications
  - Custom generators for your workflows
  - Custom workers that execute Python functions
  - Embedding the cluster in your applications
  - Best practices and patterns

### Reference Documentation
- **[API Documentation](API.md)** - Complete WebSocket protocol reference
  - Message formats
  - Connection lifecycle
  - Error handling
  - Best practices

- **[Architecture Overview](ARCHITECTURE.md)** - Design deep-dive
  - Component architecture
  - Concurrency model
  - Data flow diagrams
  - Performance considerations

## Requirements

- Python 3.7+
- websockets library: `pip install websockets`

## Use Cases

CrepuscularCluster is ideal for:
- **Batch Processing** - Distribute computational tasks across multiple machines
- **Parallel Workflows** - Process large datasets in parallel
- **Development Testing** - Test distributed systems locally
- **Education** - Learn about distributed systems and async Python

## Features

- ✅ WebSocket-based communication
- ✅ Async/await architecture
- ✅ Multi-core worker support
- ✅ Multiple generators per workload
- ✅ Real-time progress monitoring
- ✅ Automatic job reassignment on worker failure
- ✅ Simple integration into existing applications

