# CrepuscularCluster

A creepy, crawly, batch job executor over websockets.

[Watch a full demo on YouTube](https://www.youtube.com/watch?v=EbcG25HSExI)

## Workloads

a string keyword that matches Generators and Workers together. There's an implicit assumption in the code right now that there will be one generator and any number of workers for a given workload.

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

## Known Bugs

Generator support is pretty weak. All connected generators will receive responses back, so only connect a single generator for any workload at a time.

There's some async bug that prevents the generator from always perfectly counting the responses received.

