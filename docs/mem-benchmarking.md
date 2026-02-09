## Memory, CPU Usage, and I/O Benchmarking

*Note on disabling swap: check that `free -m` shows 0s for the swap row.*

# sysbench

Note that sysbench is more so for baseline measurements of system performance, not for how the node handles tasks. The tests are built-in.

- https://www.howtoforge.com/how-to-benchmark-your-system-cpu-file-io-mysql-with-sysbench

- https://cylab.be/blog/330/benchmark-linux-systems-with-sysbench -- this one gives some more detail on benchmarking with threads. 

Use the memory bandwidth test ("which allocates a large buffer of memory and measures the time required for the CPU to sequentially read the entire buffer"). Do this on varying number of cores. 

What I did:

```bash

```

# pidstat

Now, we look at how to monitor while a task is running. We set up pidstat to monitor on each node and save the stats. 

### Flags
```bash
-h # human readable
-r # memory stats
-u # CPU stats
-d # disk I/O stats
1 # sample interval
-C # get process with name
```

To copy a file over manually from pi:
```bash
scp root@192.168.50.196:/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/results/pidstat/<file> /home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/pidstat/
```

## Troubleshooting

