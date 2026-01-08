## Memory, CPU Usage, and I/O Benchmarking

# sysbench

Note that sysbench is more so for baseline measurements of system performance, not for how the node handles tasks. The tests are built-in.

https://www.howtoforge.com/how-to-benchmark-your-system-cpu-file-io-mysql-with-sysbench


# pidstat

Now, we look at how to monitor while a task is running. We set up pidstat to monitor on each node and save the stats. 

We'll update the Ansible to include starting pidstat monitoring, ending monitoring, and sending over the log files. 

### Flags
```bash
-h # human readable
-r # memory stats
-u # CPU stats
-d # disk I/O stats
1 # sample interval
-C # get process with name
```
