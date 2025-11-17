# RPi-Cluster-Spark

## Setup Info
Links:
- [Blog Post](https://www.hamishmb.com/howto-pxe-network-booting)
- [Not sure](https://gist.github.com/G-UK/779b344d534296ad26db32adcafff781)

Basic files: 
- Place all the boot-related files (including the kernel, DTB, and config files, etc.) into the `tank/project/tftpboot/<OS-JVM>/` directory. In particular, the `cmdline.txt` file will have to specify that the root filesystem will be NFS. Then, the root filesystem (so all directories aside from the boot partition) will be mounted in the NFS server once that's ready.
- Run this: `vcgencmd otp_dump | grep 17:`, should equal `17:3020000a`.

## Network File System Structure
*With PXEBoot (i.e. TFTP + NFS)*

Temporarily assuming this, check once it's set up:
```
/nfs/shared/
├── tpch-data
├── scripts/
│    └── tpch_benchmark.py
└── spark-logs/
```

## Usage
Once the PXEBoot setup is done, going to do this:

For the manual version (no Ansible yet)
```bash
# on master
$SPARK_HOME/sbin/start-master.sh -h <master-hostname> -p 7077

# on worker, the cores and mem are what I specified in SparkSession.builder
$SPARK_HOME/sbin/start-worker.sh spark://<master-hostname>:7077 --cores 2 --memory 512M

# back on master, run the script
spark-submit --master spark://<master-hostname>:7077 /nfs/shared/scripts/tpch_benchmark.py