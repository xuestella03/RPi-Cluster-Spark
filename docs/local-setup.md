# Local Setup

*These are logs for my local setup with server as my PC and cluster as single node*

## Steps

1. Check connection and ssh. Do so by running 

    ```bash
    ansible-playbook -i ansible/inventory/hosts.yml ansible/playbooks/test.yml
    ```
2. Install JVM and set `JAVA_HOME`. This varies depending on which JVM. For Eclipse OpenJ9, see [OS.md](/docs/OS.md). For standard OpenJDK, do the following:

    ```bash
    sudo apt update && sudo apt upgrade -y
    sudo apt install openjdk-21-jdk-headless -y # check the version

    # To set java home
    # path is something like /usr/lib/jvm/xxx
    # add this to ~/.bashrc
    JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 # armhf for raspios
    export JAVA_HOME
    export PATH=$JAVA_HOME/bin:$PATH

    # then exit and 
    source ~/.bashrc

    # https://download.bell-sw.com/java/21.0.9+15/bellsoft-jdk21.0.9+15-linux-arm32-vfp-hflt-lite.deb
    ```

    ```bash
    # Download tarball version
    wget https://download.bell-sw.com/java/21.0.9+15/bellsoft-jdk21.0.9+15-linux-arm32-vfp-hflt-lite.tar.gz

    # https://download.bell-sw.com/java/21.0.9+15/bellsoft-jdk21.0.9+15-linux-aarch64-lite.tar.gz

    # Extract to /opt
    sudo mkdir -p /opt/liberica-jdk21
    sudo tar -xzf bellsoft-jdk21.0.9+15-linux-arm32-vfp-hflt-lite.tar.gz -C /opt/liberica-jdk21 --strip-components=1

    # Set JAVA_HOME
    echo 'JAVA_HOME=/opt/liberica-jdk21' >> ~/.bashrc
    echo 'export JAVA_HOME' >> ~/.bashrc
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

    source ~/.bashrc
    java -version
    ```

3. Install Python
    ```bash
    sudo apt update && sudo apt upgrade -y
    sudo apt install python3 -y
    ```
4. Set up NFS
    ```bash
    # On PC (only need to do this once)
    sudo apt update
    sudo apt install nfs-kernel-server
    sudo chmod -R 755 /local/path/to/data

    sudo nano /etc/exports
    # to this file, add this line: 
    /local/path/to/data <ip>/24(rw,sync,no_subtree_check,no_root_squash)

    # then
    sudo exportfs -ra
    sudo systemctl restart nfs-kernel-server
    sudo exportfs -v

    # On Pi (I should make this a playbook)
    sudo apt update 
    sudo apt install nfs-common -y

    sudo mkdir -p /mnt/tpch
    sudo mount -t nfs <pc-ip>:/pc/path/to/data /mnt/tpch
    # ls and you should see the .tbl files are there

    # now make persistent
    sudo nano /etc/fstab

    # in that file add 
    192.168.50.247:/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/data  /mnt/tpch  nfs  defaults  0  0

    # then
    sudo mount -a

    # to set up /mnt/tpch in PC so then we can use that as universal data path
    sudo mkdir -p /mnt/tpch
    sudo mount --bind /home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/data /mnt/tpch

    # do this for permanent version in /etc/fstab
    /home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/data /mnt/tpch none bind 0 0

5. Download Spark by running the install spark playbook, (I wonder if we can do this downloading to the NFS to save space locally.) Note that on the workers, it's not necessary to install PySpark once you have Spark and Python.

## Troubleshooting

### No space for Spark

The error is something like this: 

```
[Errno 28] No space left on device: b<file path> ... spark.tar.gz
```

Easiest way is to manually install Spark on each node. (Or do this in sda for more space)

```bash
# first clear tmp

# then do this
# Download Spark
wget -O /tmp/spark.tar.gz https://dlcdn.apache.org/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz

# Create installation directory
sudo mkdir -p /opt/spark

# Extract Spark
sudo tar -xzf /tmp/spark.tar.gz -C /opt/spark --strip-components=1

# Set environment variables
echo 'export SPARK_HOME=/opt/spark' | sudo tee /etc/profile.d/spark.sh
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' | sudo tee -a /etc/profile.d/spark.sh
sudo chmod 644 /etc/profile.d/spark.sh

# Add to your bashrc
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin' >> ~/.bashrc

# Clean up
rm /tmp/spark.tar.gz

# Verify installation
source ~/.bashrc
spark-submit --version
```

Another way: we can instead install Spark on the USB and mount that. Instructions:

```bash
# On Pi
sudo mkdir -p /mnt/usb

# Mount the partition (the sda1 is the one that has /boot under MOUNTPOINT when you run `df -h`)
sudo mount /dev/sda1 /mnt/usb

# Verify
df -h /mnt/usb 

# Permissions (for everyone)
sudo chmod 777 /mnt/usb
```

Now, run the `install-spark-with-mount.yml` playbook. 

### Timeout
The error looks like this:

```
fatal: [raspberry-pi]: FAILED! => {"msg": "Timeout (12s) waiting for privilege escalation prompt: "}
```

Configure passwordless sudo. On the Raspberry Pi, run:

```
sudo visudo

# add this to the end
<user> ALL=(ALL) NOPASSWD: ALL
```

### No cores running
