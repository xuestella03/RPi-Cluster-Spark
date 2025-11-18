# OS Custom Image Configurations
## Base OS Images
Links to tutorials for each base OS. 
- DietPi
- Raspberry Pi OS
- Ubuntu

### Make sure they are compatible with PXEBoot 
- Run this: `vcgencmd otp_dump | grep 17:`, should equal `17:3020000a`
Setting up the initial ram filesystem. See tutorial. 
- DietPi: kernel is `vmlinuz-6.12.47+rpt-rpi-v8`, i.e., kernel version is `6.12.47+rpt-rpi-v8` (find this by running `uname -r`).

**Necessary Changes**
*For Raspberry Pi do we do the below or manage initramfs?*

- [Link for RPi `initramfs`](https://raspberrypi.stackexchange.com/questions/92557/how-can-i-use-an-init-ramdisk-initramfs-on-boot-up-raspberry-pi)
- [Link for instructions including below](https://gist.github.com/G-UK/779b344d534296ad26db32adcafff781)
---

In `boot/cmdline.txt` it'll say this:

```
root=PARTUUID=50e4c9ce-02 rootfstype=ext4 rootwait fsck.repair=yes net.ifnames=0 logo.nologo console=tty1
```

To enable NFS, change it to this:

```
console=tty1 root=/dev/nfs nfsroot=<server-ip>:<path-to-nfs-root>,vers=3,tcp rw ip=dhcp rootwait net.ifnames=0 logo.nologo
```

### To Get Onto Dubliner
Use `pishrink` to get a smaller image. 

```bash
sudo dd if=/dev/sda of=name.img bs=4M status=progress
wget https://raw.githubusercontent.com/Drewsif/PiShrink/master/pishrink.sh
chmod +x pishrink.sh
sudo ./pishrink.sh -z name .img
```

<!-- I created a compressed `.img.gz` file, copied it to my Windows machine and copied it to Dubliner.

1. Navigate to `home/dietpi/`. This creates the compressed file in that directory.
2. Run the following (check what device it is with `lsblk`, returns something like `/dev/sda`):

```bash
sudo dd if=/dev/sda bs=4M status=progress | gzip -9 > file-name.img.gz
```
3. Run this to uncompress:

```bash
gunzip -c file-name.img.gz | sudo dd of=/dev/sda bs=4M status=progress
``` -->
## JVMs

### Eclipse OpenJ9
```bash
# Download IBM Semeru Runtime (OpenJDK with OpenJ9) for ARM64
cd /tmp

# For Java 17 ARM64
wget https://github.com/ibmruntimes/semeru17-binaries/releases/download/jdk-17.0.9%2B9_openj9-0.41.0/ibm-semeru-open-jdk_aarch64_linux_17.0.9_9_openj9-0.41.0.tar.gz

# Extract
sudo mkdir -p /opt/semeru
sudo tar -xzf ibm-semeru-open-jdk_aarch64_linux_17.0.9_9_openj9-0.41.0.tar.gz -C /opt/semeru --strip-components=1

# Set up alternatives
sudo update-alternatives --install /usr/bin/java java /opt/semeru/bin/java 1
sudo update-alternatives --install /usr/bin/javac javac /opt/semeru/bin/javac 1

# Set environment variables
echo 'export JAVA_HOME=/opt/semeru' | sudo tee -a /etc/profile.d/java.sh
echo 'export PATH=$JAVA_HOME/bin:$PATH' | sudo tee -a /etc/profile.d/java.sh
source /etc/profile.d/java.sh

# Verify installation
java -version
# should say xxx openj9 xxx
```

## PySpark

### Testing the install
```bash
python3 -c "import pyspark; print(pyspark.__version__)"
```

### Troubleshooting

These are the issues I ran into for pyspark installation.

**No Space**

```
Could not write to build/lib/pyspark/jars/ivy-2.5.3.jar: no space left on device. 
```

This is because there's only 1GB of RAM and tmp is in there an unable to build via pip. So make a new tmp directory that is instead on the partition that has more space (so in this case the root from the flash drive).

```bash
mkdir -p ~/pip-build-tmp

# you have to pip install with --break-system-packages
TMPDIR=~/pip-build-tmp pip install pyspark --no-build-isolation --break-system-packages
```

