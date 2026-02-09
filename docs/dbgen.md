```bash
cd /home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/data/tpch-dbgen

# to generate only one tpch table
./dbgen -s 1 -T n # for nation

# move to folder
mv *.tbl ../sf1

# terrible file structure right now so copy to correct location
# the reason is because I set the data folder to require it to be the same
# for the worker and the master 
# fix later...
sudo cp -r /home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf1 /home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/

# then copy entire folder to rpi
scp -r /home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf1/ root@192.168.50.196:/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/
```