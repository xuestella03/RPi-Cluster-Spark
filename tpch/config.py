"""
Here I'm configuring it so it's easy to go from local testing
to the actual cluster.

Right now at home I'm testing using my PC as the server and a
single raspberry pi as a worker. 
"""

SPARK_MASTER = 'local[*]'
CLUSTER_MASTER_IP = "192.168.50.247"
CLUSTER_MASTER_URL = f"spark://{CLUSTER_MASTER_IP}:7077"

SPARK_DRIVER_MEMORY = "1g"
SPARK_EXECUTOR_MEMORY = "1g"
# SPARK_DRIVER_MEMORY = "500m"
# SPARK_EXECUTOR_MEMORY = "500m"

# Current OS-JVM configuration (file name for results)
CUR_CONFIG = "dietpi-eclipse-j9"
