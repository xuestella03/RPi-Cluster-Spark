"""
Here I'm configuring it so it's easy to go from local testing
to the actual cluster.

Right now at home I'm testing using my PC as the server and a
single raspberry pi as a worker. 
"""

SPARK_MASTER = 'local[*]'
CLUSTER_MASTER_IP = "192.168.50.247"
CLUSTER_MASTER_URL = f"spark://{CLUSTER_MASTER_IP}:7077"

SPARK_DRIVER_MEMORY = "2g"
# SPARK_EXECUTOR_MEMORY = "1g"
# SPARK_DRIVER_MEMORY = "500m"
SPARK_EXECUTOR_MEMORY = "600m"

# Current OS-JVM configuration (file name for results)
CUR_CONFIG = "dietpi-eclipse-j9"
CUR_OS = "dietpi"
CUR_JVM = "liberica"

JVM_HEAP_MIN = "1g"
JVM_HEAP_MAX = "1g"

# JVM_GC_ALGORITHM = "SerialGC"
# JVM_GC_ALGORITHM = "ParallelGC"
JVM_GC_ALGORITHM = "Default"

SF = "0.1"

def build_jvm_options_string():
    # Fix later for more parameters
    
    options = []
    # options.append(f"-Xms{JVM_HEAP_MIN}")
    # options.append(f"-Xmx{JVM_HEAP_MAX}")
    # options.append(f"-XX:+Use{JVM_GC_ALGORITHM}")
    print(options)

    return " ".join(options)
