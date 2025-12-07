from pyspark.sql import SparkSession
import os
import socket

def create_spark_session():
    """
    Create the sesssion if it's the master node.

    Python doesn't allow inline comments for multiline so comments for 
    SparkSession.builder here:
      - .master: this is assuming standalone cluster
      - I'm letting the spark.driver.memory be whatever
      - Executor memory set so that there's space for OS
      - Not sure how many executor cores we want, RPi 3b+ has 4 total
      - Not sure if we plan on dynamic allocation
      - Shuffle partitions subject to change, common guideline is 2-4 times the number 
        of cores in cluster?
    """
    hostname = socket.gethostname() # or do this with ansible
    if hostname == "dubliner": # check actual name
        print("This is the master node!")
        builder = SparkSession.builder \
            .appName("TPC-H Benchmarking") \
            .master("spark://dubliner:7077") \
            .config("spark.executor.memory", "512m") \
            .config("spark.executor.cores", "2") \
            .config("spark.dynamicAllocation.enabled") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/nfs/shared/spark-logs") \
            .getOrCreate()
        
        data_path = "/nfs/shared/tpch-data/"

        # and then run queries here.

    else:
        print("This is a worker node!")
        exit(0)