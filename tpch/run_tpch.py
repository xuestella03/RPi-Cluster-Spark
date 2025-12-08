'''
initialize the spark session
this is called by deploy-tpch.yml
  - load data using load_data.py
  - execute queries from queries.py
  - save results
'''

from pyspark.sql import SparkSession
import config 
import time 

def create_spark_session():

    spark = SparkSession.builder \
        .appName("TPC-H Benchmark") \
        .master(config.CLUSTER_MASTER_URL) \
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    print(f"Spark session started.")
    print(f"Spark UI here: {spark.sparkContext.uiWebUrl}")
    
    return spark 

if __name__ == "__main__":
    print("Running run_tpch.py...")
    spark = create_spark_session()

    # time.sleep(10)
    spark.stop()