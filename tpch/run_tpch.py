"""
Initialize the spark session
This is called by run-tpch.yml
  - execute queries from queries.py
  - save results

Right now with ansible, this just collects all the stdout
and prints it in the next task. 

There is a way to do it like "streaming" with the async
module -- do this later. 
https://stackoverflow.com/questions/76218894/how-can-i-get-the-output-of-an-ansible-play-as-it-is-being-run

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
import config 
import queries
import os
import time 

class TPCH:
    def __init__(self, data_path, sf=0.1):
        """
        :param data_path: relative path to .tbl files
        :param sf: scale factor
        """
        self.data_path = data_path
        self.sf = sf 

        self.spark = SparkSession.builder \
            .appName("TPC-H Benchmark") \
            .master(config.CLUSTER_MASTER_URL) \
            .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
            .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        print(f"Spark session started.")
        print(f"Spark UI here: {self.spark.sparkContext.uiWebUrl}")

        self.load_tables()

    def load_tables(self):
        print("Loading TPC-H tables to dataframe...")
        
        schemas = {
            'customer': StructType([
                StructField("c_custkey", IntegerType(), True),
                StructField("c_name", StringType(), True),
                StructField("c_address", StringType(), True),
                StructField("c_nationkey", IntegerType(), True),
                StructField("c_phone", StringType(), True),
                StructField("c_acctbal", DecimalType(15, 2), True),
                StructField("c_mktsegment", StringType(), True),
                StructField("c_comment", StringType(), True)
            ]),
            
            'lineitem': StructType([
                StructField("l_orderkey", IntegerType(), True),
                StructField("l_partkey", IntegerType(), True),
                StructField("l_suppkey", IntegerType(), True),
                StructField("l_linenumber", IntegerType(), True),
                StructField("l_quantity", DecimalType(15, 2), True),
                StructField("l_extendedprice", DecimalType(15, 2), True),
                StructField("l_discount", DecimalType(15, 2), True),
                StructField("l_tax", DecimalType(15, 2), True),
                StructField("l_returnflag", StringType(), True),
                StructField("l_linestatus", StringType(), True),
                StructField("l_shipdate", DateType(), True),
                StructField("l_commitdate", DateType(), True),
                StructField("l_receiptdate", DateType(), True),
                StructField("l_shipinstruct", StringType(), True),
                StructField("l_shipmode", StringType(), True),
                StructField("l_comment", StringType(), True)
            ]),
            
            'nation': StructType([
                StructField("n_nationkey", IntegerType(), True),
                StructField("n_name", StringType(), True),
                StructField("n_regionkey", IntegerType(), True),
                StructField("n_comment", StringType(), True)
            ]),
            
            'orders': StructType([
                StructField("o_orderkey", IntegerType(), True),
                StructField("o_custkey", IntegerType(), True),
                StructField("o_orderstatus", StringType(), True),
                StructField("o_totalprice", DecimalType(15, 2), True),
                StructField("o_orderdate", DateType(), True),
                StructField("o_orderpriority", StringType(), True),
                StructField("o_clerk", StringType(), True),
                StructField("o_shippriority", IntegerType(), True),
                StructField("o_comment", StringType(), True)
            ]),
            
            'part': StructType([
                StructField("p_partkey", IntegerType(), True),
                StructField("p_name", StringType(), True),
                StructField("p_mfgr", StringType(), True),
                StructField("p_brand", StringType(), True),
                StructField("p_type", StringType(), True),
                StructField("p_size", IntegerType(), True),
                StructField("p_container", StringType(), True),
                StructField("p_retailprice", DecimalType(15, 2), True),
                StructField("p_comment", StringType(), True)
            ]),
            
            'partsupp': StructType([
                StructField("ps_partkey", IntegerType(), True),
                StructField("ps_suppkey", IntegerType(), True),
                StructField("ps_availqty", IntegerType(), True),
                StructField("ps_supplycost", DecimalType(15, 2), True),
                StructField("ps_comment", StringType(), True)
            ]),
            
            'region': StructType([
                StructField("r_regionkey", IntegerType(), True),
                StructField("r_name", StringType(), True),
                StructField("r_comment", StringType(), True)
            ]),
            
            'supplier': StructType([
                StructField("s_suppkey", IntegerType(), True),
                StructField("s_name", StringType(), True),
                StructField("s_address", StringType(), True),
                StructField("s_nationkey", IntegerType(), True),
                StructField("s_phone", StringType(), True),
                StructField("s_acctbal", DecimalType(15, 2), True),
                StructField("s_comment", StringType(), True)
            ])
        }

        self.tables = {}

        for table_name, schema in schemas.items():
            file_path = os.path.join(self.data_path, f"{table_name}.tbl")

            df = self.spark.read \
                .option("delimiter", "|") \
                .option("header", "false") \
                .schema(schema) \
                .csv(file_path)
            
            df.createOrReplaceTempView(table_name)
            self.tables[table_name] = df

    def run_query(self, query_num, query_function):
        """Run a single query and return timing and result snippet"""
        
        # TODO: attach the result prints to a verbose flag
        
        print(f"\nRunning query {query_num}")
        start = time.time()
        result_df = query_function(self.spark)

        print("Sample Results:")
        result_df.show(3, truncate=False)

        elapsed = time.time() - start 

        print(f"Time Elapsed: {elapsed:.02f}")

        return elapsed, result_df

    def run_queries(self):

        print("\nRunning queries...")
        print("The queries to run:")
        print(list(queries.QUERIES.keys()))

        times = {}

        for query_num, query_function in queries.QUERIES.items():
            elapsed, _ = self.run_query(query_num, query_function) # temporarily don't store results df
            times[query_num] = elapsed
        
        print("\n-----")
        print("SUMMARY")
        print("Query Number: Elapsed Time")

        for query_num, time in times.items():
            print(f"{query_num}: {time:.2f}")


    def cleanup(self):
        self.spark.stop()

if __name__ == "__main__":
    """
    TODO: add arg options - run all 4 queries or run specific queries
    """
    print("Running run_tpch.py...")

    # Initialize 
    # PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    # print(f"Data directory: {DATA_DIR}")
    benchmark = TPCH(data_path="/mnt/tpch") # for now use default sf 0.1

    # Run queries
    benchmark.run_queries()

    # Cleanup
    benchmark.cleanup()

