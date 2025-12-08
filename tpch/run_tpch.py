"""
Initialize the spark session
This is called by deploy-tpch.yml
  - load data using load_data.py
  - execute queries from queries.py
  - save results

Right now with ansible, this just collects all the stdout
and prints it in the next task. 

There is a way to do it like "streaming" with the async
module -- do this later. 
https://stackoverflow.com/questions/76218894/how-can-i-get-the-output-of-an-ansible-play-as-it-is-being-run

"""

from pyspark.sql import SparkSession
import config 
import queries
import os
import time 

class TPCH:
    def __init__(self, data_path="/data/", sf=0.1):
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
            'customer': ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 
                        'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'],
            'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
                        'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
                        'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                        'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
            'nation': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment'],
            'orders': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice',
                    'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
            'part': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type',
                    'p_size', 'p_container', 'p_retailprice', 'p_comment'],
            'partsupp': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'],
            'region': ['r_regionkey', 'r_name', 'r_comment'],
            'supplier': ['s_suppkey', 's_name', 's_address', 's_nationkey',
                        's_phone', 's_acctbal', 's_comment']
        }

        self.tables = {}

        for table_name, columns in schemas.items():
            file_path = os.path.join(self.data_path, f"{table_name}.tbl")

            df = self.spark.read \
                .option("delimiter", "|") \
                .option("header", "false") \
                .csv(file_path)
            
            for i, col in enumerate(columns):
                # Rename the col names
                df = df.withColumnRenamed(f"_c{i}", col)
            
            df.createOrReplaceTempView(table_name)
            self.tables[table_name] = df

    def run_query(self, query_num, query_function):
        """Run a single query and return timing and result snippet"""
        
        # TODO: attach the result prints to a verbose flag
        
        print(f"\nRunning query {query_num}")
        start = time.time()
        result_df = query_function(self.spark)

        print("Sample Results:")
        result_df.show(3, trucate=False)

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
    benchmark = TPCH() # for now use default data path and sf

    # Run queries
    benchmark.run_queries()

    # Cleanup
    benchmark.cleanup()

