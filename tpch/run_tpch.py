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
import csv 
import socket
import requests
import json
from datetime import datetime

class TPCH:
    def __init__(self, data_path, sf=0.1):
        """
        :param data_path: relative path to .tbl files
        :param sf: scale factor
        """
        self.data_path = data_path
        self.sf = sf 

        jvm_options = config.build_jvm_options_string()

        # self.spark = SparkSession.builder \
        #     .appName("TPC-H Benchmark") \
        #     .master(config.CLUSTER_MASTER_URL) \
        #     .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        #     .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        #     .config("spark.sql.shuffle.partitions", "8") \
        #     .config("spark.eventLog.enabled", True) \
        #     .config("spark.eventLog.dir", "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/memory") \
        #     .config("spark.log.structuredLogging.enabled", True) \
        #     .getOrCreate()
        
        # self.spark = SparkSession.builder \
        #     .appName("TPC-H Benchmark") \
        #     .master(config.CLUSTER_MASTER_URL) \
        #     .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        #     .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        #     .config("spark.sql.shuffle.partitions", "4") \
        #     .config("spark.driver.extraJavaOptions", jvm_options) \
        #     .config("spark.executor.extraJavaOptions", jvm_options) \
        #     .getOrCreate()

        self.spark = SparkSession.builder \
            .appName("TPC-H Benchmark") \
            .master(config.CLUSTER_MASTER_URL) \
            .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
            .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.executor.processTreeMetrics.enabled", "true")  \
            .config("spark.executor.metrics.pollingInterval", "1s") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/memory") \
            .config("spark.driver.extraJavaOptions", jvm_options) \
            .config("spark.executor.extraJavaOptions", jvm_options) \
            .config("spark.memory.storageFraction", config.STORAGE_FRACTION) \
            .config("spark.memory.fraction", config.MEMORY_FRACTION) \
            .getOrCreate()
        

        # Can't do below because you can't set Xmx and Xms through java options 
        # need to use spark.driver.memory
        # self.spark = SparkSession.builder \
        #     .appName("TPC-H Benchmark") \
        #     .master(config.CLUSTER_MASTER_URL) \
        #     .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        #     .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        #     .config("spark.driver.extraJavaOptions", jvm_options) \
        #     .config("spark.executor.extraJavaOptions", jvm_options) \
        #     .config("spark.sql.shuffle.partitions", "8") \
        #     .getOrCreate()
        
        # self.spark = SparkSession.builder \
        #     .appName("TPC-H Benchmark") \
        #     .master(config.CLUSTER_MASTER_URL) \
        #     .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY) \
        #     .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY) \
        #     .config("spark.sql.shuffle.partitions", "8") \
        #     .getOrCreate()

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

    def get_spark_configs(self):
        """Extract relevant Spark and JVM configurations"""
        conf = self.spark.sparkContext.getConf()
        
        configs = {
            'driver.memory': conf.get('spark.driver.memory', 'default'),
            'executor.memory': conf.get('spark.executor.memory', 'default'),
            'driver.extraJavaOptions': conf.get('spark.driver.extraJavaOptions', 'none'),
            'executor.extraJavaOptions': conf.get('spark.executor.extraJavaOptions', 'none'),
            'sql.shuffle.partitions': conf.get('spark.sql.shuffle.partitions', 'default'),
            'master': conf.get('spark.master', 'unknown'),
            'memory.storageFraction': conf.get('spark.memory.storageFraction', 'not working'),
            'memory.fraction': conf.get('spark.memory.fraction', 'not working')
        }
        
        return configs

    def get_jvm_runtime_info(self):
        """Get actual JVM runtime configuration using Java Management API"""
        try:
            from py4j.java_gateway import java_import
            
            jvm = self.spark._jvm
            java_import(jvm, "java.lang.management.*")
            
            runtime_bean = jvm.java.lang.management.ManagementFactory.getRuntimeMXBean()
            memory_bean = jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
            gc_beans = jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()
            
            info = {}
            
            # Heap Memory
            heap_memory = memory_bean.getHeapMemoryUsage()
            info['heap_init_mb'] = heap_memory.getInit() / (1024**2)
            info['heap_max_mb'] = heap_memory.getMax() / (1024**2)
            info['heap_used_mb'] = heap_memory.getUsed() / (1024**2)
            info['heap_committed_mb'] = heap_memory.getCommitted() / (1024**2)
            
            # Non-Heap Memory
            non_heap = memory_bean.getNonHeapMemoryUsage()
            info['non_heap_init_mb'] = non_heap.getInit() / (1024**2)
            info['non_heap_max_mb'] = non_heap.getMax() / (1024**2)
            info['non_heap_used_mb'] = non_heap.getUsed() / (1024**2)
            
            # Garbage Collectors
            gc_info = []
            for i in range(gc_beans.size()):
                gc = gc_beans.get(i)
                gc_info.append({
                    'name': gc.getName(),
                    'count': gc.getCollectionCount(),
                    'time_ms': gc.getCollectionTime()
                })
            info['gc_collectors'] = gc_info
            
            # JVM Arguments - filter for relevant ones
            input_args = runtime_bean.getInputArguments()
            relevant_args = []
            for i in range(input_args.size()):
                arg = input_args.get(i)
                if any(x in arg for x in ['Xms', 'Xmx', 'XX:', 'MaxMetaspace', 'GC', 'NewSize', 'NewRatio', 'Survivor']):
                    relevant_args.append(arg)
            info['jvm_args'] = relevant_args
            
            return info
        except Exception as e:
            print(f"Warning: Could not retrieve JVM runtime info: {e}")
            return None

    def print_config_summary(self):
        """Print current Spark and JVM configuration"""
        print("\n" + "="*60)
        print("SPARK & JVM CONFIGURATION")
        print("="*60)
        
        configs = self.get_spark_configs()
        
        print(f"Master:                    {configs['master']}")
        print(f"Driver Memory:             {configs['driver.memory']}")
        print(f"Executor Memory:           {configs['executor.memory']}")
        print(f"Shuffle Partitions:        {configs['sql.shuffle.partitions']}")
        print(f"\nDriver JVM Options:        {configs['driver.extraJavaOptions']}")
        print(f"Executor JVM Options:      {configs['executor.extraJavaOptions']}")
        print("="*60 + "\n")
        
        # Get actual JVM runtime info
        jvm_info = self.get_jvm_runtime_info()
        if jvm_info:
            print("\n" + "="*60)
            print("ACTUAL JVM RUNTIME CONFIGURATION")
            print("="*60)
            
            print(f"\nHeap Memory:")
            print(f"  Initial (Xms):     {jvm_info['heap_init_mb']:.0f} MB")
            print(f"  Maximum (Xmx):     {jvm_info['heap_max_mb']:.0f} MB")
            print(f"  Currently Used:    {jvm_info['heap_used_mb']:.0f} MB")
            print(f"  Currently Committed: {jvm_info['heap_committed_mb']:.0f} MB")
            
            print(f"\nNon-Heap Memory (Metaspace, Code Cache):")
            print(f"  Maximum:           {jvm_info['non_heap_max_mb']:.0f} MB")
            print(f"  Currently Used:    {jvm_info['non_heap_used_mb']:.0f} MB")
            
            print(f"\nGarbage Collectors in Use:")
            for gc in jvm_info['gc_collectors']:
                print(f"  {gc['name']}")
                if gc['count'] > 0:
                    print(f"    Collections: {gc['count']}, Total Time: {gc['time_ms']} ms")
            
            if jvm_info['jvm_args']:
                print(f"\nRelevant JVM Arguments:")
                for arg in jvm_info['jvm_args']:
                    print(f"  {arg}")
            
            print("="*60 + "\n")

    def get_executor_peak_metrics(self):
        """
        Pull peak executor memory metrics from Spark REST API.
        Returns dict of {executor_id: peakMemoryMetrics}.
        """
        try:
            app_id = self.spark.sparkContext.applicationId
            ui_url = self.spark.sparkContext.uiWebUrl
            
            url = f"{ui_url}/api/v1/applications/{app_id}/executors"
            response = requests.get(url, timeout=10)
            executors = response.json()
            
            results = {}
            for ex in executors:
                ex_id = ex.get("id", "unknown")
                peak = ex.get("peakMemoryMetrics", {})
                if peak:
                    results[ex_id] = {
                        # Unified memory
                        "OnHeapExecutionMemory_MB":  peak.get("OnHeapExecutionMemory", 0) / 1e6,
                        "OnHeapStorageMemory_MB":    peak.get("OnHeapStorageMemory", 0) / 1e6,
                        "OnHeapUnifiedMemory_MB":    peak.get("OnHeapUnifiedMemory", 0) / 1e6,
                        # Total JVM heap
                        "JVMHeapMemory_MB":          peak.get("JVMHeapMemory", 0) / 1e6,
                        "JVMOffHeapMemory_MB":       peak.get("JVMOffHeapMemory", 0) / 1e6,
                        # Process-level (RSS = actual physical RAM used), should match pidstat
                        "ProcessTreeJVMRSS_MB":      peak.get("ProcessTreeJVMRSSMemory", 0) / 1e6,
                        # GC
                        "MinorGCCount":  peak.get("MinorGCCount", 0),
                        "MinorGCTime_ms": peak.get("MinorGCTime", 0),
                        "MajorGCCount":  peak.get("MajorGCCount", 0),
                        "MajorGCTime_ms": peak.get("MajorGCTime", 0),
                    }
            return results
        except Exception as e:
            print(f"Warning: could not fetch executor metrics: {e}")
            return {}
    
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
        
        # Note: if running multiple queries in one app, 
        # will need to take delta of before that query to after 
        metrics = self.get_executor_peak_metrics()
        print(f"Peak Metrics: {json.dumps(metrics, indent=2)}")

        return elapsed, result_df, metrics

    def run_queries(self):

        print("\nRunning queries...")
        print("The queries to run:")
        print(list(queries.QUERIES.keys()))

        results_dir = "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/memory"
        os.makedirs(results_dir, exist_ok=True)
    
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_num = config.CUR_QUERY
        csv_path = os.path.join(
            results_dir,
            f"query{query_num}-{config.ACTIVE_CONFIG}-sf{config.SF}.csv"
        )

        spark_cfg = self.get_spark_configs()

        # These are things we want in the csv 
        # including the things from the spark api
        fieldnames = [
            "timestamp", "query", "elapsed_s", "jvm_config", "executor_memory", "memory_fraction",
            "storage_fraction", "shuffle_partitions","OnHeapExecutionMemory_MB", "OnHeapStorageMemory_MB", "OnHeapUnifiedMemory_MB",
            "JVMHeapMemory_MB", "ProcessTreeJVMRSS_MB",
            "MinorGCCount", "MinorGCTime_ms", "MajorGCCount", "MajorGCTime_ms"
        ]

        times = {}

        with open(csv_path, 'a', newline='') as f:

            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            # Honestly change this because I only every run one at a time now
            for query_num, query_function in queries.QUERIES.items():
                elapsed, _, peaks = self.run_query(query_num, query_function) # temporarily don't store results df
                times[query_num] = elapsed
                row = {
                    "timestamp": timestamp,
                    "query": query_num,
                    "elapsed_s": round(elapsed, 3),
                    "jvm_config": config.ACTIVE_CONFIG,
                    "executor_memory": config.SPARK_EXECUTOR_MEMORY,
                    "memory_fraction": spark_cfg.get("memory.fraction", "0.6"),
                    "storage_fraction": spark_cfg.get("memory.storageFraction", "0.5"), # add later
                    "shuffle_partitions": spark_cfg.get("sql.shuffle.partitions", "4"),
                    **{k: round(peaks.get("0", "1").get(k, 0), 2) for k in fieldnames 
                    if k in peaks.get("0", "1")}
                }
                writer.writerow(row)
                f.flush()  # write after each query in case of crash
            
            print("\n" + "="*60)
            print("BENCHMARK SUMMARY")
            print("="*60)
        
        # Print configuration
        self.print_config_summary()
        
        # Print query times
        print("Query Execution Times:")
        print("-"*60)
        for query_num, elapsed in times.items():
            print(f"  {query_num}: {elapsed:.2f}s")
        print("-"*60)
        print(f"  Total Time: {sum(times.values()):.2f}s")
        print("="*60 + "\n")

        print(f"\nResults saved to: {csv_path}")

        # filename = f"/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/{config.CUR_OS}-jvm/{config.CUR_JVM}-{config.JVM_GC_ALGORITHM}-sf{config.SF}.csv"
        # fieldnames = times.keys()

        # with open(filename, 'a', newline='') as f:
        #     writer = csv.DictWriter(f, fieldnames)
        #     writer.writerow(times)


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
    # benchmark = TPCH(data_path="/mnt/tpch/sf1") # change to path to correct sf
    benchmark = TPCH(data_path=f"/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf{config.SF}")

    # hostname = socket.gethostname()
    # if 'dietpi' in hostname or 'raspberrypi' in hostname:
    #     data_path = "/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf10"
    # else:
    #     data_path = "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf10"

    # benchmark = TPCH(data_path=data_path)
    # Run queries
    benchmark.run_queries()

    # Cleanup
    benchmark.cleanup()

