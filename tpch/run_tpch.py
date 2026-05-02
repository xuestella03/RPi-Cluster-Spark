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
import random

class TPCH:
    def __init__(self, data_path, sf=0.1):
        """
        :param data_path: relative path to .tbl files
        :param sf: scale factor
        """
        self.data_path = data_path
        self.sf = sf 

        jvm_options = config.build_jvm_options_string()

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
            .config("spark.executor.cores", config.SPARK_EXECUTOR_CORES) \
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

    def get_executor_memory_metrics(self):
        """
        Pull memory metrics from Spark REST API. 
        These are snapshots, so we run this before and after we force GC. 
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
                mem = ex.get("memoryMetrics", {})
                if mem:
                    results[ex_id] = {
                        "usedOnHeapStorageMemory_MB":  mem.get("usedOnHeapStorageMemory", 0) / 1e6,
                        "totalOnHeapStorageMemory_MB":  mem.get("totalOnHeapStorageMemory", 0) / 1e6,
                    }
            return results
        except Exception as e:
            print(f"Warning: could not fetch executor metrics: {e}")
            return {}
    
    def get_executor_peak_metrics(self):
        """
        Pull peak executor memory metrics from Spark REST API.
        Returns dict of {executor_id: {metric_name: value}}.
        Note: these are CUMULATIVE since app start — use snapshot_and_delta()
        to get per-run values.
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
                        # Unified memory (these are true peaks — take max, not delta)
                        "OnHeapExecutionMemory_MB":  peak.get("OnHeapExecutionMemory", 0) / 1e6,
                        "OnHeapStorageMemory_MB":    peak.get("OnHeapStorageMemory", 0) / 1e6,
                        "OnHeapUnifiedMemory_MB":    peak.get("OnHeapUnifiedMemory", 0) / 1e6,
                        # Total JVM heap (true peak — take max)
                        "JVMHeapMemory_MB":          peak.get("JVMHeapMemory", 0) / 1e6,
                        "JVMOffHeapMemory_MB":       peak.get("JVMOffHeapMemory", 0) / 1e6,
                        # Process-level RSS (true peak — take max)
                        "ProcessTreeJVMRSS_MB":      peak.get("ProcessTreeJVMRSSMemory", 0) / 1e6,
                        # GC counts/times (cumulative counters — must delta these)
                        "MinorGCCount":   peak.get("MinorGCCount", 0),
                        "MinorGCTime_ms": peak.get("MinorGCTime", 0),
                        "MajorGCCount":   peak.get("MajorGCCount", 0),
                        "MajorGCTime_ms": peak.get("MajorGCTime", 0),
                    }
            return results
        except Exception as e:
            print(f"Warning: could not fetch executor metrics: {e}")
            return {}

    def aggregate_peak_metrics(self, metrics_before, metrics_after):
        """
        Compute per-run metrics by combining a before-snapshot and after-snapshot.

        Memory metrics (OnHeap*, JVMHeap*, RSS):
            These are true high-water marks tracked by Spark since app start.
            They never decrease. So the "after" value IS the peak for the run —
            we just take max across executors. We do NOT delta these because a
            lower peak in a later run would give a negative delta, which is wrong.

        GC metrics (MinorGCCount, MinorGCTime_ms, MajorGCCount, MajorGCTime_ms):
            These are cumulative counters, so we subtract the before from the after
            to get the GC for that run specifically. We also need to sum across the 
            executors (GC happens independently on each executor).

        Returns a flat dict ready for CSV writing.
        """
        MEMORY_KEYS = {
            "OnHeapExecutionMemory_MB",
            "OnHeapStorageMemory_MB",
            "OnHeapUnifiedMemory_MB",
            "JVMHeapMemory_MB",
            # "JVMOffHeapMemory_MB",
            "ProcessTreeJVMRSS_MB",
        }
        GC_KEYS = {
            "MinorGCCount",
            "MinorGCTime_ms",
            "MajorGCCount",
            "MajorGCTime_ms",
        }
        ALL_KEYS = MEMORY_KEYS | GC_KEYS

        # Work only with worker executors (exclude driver)
        def worker_only(metrics):
            filtered = {eid: m for eid, m in metrics.items() if eid != "driver"}
            return filtered if filtered else metrics  # fallback if only driver exists

        after_workers = worker_only(metrics_after)
        before_workers = worker_only(metrics_before)

        if not after_workers:
            print("WARNING: No executor peak metrics found.")
            return {k: None for k in ALL_KEYS}

        result = {}

        for k in MEMORY_KEYS:
            # True peak since app start — take max across executors from the AFTER snapshot.
            # The before snapshot is irrelevant for memory peaks.
            vals = [m.get(k, 0) for m in after_workers.values() if m.get(k) is not None]
            result[k] = max(vals) if vals else None

        for k in GC_KEYS:
            # Cumulative counter — delta each executor then sum.
            # This gives total GC activity that happened during this specific run.
            total = 0
            for eid, after_vals in after_workers.items():
                after_val = after_vals.get(k, 0)
                before_val = before_workers.get(eid, {}).get(k, 0)
                total += max(0, after_val - before_val)  # max(0,...) guards against clock skew
            result[k] = total

        return result

    def force_gc(self):
        """Force GC on driver and all executors via JVM."""
        import time

        # --- Driver GC ---
        self.spark._jvm.java.lang.System.gc()

        # --- Executor GC via a lightweight Spark task ---
        sc = self.spark.sparkContext

        num_executors = max(
            len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1,
            1
        )

        def run_gc_on_executor(partition_iter):
            """
            Runs inside the executor's Python worker.
            We call System.gc() via SparkEnv's RpcEnv — but the simplest
            reliable path is just letting the JVM side handle it via
            a small Java call through the existing worker gateway.
            """
            import ctypes
            import gc

            print("Attempting to force GC")

            # Python-side GC (frees Python objects on executor)
            gc.collect()

            # Trigger JVM GC on the executor via jvm handle available in workers
            try:
                from pyspark import SparkContext
                jvm = SparkContext._jvm
                if jvm is not None:
                    print("SparkContext._jvm is not None; running System.gc()")
                    jvm.java.lang.System.gc()
            except Exception:
                print("Failed to run System.gc()")
                pass  # Best-effort; don't crash the worker

            yield 1

        sc.parallelize(range(num_executors), num_executors) \
            .mapPartitions(run_gc_on_executor) \
            .collect()

        time.sleep(2)
        print("[GC] Forced GC on driver and all executors")

    def unpersist_broadcast(self):
        """
        Spark's storage memory has cached RDDs/DFs and broadcast variables. clearCache() will clear
        cached RDDs and DFs, but broadcast variables are not evicted. 
        """
        sc = self.spark.sparkContext
        for broadcast in sc._jsc.sc().broadcastManager().cachedValues().values():
            try:
                broadcast.unpersist()

            except:
                pass 

    def force_executor_gc_and_measure(self):
        sc = self.spark.sparkContext
        num_executors = max(
            len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1,
            1
        )

        # Pass 1: collect PIDs from executors
        def collect_pid(partition_iter):
            list(partition_iter)
            import os
            yield (
                os.environ.get("SPARK_EXECUTOR_ID", "unknown"),
                os.uname().nodename,
                os.getpid(),
            )

        pid_list = (
            sc.parallelize(range(num_executors), num_executors)
            .mapPartitions(collect_pid)
            .collect()
        )

        # Pass 2: run jcmd on each executor's own machine in a separate task
        def measure_and_gc(partition_iter):
            import subprocess, time, os

            items = list(partition_iter)
            if not items:
                return
            executor_id, hostname, pid = items[0]

            def heap_mb():
                try:
                    r = subprocess.run(
                        ["jcmd", str(pid), "GC.heap_info"],
                        capture_output=True, text=True, timeout=10
                    )
                    used_kb = sum(
                        int(p[5:-1]) for p in r.stdout.split()
                        if p.startswith("used=") and p.endswith("K")
                    )
                    return used_kb / 1024
                except Exception as e:
                    return -1.0

            def rss_mb():
                try:
                    with open(f"/proc/{pid}/status") as f:
                        for line in f:
                            if line.startswith("VmRSS:"):
                                return int(line.split()[1]) / 1024
                except Exception:
                    pass
                return 0.0

            before = heap_mb()
            rss_before = rss_mb()

            subprocess.run(
                ["jcmd", str(pid), "GC.run"],
                capture_output=True, text=True, timeout=20
            )
            time.sleep(2)

            after = heap_mb()
            rss_after = rss_mb()

            yield {
                "executor_id": executor_id,
                "hostname":    hostname,
                "heap_before": round(before, 1),
                "heap_after":  round(after, 1),
                "freed":       round(before - after, 1),
                "rss_before":  round(rss_before, 1),
                "rss_after":   round(rss_after, 1),
            }

        results = (
            sc.parallelize(pid_list, len(pid_list))
            .mapPartitions(measure_and_gc)
            .collect()
        )

        # Driver GC via Py4J (this part works fine since driver is local)
        import time
        from py4j.java_gateway import java_import
        jvm = self.spark._jvm
        java_import(jvm, "java.lang.management.*")
        memory_bean = jvm.java.lang.management.ManagementFactory.getMemoryMXBean()

        driver_before = memory_bean.getHeapMemoryUsage().getUsed() / 1e6
        jvm.java.lang.System.gc()
        time.sleep(1)
        driver_after = memory_bean.getHeapMemoryUsage().getUsed() / 1e6

        print(f"\n{'='*75}")
        print(f"  GC REPORT")
        print(f"{'='*75}")
        print(f"  {'Node':<20} {'Heap Before':>12} {'Heap After':>12} {'Freed':>10} {'RSS Before':>11} {'RSS After':>10}")
        print(f"  {'-'*75}")
        print(f"  {'driver':<20} {driver_before:>11.1f}m {driver_after:>11.1f}m {driver_before-driver_after:>9.1f}m {'n/a':>11} {'n/a':>10}")
        for r in sorted(results, key=lambda x: x["executor_id"]):
            print(f"  {r['executor_id']} {r['hostname']:<20} {r['heap_before']:>11.1f}m {r['heap_after']:>11.1f}m {r['freed']:>9.1f}m {r['rss_before']:>10.1f}m {r['rss_after']:>9.1f}m")
        print(f"{'='*75}\n")


    # def force_executor_gc_and_measure(self):
    #     sc = self.spark.sparkContext
    #     num_executors = max(
    #         len(sc._jsc.sc().statusTracker().getExecutorInfos()) - 1,
    #         1
    #     )

    #     # Step 1: collect executor PIDs and RSS — no jcmd yet
    #     def collect_pid_and_rss(partition_iter):
    #         list(partition_iter)
    #         import os

    #         pid = os.getpid()
    #         executor_id = os.environ.get("SPARK_EXECUTOR_ID", "unknown")
    #         hostname = os.uname().nodename

    #         def read_rss_mb():
    #             with open(f"/proc/{pid}/status") as f:
    #                 for line in f:
    #                     if line.startswith("VmRSS:"):
    #                         return int(line.split()[1]) / 1024
    #             return 0.0

    #         yield {
    #             "executor_id": executor_id,
    #             "hostname": hostname,
    #             "pid": pid,
    #             "rss_mb": round(read_rss_mb(), 1),
    #         }

    #     executor_info = (
    #         sc.parallelize(range(num_executors), num_executors)
    #         .mapPartitions(collect_pid_and_rss)
    #         .collect()
    #     )

    #     # Step 2: run jcmd FROM THE DRIVER against each executor's PID
    #     # This is safe because jcmd attaches externally, not from within the task
    #     import subprocess

    #     def jcmd_heap_mb(pid):
    #         try:
    #             result = subprocess.run(
    #                 ["jcmd", str(pid), "GC.heap_info"],
    #                 capture_output=True, text=True, timeout=10
    #             )
    #             used_kb = 0
    #             for part in result.stdout.split():
    #                 if part.startswith("used=") and part.endswith("K"):
    #                     used_kb += int(part[5:-1])
    #             return used_kb / 1024
    #         except Exception as e:
    #             print(f"  [jcmd heap_info error pid={pid}]: {e}")
    #             return 0.0

    #     def jcmd_run_gc(pid):
    #         try:
    #             subprocess.run(
    #                 ["jcmd", str(pid), "GC.run"],
    #                 capture_output=True, text=True, timeout=20
    #             )
    #         except Exception as e:
    #             print(f"  [jcmd GC.run error pid={pid}]: {e}")

    #     # Step 3: for each executor, measure heap before, force GC, measure after
    #     import time
    #     from py4j.java_gateway import java_import
    #     jvm = self.spark._jvm
    #     java_import(jvm, "java.lang.management.*")
    #     memory_bean = jvm.java.lang.management.ManagementFactory.getMemoryMXBean()

    #     driver_before = memory_bean.getHeapMemoryUsage().getUsed() / 1e6
    #     jvm.java.lang.System.gc()
    #     time.sleep(1)
    #     driver_after = memory_bean.getHeapMemoryUsage().getUsed() / 1e6

    #     results = []
    #     for info in executor_info:
    #         pid = info["pid"]
    #         heap_before = jcmd_heap_mb(pid)
    #         rss_before = info["rss_mb"]

    #         jcmd_run_gc(pid)
    #         time.sleep(2)

    #         heap_after = jcmd_heap_mb(pid)

    #         # RSS after GC — run another tiny task to read it
    #         # (or just note that RSS rarely drops after GC since OS doesn't reclaim immediately)
    #         results.append({
    #             "executor_id": info["executor_id"],
    #             "hostname":    info["hostname"],
    #             "heap_before": round(heap_before, 1),
    #             "heap_after":  round(heap_after, 1),
    #             "freed":       round(heap_before - heap_after, 1),
    #             "rss_before":  rss_before,
    #         })

    #     print(f"\n{'='*70}")
    #     print(f"  GC REPORT")
    #     print(f"{'='*70}")
    #     print(f"  {'Node':<20} {'Heap Before':>12} {'Heap After':>12} {'Freed':>10} {'RSS':>8}")
    #     print(f"  {'-'*70}")
    #     print(f"  {'driver':<20} {driver_before:>11.1f}m {driver_after:>11.1f}m {driver_before-driver_after:>9.1f}m {'n/a':>8}")
    #     for r in sorted(results, key=lambda x: x["executor_id"]):
    #         print(f"  {r['hostname']:<20} {r['heap_before']:>11.1f}m {r['heap_after']:>11.1f}m {r['freed']:>9.1f}m {r['rss_before']:>7.1f}m")
    #     print(f"{'='*70}\n")


    def run_query(self, query_num, query_function):
        """
        Run a single query.
        
        Snapshots metrics before and after execution so the caller can compute
        per-run deltas. Returns (elapsed_s, result_df, metrics_before, metrics_after).
        """
        print(f"\nRunning query {query_num}")

        # Snapshot BEFORE -- captures cumulative state up to this point
        metrics_before = self.get_executor_peak_metrics()

        start = time.time()
        result_df = query_function(self.spark)
        result_df.show(3, truncate=False)  # triggers actual execution
        elapsed = time.time() - start

        # Snapshot AFTER
        metrics_after = self.get_executor_peak_metrics()

        print(f"Time Elapsed: {elapsed:.2f}s")

        return elapsed, result_df, metrics_before, metrics_after

    def run_queries(self):
        """
        Run a warmup query.

        for i in range(number of times we run everything):
            for q in random order of queries:
                run and collect data 
                compute values delta -- note that this 
                clear cache after the q runs
        take mean over i iterations for each run

        """
        print("\nRunning queries...")
        print("The queries to run:", list(queries.QUERIES.keys()))

        results_dir = "/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/memory"
        os.makedirs(results_dir, exist_ok=True)
    
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_num_label = config.CUR_QUERY
        csv_path = os.path.join(
            results_dir,
            f"{timestamp}-{config.ACTIVE_CONFIG}-sf{config.SF}.csv"
        )

        spark_cfg = self.get_spark_configs()

        fieldnames = [
            "timestamp", "run", "query", "elapsed_s", "jvm_config", "executor_memory",
            "memory_fraction", "storage_fraction", "shuffle_partitions",
            "OnHeapExecutionMemory_MB", "OnHeapStorageMemory_MB", "OnHeapUnifiedMemory_MB",
            "JVMHeapMemory_MB", "ProcessTreeJVMRSS_MB",
            "MinorGCCount", "MinorGCTime_ms", "MajorGCCount", "MajorGCTime_ms"
        ]

        times = {}

        with open(csv_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            # Before running any actual things we need to do a warmup that'll consist mostly of the disk i/o
            print(f"Running warmup")
            # index = random.randint(0, 3)
            index = 0
            query_num, query_function = list(queries.QUERIES.items())[index]
            self.run_query(query_num, query_function)
            self.spark.catalog.clearCache()
            # TODO: actually record this so we can have the timing to map to pidstat

            # For now we'll do 5 iterations
            for i in range(2):
                
                # Randomize queries 
                shuffled = random.sample(list(queries.QUERIES.items()), k=len(queries.QUERIES))

                for query_num, query_function in shuffled:
                    memory_before = self.get_executor_memory_metrics()
                    print(f"Memory before: {memory_before['0']}")
                    print(f"Running iteration {i}: query {query_num}")
                    run_elapsed, _, before_run, after_run = self.run_query(query_num, query_function)
                    run_peaks = self.aggregate_peak_metrics(before_run, after_run)
                    run_row = {
                        "timestamp": timestamp,
                        "query": query_num,
                        "elapsed_s": round(run_elapsed, 3),
                        "jvm_config": config.ACTIVE_CONFIG,
                        "executor_memory": config.SPARK_EXECUTOR_MEMORY,
                        "memory_fraction": spark_cfg.get("memory.fraction", "0.6"),
                        "storage_fraction": spark_cfg.get("memory.storageFraction", "0.5"),
                        "shuffle_partitions": spark_cfg.get("sql.shuffle.partitions", "4"),
                        **{k: round(v, 2) if v is not None else "" for k, v in run_peaks.items()}
                    }

                    writer.writerow(run_row)
                    f.flush()
                    
                    memory_after = self.get_executor_memory_metrics()
                    print(f"Memory after: {memory_after['0']}")

                    self.spark.catalog.clearCache()
                    # self.force_gc()
                    # self.unpersist_broadcast()
                    time.sleep(3) # in case clearCache and forcing gc aren't instantaneous

                    memory_after_clearing = self.get_executor_memory_metrics()
                    print(f"Memory after clearing: {memory_after_clearing['0']}")

    def cleanup(self):
        self.spark.stop()


if __name__ == "__main__":
    print("Running run_tpch.py...")
    benchmark = TPCH(data_path=f"/home/dietpi/Documents/Repositories/RPi-Cluster-Spark/tpch/data/sf{config.SF}")
    benchmark.run_queries()
    benchmark.cleanup()