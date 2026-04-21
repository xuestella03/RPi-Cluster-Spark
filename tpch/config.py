"""
Here I'm configuring it so it's easy to go from local testing
to the actual cluster.

Right now at home I'm testing using my PC as the server and a
single raspberry pi as a worker. 
"""

# SPARK_MASTER = 'local[*]'
# CLUSTER_MASTER_IP = "192.168.50.247"
CLUSTER_MASTER_IP = "192.168.50.65"
CLUSTER_MASTER_URL = f"spark://{CLUSTER_MASTER_IP}:7077"

SPARK_DRIVER_MEMORY = "2g"

# SPARK_EXECUTOR_MEMORY = "640m"
# SPARK_EXECUTOR_MEMORY = "512m"
SPARK_EXECUTOR_MEMORY = "768m"
# SPARK_EXECUTOR_MEMORY = "896m"
# SPARK_EXECUTOR_MEMORY = "450m" # absolute minimum (1.5 * 300)
# SPARK_EXECUTOR_MEMORY = "1g"

SPARK_EXECUTOR_CORES = "4"

# Current OS-JVM configuration (file name for results)
# CUR_CONFIG = "dietpi-liberica-2pis"
CUR_CONFIG = "dietpi-liberica"
CUR_OS = "dietpi"
CUR_JVM = "liberica"
CUR_QUERY = "5"


# ========================================
# More Spark Configs
# ========================================
MEMORY_FRACTION = "0.45"
STORAGE_FRACTION = "0.5"

SF = "0.3"

JVM_CONFIGS = {
    # ========================================
    # Default
    # ========================================
    "default": {
        "name": "default",
        "options": [],
        "description": "JVM defaults (no custom options)",
        "expected": "Baseline for comparison"
    },
    
    # ========================================
    # Max heap size (ignore this)
    # ========================================
    "xmx-400": {
        "name": "xmx-400",
        "options": [
            "-Xmx400m"
        ],
        "description": "Max heap size 400",
        "expected": "Default is ~252"
    },

    # ========================================
    # Reserved code cache size
    # ========================================
    "reserved-code-cache-64": {
        "name": "reserved-code-cache-64",
        "options": [
            "-XX:ReservedCodeCacheSize=64m"
        ],
        "description": "default is 240",
        "expected": ""
    },

    "reserved-code-cache-48": {
        "name": "reserved-code-cache-48",
        "options": [
            "-XX:ReservedCodeCacheSize=48m"
        ],
        "description": "default is 240",
        "expected": ""
    },

    "reserved-code-cache-128": {
        "name": "reserved-code-cache-128",
        "options": [
            "-XX:ReservedCodeCacheSize=128m"
        ],
        "description": "default is 240",
        "expected": ""
    },

    "reserved-code-cache-240": {
        "name": "reserved-code-cache-240",
        "options": [
            "-XX:ReservedCodeCacheSize=240m"
        ],
        "description": "default is 240",
        "expected": ""
    },

    # ========================================
    # RAM
    # ========================================
    "ram-95": {
        "name": "ram-95",
        "options": [
            "-XX:MaxRAM=640m",
            "-XX:MaxRAMPercentage=90.0"
        ],
        "description": "",
        "expected": ""
    },
    
    "ram-90": {
        "name": "ram-90",
        "options": [
            "-XX:MaxRAM=640m",
            "-XX:MaxRAMPercentage=90.0"
        ],
        "description": "",
        "expected": ""
    },

    "ram-70": {
        "name": "ram-70",
        "options": [
            "-XX:MaxRAM=640m",
            "-XX:MaxRAMPercentage=70.0"
        ],
        "description": "",
        "expected": ""
    },

    "ram-50": {
        "name": "ram-50",
        "options": [
            "-XX:MaxRAM=640m",
            "-XX:MaxRAMPercentage=50.0"
        ],
        "description": "",
        "expected": ""
    },
    
    # ========================================
    # GC
    # ========================================
    "reg-g1gc": {
        "name": "reg-g1gc",
        "options": [
            "-XX:+UseG1GC",
        ],
        "description": "G1GC optimized for 500MB heap",
        "expected": "Good balance of throughput and pause times"
    },

    # "reg-g1gc": {
    #     "name": "reg-g1gc",
    #     "options": [
    #         "-XX:+UseG1GC",
    #     ],
    #     "description": "",
    #     "expected": ""
    # },

    "g1gc": {
        "name": "g1gc",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=200",
            "-XX:G1HeapRegionSize=1m",
            "-XX:InitiatingHeapOccupancyPercent=45",
            "-XX:ParallelGCThreads=4",
            "-XX:ConcGCThreads=1",
        ],
        "description": "G1GC optimized for 600MB heap",
        "expected": "Good balance of throughput and pause times"
    },
    
    "parallel": {
        "name": "parallel",
        "options": [
            "-XX:+UseParallelGC",
            "-XX:ParallelGCThreads=4",
            "-XX:MaxGCPauseMillis=200",
            "-XX:GCTimeRatio=19",
        ],
        "description": "Parallel GC for maximum throughput",
        "expected": "Best throughput, higher pause times"
    },

    "reg-parallel": {
        "name": "reg-parallel",
        "options": [
            "-XX:+UseParallelGC",
        ],
        "description": "Parallel GC for maximum throughput",
        "expected": "Best throughput, higher pause times"
    },
    
    "reg-serial": {
        "name": "reg-serial",
        "options": [
            "-XX:+UseSerialGC",
        ],
        "description": "Simple serial GC (single-threaded)",
        "expected": "Simplest, predictable, but slower"
    },

    "epsilon": {
        "name": "epsilon",
        "options": [
            "-XX:+UnlockExperimentalVMOptions",
            "-XX:+UseEpsilonGC",
        ],
        "description": "",
        "expected": "S"
    },
    
    "two-thread-parallel": {
        "name": "reg-parallel",
        "options": [
            "-XX:+UseParallelGC",
            "-XX:ParallelGCThreads=2",
        ],
        "description": "Parallel GC for maximum throughput",
        "expected": "Best throughput, higher pause times"
    },

    "one-thread-parallel": {
        "name": "reg-parallel",
        "options": [
            "-XX:+UseParallelGC",
            "-XX:ParallelGCThreads=1",
        ],
        "description": "Parallel GC for maximum throughput",
        "expected": "Best throughput, higher pause times"
    },

    # ========================================
    # Initial heap size
    # ========================================
    "heap-min-0": {
        "name": "heap-min-0",
        "options": [
            "-XX:+UseSerialGC",
            "-Xms0m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },

    "heap-min-0-any-gc": {
        "name": "heap-min-0-any-gc",
        "options": [
            "-Xms0m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },

    "heap-min-64": {
        "name": "heap-min-64",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms64m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },
    
    "heap-min-128": {
        "name": "heap-min-128",
        "options": [
            "-XX:+UseSerialGC",
            "-Xms128m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },

    "heap-min-128-additional": {
        "name": "heap-min-128-additional",
        "options": [
            "-XX:+UseParallelGC",
            "-XX:ParallelGCThreads=4",
            "-XX:MaxGCPauseMillis=200",
            "-XX:GCTimeRatio=19",
            "-Xms128m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },

    "heap-min-128-any-gc": {
        "name": "heap-min-128-any-gc",
        "options": [
            # "-XX:+UseG1GC",
            "-Xms128m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },
    
    "heap-min-256": {
        "name": "heap-min-256",
        "options": [
            "-XX:+UseSerialGC",
            "-Xms256m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },

    "heap-min-256-any-gc": {
        "name": "heap-min-256-any-gc",
        "options": [
            # "-XX:+UseG1GC",
            "-Xms256m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },
    
    "heap-min-384": {
        "name": "heap-384",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms384m",  
        ],
        "description": "Pre-allocate 400m heap at startup",
        "expected": "Less growth overhead"
    },

    "heap-min-384-any-gc": {
        "name": "heap-384-any-gc",
        "options": [
            # "-XX:+UseG1GC",
            "-Xms384m",  
        ],
        "description": "Pre-allocate 400m heap at startup",
        "expected": "Less growth overhead"
    },


    "heap-min-512": {
        "name": "heap-512",
        "options": [
            "-XX:+UseSerialGC",
            "-Xms512m",  
        ],
        "description": "Pre-allocate 400m heap at startup",
        "expected": "Less growth overhead"
    },

    "heap-min-512-any-gc": {
        "name": "heap-512-any-gc",
        "options": [
            # "-XX:+UseG1GC",
            "-Xms512m",  
        ],
        "description": "Pre-allocate 400m heap at startup",
        "expected": "Less growth overhead"
    },

    "heap-min-max": {
        "name": "heap-max",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms768",  # Start at maximum
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },

    "heap-min-max-larger": {
        "name": "heap-max-larger",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms768m",  # Start at maximum
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },
    

    "reserved-code-cache-64": {
        "name": "reserved-code-cache-64",
        "options": [
            "-XX:+UseSerialGC",
            "-XX:ReservedCodeCacheSize=64m",
            "-XX:+PrintCodeCache"
        ],
        "description": "",
        "expected": ""
    },

    "reserved-code-cache-48": {
        "name": "reserved-code-cache-48",
        "options": [
            "-XX:+UseSerialGC",
            "-XX:ReservedCodeCacheSize=48m",
            "-XX:+PrintCodeCache"
        ],
        "description": "",
        "expected": ""
    },


    # ========================================
    # Metaspace
    # ========================================
    "metaspace-unlimited": {
        "name": "metaspace-unlimited",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m",
            # No metaspace limits
        ],
        "description": "Unlimited metaspace (can grow unbounded), this is just G1GC with no extra configs",
        "expected": "Risk of metaspace OOM, but no artificial limits"
    },
    
    "metaspace-128": {
        "name": "metaspace-128",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m",
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=128m",
        ],
        "description": "Conservative metaspace limit (128MB)",
        "expected": "Safe for simple queries, may limit complex UDFs"
    },
    
    "metaspace-192": {
        "name": "metaspace-192",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m", # comment out for q9
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=192m",
        ],
        "description": "Generous metaspace limit (192MB)",
        "expected": "Should handle all TPC-H queries"
    },
    
    "metaspace-256": {
        "name": "metaspace-256",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m", 
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=256m",
        ],
        "description": "Large metaspace limit (256MB)",
        "expected": "Maximum headroom for class metadata"
    },
    
    # ========================================
    # Compilation
    # ========================================

    "tiered-full": {
        "name": "tiered-full",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m", 
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=256m",
            # "-XX:+TieredCompilation",
            # "-XX:TieredStopAtLevel=4",  # Full C1 + C2
        ],
        "description": "Full tiered compilation (default)",
        "expected": "Balanced startup and peak performance"
    },
    

    "tiered-c1-only": {
        "name": "tiered-c1-only",
        "options": [
            "-XX:+UseSerialGC",
            # "-Xms128m", 
            # "-XX:MetaspaceSize=96m",
            # "-XX:MaxMetaspaceSize=256m",
            # "-XX:+TieredCompilation",
            "-XX:TieredStopAtLevel=1",  # C1 compiler only
        ],
        "description": "Fast startup with C1 compiler only",
        "expected": "Quick startup, lower peak performance"
    },

    "c2-only": {
        "name": "c2-only",
        "options": [
            "-XX:+UseSerialGC",
            "-XX:-TieredCompilation",
        ],
        "description": "",
        "expected": ""
    },

    

    "no-tiered-c2": {
        "name": "no-tiered-c2",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m", 
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=256m",
            "-XX:-TieredCompilation",
        ],
        "description": "C2 compiler only (no tiering)",
        "expected": "Slower startup, best peak performance"
    },
    
    # ========================================
    # Memory optimizations
    # ========================================
    "compressed-oops": {
        "name": "compressed-oops",
        "options": [
            "-XX:+UseG1GC",
            "-XX:+UseCompressedOops",
            "-XX:+UseCompressedClassPointers",
        ],
        "description": "Compressed object pointers (usually default)",
        "expected": "Small memory savings"
    },
    
    # ========================================
    # Combined
    # ========================================
    "optimized-throughput": {
        "name": "optimized-throughput",
        "options": [
            "-XX:+UseParallelGC",
            "-XX:ParallelGCThreads=4",
            "-Xms600m",
            "-XX:+AlwaysPreTouch",
            "-XX:MetaspaceSize=128m",
            "-XX:MaxMetaspaceSize=192m",
            "-XX:ReservedCodeCacheSize=192m",
        ],
        "description": "Optimized for maximum throughput",
        "expected": "Best query execution time"
    },
    
    "optimized-latency": {
        "name": "optimized-latency",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=100",
            "-XX:G1HeapRegionSize=1m",
            "-Xms600m",
            "-XX:+UseStringDeduplication",
            "-XX:MetaspaceSize=128m",
            "-XX:MaxMetaspaceSize=192m",
        ],
        "description": "Optimized for low GC pauses",
        "expected": "Predictable performance, minimal pauses"
    },
    
    "optimized-memory": {
        "name": "optimized-memory",
        "options": [
            "-XX:+UseG1GC",
            "-XX:+UseStringDeduplication",
            "-XX:+UseCompressedOops",
            "-XX:MaxMetaspaceSize=128m",
            "-XX:ReservedCodeCacheSize=96m",
            "-XX:+AggressiveHeap",
        ],
        "description": "Optimized to minimize memory footprint",
        "expected": "Lowest memory usage"
    },
    
    "final": {
        "name": "final",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms768m",  # Start at maximum
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },

    "final2": {
        "name": "final2",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m", 
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },

    "final3": {
        "name": "final3",
        "options": [
            "-XX:+UseParallelGC",
            "-Xms128m",  
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=256m",
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },

    "final4": {
        "name": "final4",
        "options": [
            "-XX:+UseSerialGC",
            "-Xms128m", 
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=256m",
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },

    "serial-no-jit": {
        "name": "reg-serial",
        "options": [
            "-XX:+UseSerialGC",
            "-Xint"
        ],
        "description": "Simple serial GC (single-threaded)",
        "expected": "Simplest, predictable, but slower"
    },

}

# ============================================
# ACTIVE CONFIGURATION
# ============================================
# Configurations variable
ACTIVE_CONFIG = "c2-only"
# ACTIVE_CONFIG = "final4"
# ACTIVE_CONFIG = "final3"

# ============================================
# HELPER FUNCTIONS
# ============================================

def build_jvm_options_string():
    """
    Build JVM options string from the active configuration.
    Automatically adds GC logging for analysis.
    """
    if ACTIVE_CONFIG not in JVM_CONFIGS:
        raise ValueError(f"Unknown config: {ACTIVE_CONFIG}. Available: {list(JVM_CONFIGS.keys())}")
    
    config = JVM_CONFIGS[ACTIVE_CONFIG]
    options = config["options"].copy()
    
    # Add GC and other logging Logging
    gc_logging = [
        "-XX:+PrintGCDetails",
        "-XX:+PrintGCDateStamps",
        "-XX:+PrintGCTimeStamps",
        "-XX:+PrintGCApplicationStoppedTime",
        # f"-Xloggc:/tmp/gc-{ACTIVE_CONFIG}.log",
        # "-XX:+PrintFlagsFinal",
    ]
    options.extend(gc_logging)
    
    print(f"\n{'='*60}")
    print(f"JVM Configuration: {config['name']}")
    print(f"{'='*60}")
    print(f"Description: {config['description']}")
    print(f"Expected: {config['expected']}")
    print(f"Options: {' '.join(options)}")
    print(f"{'='*60}\n")
    
    return " ".join(options)

# def build_jvm_options_string():
#     # Fix later for more parameters
    
#     options = []
#     # options.append(f"-Xms{JVM_HEAP_MIN}")
#     # options.append(f"-Xmx{JVM_HEAP_MAX}")
#     # options.append(f"-XX:+Use{JVM_GC_ALGORITHM}")
#     print(options)

#     return " ".join(options)
