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
SPARK_EXECUTOR_MEMORY = "500m"

# SPARK_MEMORY_OVERHEAD = "100m"

# Current OS-JVM configuration (file name for results)
CUR_CONFIG = "dietpi-liberica"
CUR_OS = "dietpi"
CUR_JVM = "liberica"

JVM_HEAP_MIN = "1g"
JVM_HEAP_MAX = "1g"

# JVM_GC_ALGORITHM = "SerialGC"
# JVM_GC_ALGORITHM = "ParallelGC"
JVM_GC_ALGORITHM = "Default"

SF = "1"

JVM_CONFIGS = {
    # ========================================
    # BASELINE
    # ========================================
    "default": {
        "name": "default",
        "options": [],
        "description": "JVM defaults (no custom options)",
        "expected": "Baseline for comparison"
    },
    
    # ========================================
    # GARBAGE COLLECTORS
    # ========================================
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
    
    "g1gc-frequent": {
        "name": "g1gc-frequent",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=100",  # More aggressive
            "-XX:G1HeapRegionSize=1m",
            "-XX:InitiatingHeapOccupancyPercent=35",  # Start GC earlier
            "-XX:ParallelGCThreads=4",
            "-XX:ConcGCThreads=2",  # More concurrent threads
            # "-XX:G1NewSizePercent=30",
            # "-XX:G1MaxNewSizePercent=40",
        ],
        "description": "G1GC with aggressive low-latency tuning",
        "expected": "Lower pause times, possibly lower throughput"
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
    
    "serial": {
        "name": "serial",
        "options": [
            "-XX:+UseSerialGC",
        ],
        "description": "Simple serial GC (single-threaded)",
        "expected": "Simplest, predictable, but slower"
    },
    
    # ========================================
    # HEAP PRE-ALLOCATION
    # ========================================
    "heap-min-256": {
        "name": "heap-min-256",
        "options": [
            "-XX:+UseG1GC",
            "-Xms256m",  # Start at 256MB
            # Max is 600MB (set by spark.executor.memory)
        ],
        "description": "Heap grows from 256m to 600m",
        "expected": "Lower initial memory, GC overhead during growth"
    },
    
    "heap-min-400": {
        "name": "heap-400",
        "options": [
            "-XX:+UseG1GC",
            "-Xms400m",  
        ],
        "description": "Pre-allocate 400m heap at startup",
        "expected": "Less growth overhead"
    },

    # "heap-large": {
    #     "name": "heap-large",
    #     "options": [
    #         "-XX:+UseG1GC",
    #         "-Xms500m",  # Start at maximum
    #     ],
    #     "description": "Pre-allocate full 600m heap at startup",
    #     "expected": "No growth overhead, uses memory immediately"
    # },

    "heap-min-max": {
        "name": "heap-max",
        "options": [
            "-XX:+UseG1GC",
            "-Xms500m",  # Start at maximum
        ],
        "description": "Pre-allocate full 600m heap at startup",
        "expected": "No growth overhead, uses memory immediately"
    },
    
    "heap-aggressive-preallocate": {
        "name": "heap-aggressive-preallocate",
        "options": [
            "-XX:+UseG1GC",
            "-Xms600m",
            "-XX:+AlwaysPreTouch",  # Touch all pages at startup
        ],
        "description": "Pre-allocate and touch all memory pages",
        "expected": "Slower startup, best runtime performance"
    },
    
    # ========================================
    # METASPACE TUNING
    # ========================================
    "metaspace-unlimited": {
        "name": "metaspace-unlimited",
        "options": [
            "-XX:+UseG1GC",
            # No metaspace limits
        ],
        "description": "Unlimited metaspace (can grow unbounded)",
        "expected": "Risk of metaspace OOM, but no artificial limits"
    },
    
    "metaspace-128": {
        "name": "metaspace-128",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MetaspaceSize=96m",
            "-XX:MaxMetaspaceSize=128m",
        ],
        "description": "Conservative metaspace limit (128MB)",
        "expected": "Safe for simple queries, may limit complex UDFs"
    },
    
    "metaspace-192": {
        "name": "metaspace-192",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MetaspaceSize=128m",
            "-XX:MaxMetaspaceSize=192m",
        ],
        "description": "Generous metaspace limit (192MB)",
        "expected": "Should handle all TPC-H queries"
    },
    
    "metaspace-256": {
        "name": "metaspace-256",
        "options": [
            "-XX:+UseG1GC",
            "-XX:MetaspaceSize=128m",
            "-XX:MaxMetaspaceSize=256m",
        ],
        "description": "Large metaspace limit (256MB)",
        "expected": "Maximum headroom for class metadata"
    },
    
    # ========================================
    # YOUNG GENERATION TUNING
    # ========================================
    "young-gen-small": {
        "name": "young-gen-small",
        "options": [
            "-XX:+UseG1GC",
            "-XX:NewRatio=3",  # Old:Young = 3:1 (25% young)
        ],
        "description": "Small young generation (25%)",
        "expected": "Fewer young GCs, longer pauses"
    },
    
    "young-gen-large": {
        "name": "young-gen-large",
        "options": [
            "-XX:+UseG1GC",
            "-XX:NewRatio=1",  # Old:Young = 1:1 (50% young)
        ],
        "description": "Large young generation (50%)",
        "expected": "More frequent but shorter young GCs"
    },
    
    # ========================================
    # CODE CACHE TUNING
    # ========================================
    "code-cache-small": {
        "name": "code-cache-small",
        "options": [
            "-XX:+UseG1GC",
            "-XX:ReservedCodeCacheSize=96m",
            "-XX:InitialCodeCacheSize=32m",
        ],
        "description": "Small code cache (96MB)",
        "expected": "May limit JIT compilation"
    },
    
    "code-cache-large": {
        "name": "code-cache-large",
        "options": [
            "-XX:+UseG1GC",
            "-XX:ReservedCodeCacheSize=192m",
            "-XX:InitialCodeCacheSize=64m",
        ],
        "description": "Large code cache (192MB)",
        "expected": "More room for JIT compiled code"
    },
    
    # ========================================
    # COMPILATION STRATEGIES
    # ========================================
    "tiered-c1-only": {
        "name": "tiered-c1-only",
        "options": [
            "-XX:+UseG1GC",
            "-XX:+TieredCompilation",
            "-XX:TieredStopAtLevel=1",  # C1 compiler only
        ],
        "description": "Fast startup with C1 compiler only",
        "expected": "Quick startup, lower peak performance"
    },
    
    "tiered-full": {
        "name": "tiered-full",
        "options": [
            "-XX:+UseG1GC",
            "-XX:+TieredCompilation",
            "-XX:TieredStopAtLevel=4",  # Full C1 + C2
        ],
        "description": "Full tiered compilation (default)",
        "expected": "Balanced startup and peak performance"
    },
    
    "no-tiered-c2": {
        "name": "no-tiered-c2",
        "options": [
            "-XX:+UseG1GC",
            "-XX:-TieredCompilation",
            "-XX:CompileThreshold=1000",
        ],
        "description": "C2 compiler only (no tiering)",
        "expected": "Slower startup, best peak performance"
    },
    
    # ========================================
    # MEMORY OPTIMIZATIONS
    # ========================================
    "string-dedup": {
        "name": "string-dedup",
        "options": [
            "-XX:+UseG1GC",
            "-XX:+UseStringDeduplication",
        ],
        "description": "Enable string deduplication (G1GC only)",
        "expected": "Lower memory for duplicate strings"
    },
    
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
    # COMBINED OPTIMIZATIONS
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
    
    # ========================================
    # OPENJ9 SPECIFIC (if using Eclipse OpenJ9)
    # ========================================
    "openj9-gencon": {
        "name": "openj9-gencon",
        "options": [
            "-Xgcpolicy:gencon",
        ],
        "description": "OpenJ9 generational concurrent GC (default)",
        "expected": "Good general-purpose GC for OpenJ9"
    },
    
    "openj9-balanced": {
        "name": "openj9-balanced",
        "options": [
            "-Xgcpolicy:balanced",
        ],
        "description": "OpenJ9 balanced GC",
        "expected": "Best for heaps <2GB"
    },
    
    "openj9-optthruput": {
        "name": "openj9-optthruput",
        "options": [
            "-Xgcpolicy:optthruput",
        ],
        "description": "OpenJ9 optimize for throughput",
        "expected": "Maximum throughput on OpenJ9"
    },
    
    "openj9-optavgpause": {
        "name": "openj9-optavgpause",
        "options": [
            "-Xgcpolicy:optavgpause",
        ],
        "description": "OpenJ9 optimize for low pause times",
        "expected": "Lowest pauses on OpenJ9"
    },
    
    "openj9-optimized": {
        "name": "openj9-optimized",
        "options": [
            "-Xgcpolicy:balanced",
            "-Xmn256m",  # Young generation
            "-Xtune:virtualized",
            "-Xshareclasses:cacheDir=/tmp",
            "-Xquickstart",
        ],
        "description": "OpenJ9 fully optimized",
        "expected": "Best overall for OpenJ9"
    },
}

# ============================================
# ACTIVE CONFIGURATION
# ============================================
# Change this variable to switch between test configurations
ACTIVE_CONFIG = "g1gc"

# For automated testing, you can override this programmatically

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
    
    # Always add GC logging for analysis
    # Note: Adjust for Java 9+ if needed
    gc_logging = [
        "-XX:+PrintGCDetails",
        "-XX:+PrintGCDateStamps",
        "-XX:+PrintGCTimeStamps",
        "-XX:+PrintGCApplicationStoppedTime",
        f"-Xloggc:/tmp/gc-{ACTIVE_CONFIG}.log",
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
