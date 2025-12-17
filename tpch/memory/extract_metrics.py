import json
import zstandard as zstd
import os

def read_spark_log(log_file):
    """Read and parse a Spark event log"""
    
    # Decompress if needed
    if log_file.endswith('.zstd'):
        with open(log_file, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            data = dctx.decompress(f.read()).decode('utf-8')
    else:
        with open(log_file, 'r') as f:
            data = f.read()
    
    # Parse events
    events = []
    for line in data.strip().split('\n'):
        if line:
            events.append(json.loads(line))
    
    return events

def analyze_memory_usage(events):
    """Extract memory-related metrics"""
    
    for event in events:
        event_type = event.get('Event')
        
        # Executor memory info
        if event_type == 'SparkListenerExecutorAdded':
            executor_info = event.get('Executor Info', {})
            print(f"Executor Added: {executor_info.get('Total Cores')} cores, "
                  f"{executor_info.get('Total Memory')} bytes memory")
        
        # Task metrics (including memory)
        if event_type == 'SparkListenerTaskEnd':
            metrics = event.get('Task Metrics', {})
            memory_spilled = metrics.get('Memory Bytes Spilled', 0)
            disk_spilled = metrics.get('Disk Bytes Spilled', 0)
            
            if memory_spilled > 0 or disk_spilled > 0:
                print(f"Task spilled to memory: {memory_spilled} bytes, "
                      f"to disk: {disk_spilled} bytes")

# Usage
log_dir = '/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/memory/eventlog_v2_app-20251216115140-0004'
for filename in os.listdir(log_dir):
    if filename.endswith('.zstd'):
        print(f"\n=== Analyzing {filename} ===")
        events = read_spark_log(os.path.join(log_dir, filename))
        analyze_memory_usage(events)