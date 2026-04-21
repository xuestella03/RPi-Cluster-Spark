import pandas as pd

# Define the data
data = [
    ["20260420_212616",5,41.646,"heap-min-256","768m",0.45,0.5,4,9.44,14.03,23.33,149.57,384.35,78,921,0,0],
    ["20260420_212616",3,35.833,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,189.11,425.95,86,853,0,0],
    ["20260420_212616",1,50.11,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,203.33,450.01,229,1806,0,0],
    ["20260420_212616",6,17.773,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,209.84,461.81,62,508,0,0],
    ["20260420_212616",6,16.142,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,221.07,465.81,64,466,0,0],
    ["20260420_212616",3,33.961,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,239.42,491.72,86,820,1,575],
    ["20260420_212616",1,47.591,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,239.42,491.85,228,1486,0,0],
    ["20260420_212616",5,31.552,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,239.42,496.32,78,713,0,0],
    ["20260420_212616",3,32.168,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,239.42,497.34,86,835,0,0],
    ["20260420_212616",1,48.329,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,239.42,497.34,227,1641,0,0],
    ["20260420_212616",6,18.965,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,244.07,499.99,63,464,0,0],
    ["20260420_212616",5,31.746,"heap-min-256","768m",0.45,0.5,4,10.49,20.84,31.33,244.07,499.99,78,758,1,758],
]

# Create a DataFrame
columns = ['timestamp', 'query', 'elapsed_s', 'jvm_config', 'executor_memory', 'memory_fraction', 'storage_fraction','shuffle_partitions', 'OnHeapExecutionMemory_MB',
               'OnHeapStorageMemory_MB', 'OnHeapUnifiedMemory_MB', 'JVMHeapMemory_MB', 'ProcessTreeJVMRSS_MB', 'MinorGCCount', 'MinorGCTime_ms', 'MajorGCCount', 'MajorGCTime_ms']
df = pd.DataFrame(data, columns=columns)

# print(df)

# Group by type (2-cores, 3-cores, 4-cores, default) and calculate the mean for each column
grouped = df.groupby('query', as_index=False)['elapsed_s'].mean()
print(grouped)

# # Clean up the result to show the type as the first column
# grouped.reset_index(inplace=True)
# grouped.rename(columns={'index': 'Type'}, inplace=True)

# # Display the result
# print(grouped[['timestamp', 'query', 'elapsed_s', 'jvm_config', 'executor_memory', 'memory_fraction', 'storage_fraction','shuffle_partitions', 'OnHeapExecutionMemory_MB',
#                'OnHeapStorageMemory_MB', 'OnHeapUnifiedMemory_MB', 'JVMHeapMemory_MB', 'ProcessTreeJVMRSS_MB', 'MinorGCCount', 'MinorGCTime_ms', 'MajorGCCount', 'MajorGCTime_ms']])