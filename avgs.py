import pandas as pd

# Define the data
data = [
    ["20260317_235409_4-cores", 5, 108.565, "reg-serial", "768m", 0.45, 0.5, 4, 9.44, 13.55, 22.84, 109.02, 337.91, 187, 2579, 5, 1034],
    ["20260317_235707_3-cores", 5, 100.501, "reg-serial", "768m", 0.45, 0.5, 4, 13.37, 13.55, 26.77, 105.43, 324.56, 193, 2725, 5, 1243],
    ["20260317_235938_2-cores", 5, 130.679, "reg-serial", "768m", 0.45, 0.5, 4, 8.91, 12.93, 21.85, 85.86, 309.71, 46, 1944, 5, 1454],
    ["20260318_000842_default", 5, 111.995, "reg-serial", "768m", 0.45, 0.5, 4, 9.44, 13.55, 22.84, 98.74, 330.63, 197, 2768, 5, 1132],
    ["20260318_001157_4-cores", 5, 101.467, "reg-serial", "768m", 0.45, 0.5, 4, 9.44, 13.55, 22.84, 104.91, 332.6, 201, 2813, 6, 1968],
    ["20260318_001545_3-cores", 5, 100.157, "reg-serial", "768m", 0.45, 0.5, 4, 13.37, 13.55, 26.77, 102.48, 330.82, 214, 3098, 6, 1636],
    ["20260318_001839_4-cores", 5, 100.422, "reg-serial", "768m", 0.45, 0.5, 4, 9.44, 13.55, 22.84, 111.68, 322.19, 181, 2555, 5, 1203],
    ["20260318_002243_3-cores", 5, 97.053, "reg-serial", "768m", 0.45, 0.5, 4, 13.37, 13.55, 26.77, 102.53, 323.2, 196, 2907, 5, 1260],
    ["20260318_002531_4-cores", 5, 99.46, "reg-serial", "768m", 0.45, 0.5, 4, 9.44, 13.55, 22.84, 106.43, 324.81, 178, 2630, 5, 1157],
    ["20260318_002811_3-cores", 5, 98.595, "reg-serial", "768m", 0.45, 0.5, 4, 13.37, 13.55, 26.77, 98.11, 316.68, 214, 2946, 5, 1241]
]

# Create a DataFrame
columns = ['timestamp', 'query', 'elapsed_s', 'jvm_config', 'executor_memory', 'memory_fraction', 'storage_fraction','shuffle_partitions', 'OnHeapExecutionMemory_MB',
               'OnHeapStorageMemory_MB', 'OnHeapUnifiedMemory_MB', 'JVMHeapMemory_MB', 'ProcessTreeJVMRSS_MB', 'MinorGCCount', 'MinorGCTime_ms', 'MajorGCCount', 'MajorGCTime_ms']
df = pd.DataFrame(data, columns=columns)

# print(df)

# Group by type (2-cores, 3-cores, 4-cores, default) and calculate the mean for each column
grouped = df.groupby(df['timestamp'].str.extract(r'_(default|2-cores|3-cores|4-cores)')[8:])
print(grouped)

# # Clean up the result to show the type as the first column
# grouped.reset_index(inplace=True)
# grouped.rename(columns={'index': 'Type'}, inplace=True)

# # Display the result
# print(grouped[['timestamp', 'query', 'elapsed_s', 'jvm_config', 'executor_memory', 'memory_fraction', 'storage_fraction','shuffle_partitions', 'OnHeapExecutionMemory_MB',
#                'OnHeapStorageMemory_MB', 'OnHeapUnifiedMemory_MB', 'JVMHeapMemory_MB', 'ProcessTreeJVMRSS_MB', 'MinorGCCount', 'MinorGCTime_ms', 'MajorGCCount', 'MajorGCTime_ms']])