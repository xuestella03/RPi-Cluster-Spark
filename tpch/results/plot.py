import csv
import matplotlib.pyplot as plt
import numpy as np

# Read the averages.csv file
data = {}
queries = []

with open('/home/xuestella03/Documents/Repositories/RPi-Cluster-Spark/tpch/results/averages.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    queries = header[1:]  # Skip 'configuration' column
    
    for row in reader:
        config = row[0]
        values = [float(x) for x in row[1:]]
        
        # Parse OS and JVM
        parts = config.split('-', 1)
        os_name = parts[0]
        jvm = parts[1]
        
        if config not in data:
            data[config] = {
                'os': os_name,
                'jvm': jvm,
                'values': values
            }

# Group by OS and JVM
os_groups = {}
jvm_groups = {}

for config, info in data.items():
    # Group by OS
    if info['os'] not in os_groups:
        os_groups[info['os']] = {}
    os_groups[info['os']][info['jvm']] = info['values']
    
    # Group by JVM
    if info['jvm'] not in jvm_groups:
        jvm_groups[info['jvm']] = {}
    jvm_groups[info['jvm']][info['os']] = info['values']

# Plot 1: For each OS, compare JVMs
for os_name, jvms in os_groups.items():
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(queries))
    width = 0.25
    multiplier = 0
    
    for jvm, values in sorted(jvms.items()):
        offset = width * multiplier
        ax.bar(x + offset, values, width, label=jvm)
        multiplier += 1
    
    ax.set_xlabel('Query')
    ax.set_ylabel('Time (ms)')
    ax.set_title(f'Performance Comparison on {os_name.upper()} - Different JVMs')
    ax.set_xticks(x + width)
    ax.set_xticklabels(queries)
    ax.legend(loc='upper right')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'plot_{os_name}_jvms.png', dpi=300)
    print(f"Saved plot_{os_name}_jvms.png")
    plt.close()

# Plot 2: For each JVM, compare OSes
for jvm, oses in jvm_groups.items():
    fig, ax = plt.subplots(figsize=(10, 6))
    
    x = np.arange(len(queries))
    width = 0.35
    multiplier = 0
    
    for os_name, values in sorted(oses.items()):
        offset = width * multiplier
        ax.bar(x + offset, values, width, label=os_name)
        multiplier += 1
    
    ax.set_xlabel('Query')
    ax.set_ylabel('Time (ms)')
    ax.set_title(f'Performance Comparison of {jvm.upper()} - Different OSes')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels(queries)
    ax.legend(loc='upper right')
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'plot_{jvm}_oses.png', dpi=300)
    print(f"Saved plot_{jvm}_oses.png")
    plt.close()

# Plot 3: All configurations together
fig, ax = plt.subplots(figsize=(14, 7))

x = np.arange(len(queries))
width = 0.12
multiplier = 0

# Sort configurations for consistent ordering
sorted_configs = sorted(data.items(), key=lambda x: (x[1]['os'], x[1]['jvm']))

for config, info in sorted_configs:
    offset = width * multiplier
    label = f"{info['os']}-{info['jvm']}"
    ax.bar(x + offset, info['values'], width, label=label)
    multiplier += 1

ax.set_xlabel('Query')
ax.set_ylabel('Time (ms)')
ax.set_title('Performance Comparison - All Configurations')
ax.set_xticks(x + width * (len(data) - 1) / 2)
ax.set_xticklabels(queries)
ax.legend(loc='upper right', ncol=2)
ax.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('plot_all_configurations.png', dpi=300)
print("Saved plot_all_configurations.png")
plt.close()

print("\nAll plots generated successfully!")