import csv
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import sys
import tpch.config
import numpy as np

def write_csv(filename, header):
    max_rss = 0
    start = None
    end = None
    with open(filename) as f:
        for line in f:
            if line and line[0].isdigit():
                parts = line.split()
                time = parts[0]
                rss = int(parts[12])
                if start is None:
                    start = datetime.strptime(time, "%H:%M:%S")
                end = datetime.strptime(time, "%H:%M:%S")
                max_rss = max(max_rss, rss)
    runtime = int((end - start).total_seconds())
    return header, max_rss, runtime

def plot_from_csv(filename):
    df = pd.read_csv(filename)
    
    headers = df["header"].tolist()
    memory_mb = (df["maxmem"] / 1024).tolist()  # Convert KB to MB
    runtime = df["runtime"].tolist()
    
    x = np.arange(len(headers))
    width = 0.35  # Width of each bar
    
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()
    
    # Memory bars (left axis) - blue
    bars1 = ax1.bar(x - width/2, memory_mb, width, label='Max Memory (MB)', color='steelblue')
    
    # Runtime bars (right axis) - orange
    bars2 = ax2.bar(x + width/2, runtime, width, label='Runtime (sec)', color='darkorange')
    
    # Labels and title
    ax1.set_xlabel("Configuration", fontsize=12)
    ax1.set_ylabel("Max Memory (MB)", fontsize=12, color='steelblue')
    ax2.set_ylabel("Runtime (seconds)", fontsize=12, color='darkorange')
    ax1.tick_params(axis='y', labelcolor='steelblue')
    ax2.tick_params(axis='y', labelcolor='darkorange')
    
    plt.title("Memory Usage vs Runtime", fontsize=14, pad=20)
    
    # X-axis labels
    ax1.set_xticks(x)
    ax1.set_xticklabels(headers, rotation=45, ha="right")
    
    # Add legends
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    
    fig.tight_layout()
    plt.savefig(f"query{tpch.config.CUR_QUERY}-both.png", dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"Plot saved as query{tpch.config.CUR_QUERY}-both.png")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python plot_stats.py <make_csv?> <input_files>")
        # should make this flag
    
    if sys.argv[1] == "y":
        for filename in sys.argv[2:]:
            header = filename.split("/")[-1].split(".")[0]
            print(header)
            header, max_rss, runtime = write_csv(filename, header)
            with open(
                f"tpch/results/pidstat-results/query{tpch.config.CUR_QUERY}.csv",
                "a",
                newline=""
            ) as f:
                writer = csv.writer(f)
                writer.writerow([header, max_rss, runtime])
    
    # Plot after all data is written
    plot_from_csv(f"tpch/results/pidstat-results/query{tpch.config.CUR_QUERY}.csv")