import csv
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import sys
import tpch.config
import numpy as np

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
    plot_name = filename.split(".")[0]
    plt.savefig(f"{plot_name}.png", dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    print(f"Plot saved as {plot_name}.png")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python plot_from_csv.py <input_csv>")

    input_csv = sys.argv[1]
    plot_from_csv(input_csv)