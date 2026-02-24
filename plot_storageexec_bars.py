import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


def main():
    if len(sys.argv) != 2:
        print("Usage: python plot_gc_bars.py <file.csv>")
        sys.exit(1)

    filename = sys.argv[1]

    # Load CSV
    df = pd.read_csv(filename)

    # Sort executor memory numerically (strip 'm')
    # df["exec_mem_num"] = df["executor_memory"].str.replace("m", "", regex=False).astype(int)
    # df = df.sort_values("exec_mem_num")

    df["memory_fraction"] = df["memory_fraction"]
    df = df.sort_values("memory_fraction")

    # labels = df["executor_memory"].tolist()
    labels = df["memory_fraction"].tolist()
    minor = df["OnHeapStorageMemory_MB"].tolist()
    major = df["OnHeapExecutionMemory_MB"].tolist()

    x = np.arange(len(labels))
    width = 0.35

    plt.figure()
    plt.bar(x - width/2, minor, width, label="OnHeapStorageMemory_MB")
    plt.bar(x + width/2, major, width, label="OnHeapExecutionMemory_MB")

    plt.xticks(x, labels)
    # plt.xlabel("Storage Fraction")
    plt.xlabel("Memory Fraction")
    plt.ylabel("MB")
    plt.title("Storage vs Execution by Memory Fraction")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{filename.split(".")[0]}-storageexec.png", dpi=150)

if __name__ == "__main__":
    main()