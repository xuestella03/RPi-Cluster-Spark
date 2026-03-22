# import sys
# import pandas as pd
# import matplotlib.pyplot as plt
# import numpy as np


# def main():
#     if len(sys.argv) != 2:
#         print("Usage: python plot_gc_bars.py <file.csv>")
#         sys.exit(1)

#     filename = sys.argv[1]

#     # Load CSV
#     df = pd.read_csv(filename)

#     # Sort executor memory numerically (strip 'm')
#     # df["exec_mem_num"] = df["executor_memory"].str.replace("m", "", regex=False).astype(int)
#     # df = df.sort_values("exec_mem_num")

#     df["jvm_config"] = df["jvm_config"]
#     df = df.sort_values("jvm_config")

#     # labels = df["executor_memory"].tolist()
#     labels = df["jvm_config"].tolist()
#     minor = df["MinorGCTime_ms"].tolist()
#     major = df["MajorGCTime_ms"].tolist()

#     x = np.arange(len(labels))
#     width = 0.35

#     plt.figure()
#     plt.bar(x - width/2, minor, width, label="Minor GC Time (ms)")
#     plt.bar(x + width/2, major, width, label="Major GC Time (ms)")

#     plt.xticks(x, labels)
#     plt.xlabel("Initial Heap Size")
#     plt.ylabel("GC Time (ms)")
#     plt.title("Minor and Major GC Time by GC")
#     plt.legend()
#     plt.tight_layout()
#     plt.savefig(f"{filename.split(".")[0]}-gc.png", dpi=150)

# if __name__ == "__main__":
#     main()

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

    df["jvm_config"] = df["jvm_config"]
    # df = df.sort_values("jvm_config")

    labels = df["jvm_config"].tolist()
    minor = df["MinorGCTime_ms"].tolist()
    major = df["MajorGCTime_ms"].tolist()

    x = np.arange(len(labels))
    width = 0.35

    # Create the bars
    fig, ax = plt.subplots()
    bars_minor = ax.bar(x - width/2, minor, width, label="Minor GC Time (ms)")
    bars_major = ax.bar(x + width/2, major, width, label="Major GC Time (ms)")

    # Add the actual values on top of each bar
    for bar in bars_minor:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval + 5, round(yval, 2), ha='center', va='bottom')

    for bar in bars_major:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval + 5, round(yval, 2), ha='center', va='bottom')

    # Set the rest of the plot attributes
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_xlabel("Initial Heap Size")
    ax.set_ylabel("GC Time (ms)")
    ax.set_title("Minor and Major GC Time by GC")
    ax.legend()
    plt.tight_layout()

    # Save the plot to a file
    plt.savefig(f"{filename.split('.')[0]}-gc.png", dpi=150)

if __name__ == "__main__":
    main()