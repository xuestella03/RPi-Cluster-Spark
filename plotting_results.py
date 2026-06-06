from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import re

raw = r"""
6
20260527_000824,6,77.353,768m,v1-q6-x4-nfs-sf10
20260527_000824,6,68.454,768m,v1-q6-x4-nfs-sf10
20260527_000824,6,71.779,768m,v1-q6-x4-nfs-sf10
20260527_000824,6,75.305,768m,v1-q6-x4-nfs-sf10
71.846


20260526_220331,6,85.584,768m,v2-q6-x4-nfs-sf10
20260526_220331,6,71.384,768m,v2-q6-x4-nfs-sf10
20260526_220331,6,68.75,768m,v2-q6-x4-nfs-sf10
20260526_220331,6,69.425,768m,v2-q6-x4-nfs-sf10
69.853

20260527_031145,6,67.108,768m,v2-q6-x4-nfs-sf10
20260527_031145,6,67.832,768m,v2-q6-x4-nfs-sf10
20260527_031145,6,67.187,768m,v2-q6-x4-nfs-sf10
20260527_031145,6,68.512,768m,v2-q6-x4-nfs-sf10


1
20260527_001736,1,257.546,768m,v1-q1-x4-nfs-sf10
20260527_001736,1,240.279,768m,v1-q1-x4-nfs-sf10
20260527_001736,1,239.257,768m,v1-q1-x4-nfs-sf10
20260527_001736,1,237.847,768m,v1-q1-x4-nfs-sf10
239.128

20260526_223926,1,224.04,768m,v2-q1-x4-nfs-sf10
20260526_223926,1,194.693,768m,v2-q1-x4-nfs-sf10
20260526_223926,1,194.386,768m,v2-q1-x4-nfs-sf10
20260526_223926,1,193.962,768m,v2-q1-x4-nfs-sf10
194.347

20260527_024722,1,268.4,768m,v2-q1-x4-nfs-sf10
20260527_024722,1,218.206,768m,v2-q1-x4-nfs-sf10
20260527_024722,1,187.332,768m,v2-q1-x4-nfs-sf10
20260527_024722,1,190.644,768m,v2-q1-x4-nfs-sf10




3
20260527_004020,3,91.205,768m,v1-q3-x4-nfs-sf3
20260527_004020,3,79.807,768m,v1-q3-x4-nfs-sf3
20260527_004020,3,73.21,768m,v1-q3-x4-nfs-sf3
20260527_004020,3,78.551,768m,v1-q3-x4-nfs-sf3
77.189

20260527_030523,3,66.226,768m,v2-q3-x4-nfs-sf3
20260527_030523,3,37.647,768m,v2-q3-x4-nfs-sf3
20260527_030523,3,37.988,768m,v2-q3-x4-nfs-sf3
20260527_030523,3,35.336,768m,v2-q3-x4-nfs-sf3


5
20260527_010951,5,44.879,768m,v1-q5-x4-nfs-sf0.5
20260527_010951,5,29.317,768m,v1-q5-x4-nfs-sf0.5
20260527_010951,5,20.336,768m,v1-q5-x4-nfs-sf0.5
20260527_010951,5,22.072,768m,v1-q5-x4-nfs-sf0.5

20260527_033015,5,27.574,768m,v2-q5-x4-nfs-sf0.5
20260527_033015,5,15.707,768m,v2-q5-x4-nfs-sf0.5
20260527_033015,5,9.471,768m,v2-q5-x4-nfs-sf0.5
20260527_033015,5,8.511,768m,v2-q5-x4-nfs-sf0.5
"""

# Parse only CSV-like lines
lines = [l.strip() for l in raw.splitlines() if "," in l]

records = []
for line in lines:
    parts = line.split(",")
    if len(parts) == 5:
        timestamp, query, elapsed, executor_memory, config = parts
        version = re.search(r"(v\d+)", config).group(1)
        records.append({
            "timestamp": timestamp,
            "query": int(query),
            "elapsed_s": float(elapsed),
            "executor_memory": executor_memory,
            "config": config,
            "version": version
        })

df = pd.DataFrame(records)

# Group every 4 rows by timestamp/config/query
processed_rows = []

group_cols = ["timestamp", "query", "config", "version"]

for _, group in df.groupby(group_cols):
    group = group.reset_index(drop=True)

    # Remove first row of each 4-row group
    trimmed = group.iloc[1:]

    for _, row in trimmed.iterrows():
        processed_rows.append(row)

processed_df = pd.DataFrame(processed_rows)

# Aggregate by version/query
summary = (
    processed_df
    .groupby(["query", "version"])["elapsed_s"]
    .agg(["mean", "min", "max"])
    .reset_index()
)

summary["lower_err"] = summary["mean"] - summary["min"]
summary["upper_err"] = summary["max"] - summary["mean"]


# Plot
plt.figure(figsize=(10, 6))

queries = sorted(summary["query"].unique())
versions = sorted(summary["version"].unique())

x_positions = range(len(queries))
width = 0.35

for i, version in enumerate(versions):
    subset = summary[summary["version"] == version].sort_values("query")

    xs = [x + (i - 0.5) * width for x in x_positions]

    plt.bar(
        xs,
        subset["mean"],
        width=width,
        yerr=[subset["lower_err"], subset["upper_err"]],
        capsize=5
    )

plt.xticks(list(x_positions), [f"Q{q}" for q in queries])
plt.xlabel("Query")
plt.ylabel("Elapsed Time (s)")
plt.title("Runtime Comparison (First Run Removed)")
plt.legend(versions)

plot_path = "plotting_results.png"
plt.savefig(plot_path, bbox_inches="tight")