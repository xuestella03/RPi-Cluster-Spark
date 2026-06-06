from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import re

# ---------- DATASET 1 ----------
raw1 = r"""
20260527_011620,3,69.237,768m,v1-q136-x4-nfs-sf3
20260527_011620,1,111.168,768m,v1-q136-x4-nfs-sf3
20260527_011620,6,38.289,768m,v1-q136-x4-nfs-sf3
20260527_011620,6,37.952,768m,v1-q136-x4-nfs-sf3
20260527_011620,3,52.871,768m,v1-q136-x4-nfs-sf3
20260527_011620,1,102.439,768m,v1-q136-x4-nfs-sf3
20260527_011620,3,48.847,768m,v1-q136-x4-nfs-sf3
20260527_011620,6,37.028,768m,v1-q136-x4-nfs-sf3
20260527_011620,1,99.4,768m,v1-q136-x4-nfs-sf3
20260527_011620,6,37.541,768m,v1-q136-x4-nfs-sf3
20260527_011620,3,47.388,768m,v1-q136-x4-nfs-sf3
20260527_011620,1,101.539,768m,v1-q136-x4-nfs-sf3

20260527_031922,3,50.391,768m,v2-q136-x4-nfs-sf3
20260527_031922,6,25.395,768m,v2-q136-x4-nfs-sf3
20260527_031922,1,65.735,768m,v2-q136-x4-nfs-sf3
20260527_031922,6,24.794,768m,v2-q136-x4-nfs-sf3
20260527_031922,3,37.012,768m,v2-q136-x4-nfs-sf3
20260527_031922,1,60.933,768m,v2-q136-x4-nfs-sf3
20260527_031922,3,37.363,768m,v2-q136-x4-nfs-sf3
20260527_031922,6,21.886,768m,v2-q136-x4-nfs-sf3
20260527_031922,1,63.01,768m,v2-q136-x4-nfs-sf3
20260527_031922,1,62.677,768m,v2-q136-x4-nfs-sf3
20260527_031922,6,22.057,768m,v2-q136-x4-nfs-sf3
20260527_031922,3,36.48,768m,v2-q136-x4-nfs-sf3
"""

# ---------- DATASET 2 ----------
raw2 = r"""
20260527_013736,1,59.52,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,5,22.143,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,6,7.129,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,3,10.757,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,1,12.233,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,3,11.792,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,5,15.07,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,6,4.776,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,3,11.942,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,1,10.261,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,5,14.873,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,6,5.61,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,5,8.778,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,1,12.374,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,3,15.098,768m,v1-q1356-x4-nfs-sf0.3
20260527_013736,6,5.991,768m,v1-q1356-x4-nfs-sf0.3

20260527_033329,5,26.378,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,1,20.835,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,6,9.864,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,3,7.415,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,3,5.582,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,6,4.266,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,1,8.515,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,5,5.926,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,5,5.864,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,3,14.936,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,6,4.627,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,1,7.051,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,6,3.806,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,5,4.917,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,3,4.794,768m,v2-q1356-x4-nfs-sf0.3
20260527_033329,1,8.213,768m,v2-q1356-x4-nfs-sf0.3
"""

def process_dataset(raw, rows_to_drop=2):
    lines = [l.strip() for l in raw.splitlines() if "," in l]

    rows = []
    for line in lines:
        ts, query, elapsed, mem, config = line.split(",")
        version = re.search(r"(v\d+)", config).group(1)

        rows.append({
            "timestamp": ts,
            "query": int(query),
            "elapsed_s": float(elapsed),
            "version": version,
            "config": config
        })

    df = pd.DataFrame(rows)

    processed = []

    # process each timestamp/version group
    for _, group in df.groupby(["timestamp", "version"]):
        trimmed = group.iloc[rows_to_drop:]

        for _, row in trimmed.iterrows():
            processed.append(row)

    processed_df = pd.DataFrame(processed)

    summary = (
        processed_df
        .groupby(["query", "version"])["elapsed_s"]
        .agg(["mean", "min", "max"])
        .reset_index()
    )

    summary["lower_err"] = summary["mean"] - summary["min"]
    summary["upper_err"] = summary["max"] - summary["mean"]

    return summary

def make_plot(summary, title, outpath):
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
            capsize=5,
            label=version
        )

    plt.xticks(list(x_positions), [f"Q{q}" for q in queries])
    plt.xlabel("TPC-H Query")
    plt.ylabel("Elapsed Time (s)")
    plt.title(title)
    plt.legend()

    plt.savefig(outpath, bbox_inches="tight")
    plt.close()

summary1 = process_dataset(raw1, rows_to_drop=2)
summary2 = process_dataset(raw2, rows_to_drop=2)

plot1 = "results_mixed_1.png"
plot2 = "results_mixed_2.png"

make_plot(summary1, "Q1/Q3/Q6 Comparison (First Two Rows Removed)", plot1)
make_plot(summary2, "Q1/Q3/Q5/Q6 Comparison (First Two Rows Removed)", plot2)

