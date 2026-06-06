import sys
import re
from pathlib import Path

import pandas as pd


if len(sys.argv) != 3:
    print(
        "Usage: python collect_and_median.py "
        "<input_folder> <output_name>"
    )
    sys.exit(1)

input_folder = Path(sys.argv[1])
output_name = sys.argv[2]

if not input_folder.exists():
    raise FileNotFoundError(input_folder)


all_frames = []

for csv_file in sorted(input_folder.glob("*.csv")):
    try:
        df = pd.read_csv(csv_file)

        if len(df) == 0:
            continue

        all_frames.append(df.tail(7))

    except Exception as e:
        print(f"Skipping {csv_file}: {e}")

if not all_frames:
    raise ValueError("No usable CSV files found")


combined = pd.concat(all_frames, ignore_index=True)


# Save output
out_dir = Path.cwd() / "tpch" / "results" / "scala"
out_dir.mkdir(parents=True, exist_ok=True)

output_file = (
    out_dir /
    f"{output_name}_final_runtimes.csv"
)

combined.to_csv(output_file, index=False)

# Extract grouping fields
combined["ratio"] = (
    combined["active_config"]
    .str.extract(r"-(1:\d)-")
)

combined["partbytes"] = (
    combined["active_config"]
    .str.extract(r"partbytes([^-]+)")
)

combined["shufflepart"] = (
    combined["active_config"]
    .str.extract(r"-(\d+)part-")
)

# New: extract operationAware flag
combined["operationAware"] = (
    combined["active_config"]
    .str.contains(r"-operationAware", regex=True)
)

# Median computation
summary = (
    combined
    .groupby(
        [
            "operationAware",
            "ratio",
            "shufflepart",
        ]
    )["elapsed_s"]
    .median()
    .reset_index()
    .rename(
        columns={
            "elapsed_s": "median_elapsed_s"
        }
    )
)

summary["shufflepart_num"] = (
    summary["shufflepart"]
    .astype(int)
)

summary = (
    summary
    .sort_values(
        [
            "operationAware",
            "shufflepart_num",
            "ratio",
        ]
    )
    .drop(
        columns="shufflepart_num"
    )
)

print(f"\nSaved combined CSV:")
print(output_file)

print("\nMedian elapsed_s:")
print(summary.to_string(index=False))