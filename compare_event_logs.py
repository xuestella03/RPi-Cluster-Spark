#!/usr/bin/env python3
"""
Aggregate and compare Spark event log metrics across all groupBy runs
in a directory tree.

Usage:
    python parse_eventlogs.py /path/to/results/folder
"""

import json
import collections
import argparse
import os
import re
import sys
from pathlib import Path
from contextlib import redirect_stdout


def parse_config_name(run_dir_name: str) -> str:
    """
    Extract the config label from a run directory name.
    E.g. '20260605_032246-groupBy-x6-sf1-mem512-3-1:1-4part-partbytes32m'
         -> 'groupBy-x6-sf1-mem512-3-1:1-4part-partbytes32m'
    """
    # Strip leading timestamp (YYYYMMDD_HHMMSS-)
    return re.sub(r"^\d{8}_\d{6}-", "", run_dir_name)


def find_event_log(run_dir: Path) -> Path | None:
    """
    Walk a run directory and return the first file whose name starts with 'events_'.
    Structure: run_dir/eventlog_v2_*/events_1_*
    """
    for path in run_dir.rglob("events_*"):
        if path.is_file():
            return path
    return None


# def parse_event_log(event_log_path: Path):
#     """
#     Parse a single Spark event log file.
#     Returns (agg, cnt) where:
#       agg[(stage_id, host)][metric] = total value
#       cnt[(stage_id, host)]         = number of tasks
#     """
#     agg = collections.defaultdict(lambda: collections.defaultdict(float))
#     cnt = collections.defaultdict(int)

#     with open(event_log_path) as f:
#         for lineno, line in enumerate(f, 1):
#             line = line.strip()
#             if not line:
#                 continue
#             try:
#                 e = json.loads(line)
#             except json.JSONDecodeError as exc:
#                 print(f"  [warn] {event_log_path}: line {lineno} JSON error: {exc}",
#                       file=sys.stderr)
#                 continue

#             if e.get("Event") != "SparkListenerTaskEnd":
#                 continue

#             sid  = e["Stage ID"]
#             host = e["Task Info"]["Host"]
#             m    = e["Task Metrics"]
#             # a    = e["Task Info"]["Accumulables"]
#             key  = (sid, host)

#             cnt[key] += 1
#             agg[key]["run"]    += m.get("Executor Run Time", 0)
#             agg[key]["deser"]  += m.get("Executor Deserialize Time", 0)
#             agg[key]["gc"]     += m.get("JVM GC Time", 0)
#             agg[key]["bytesIn"] += m.get("Input Metrics", {}).get("Bytes Read", 0)
#             agg[key]["shWrite"] += m.get("Shuffle Write Metrics", {}).get("Shuffle Write Time", 0)

#     return agg, cnt


# def print_run(config: str, agg, cnt):
#     """Pretty-print per-stage, per-host metrics for one run."""
#     stages = sorted({sid for (sid, _) in agg})
#     print(f"{'='*70}")
#     print(f"CONFIG: {config}")

#     for sid in stages:
#         hosts = [(sid, h) for (s, h) in agg if s == sid]
#         total_tasks = sum(cnt[k] for k in hosts)
#         print(f"  --- Stage {sid}  (total tasks={total_tasks}) ---")
#         for key in sorted(hosts, key=lambda k: k[1]):
#             n   = cnt[key]
#             per = {k: round(v / n, 1) for k, v in agg[key].items()}
#             print(f"    host={key[1]}  n={n}  {per}")


# def print_comparison(all_runs: list[tuple[str, dict, dict]]):
#     """
#     Print a stage-level summary table comparing total run-time across configs.
#     Columns: config | stage | total_tasks | avg_run_ms | avg_gc_ms | total_bytesIn
#     """
#     print(f"\n\n{'='*70}")
#     print("COMPARISON SUMMARY  (per-task averages across all hosts)")
#     print(f"{'='*70}")

#     # Gather all stage IDs seen across all runs
#     all_stages = sorted({
#         sid
#         for _, agg, _ in all_runs
#         for (sid, _) in agg
#     })

#     header = f"{'Config':<45} {'Stg':>3} {'Tasks':>6} {'run(ms)':>10} {'gc(ms)':>8} {'bytesIn':>12} {'shWr(ms)':>10}"
#     print(header)
#     print("-" * len(header))

#     for config, agg, cnt in all_runs:
#         for sid in all_stages:
#             keys = [(sid, h) for (s, h) in agg if s == sid]
#             if not keys:
#                 continue
#             total_tasks = sum(cnt[k] for k in keys)
#             totals = collections.defaultdict(float)
#             for k in keys:
#                 for metric, val in agg[k].items():
#                     totals[metric] += val

#             avg = {m: round(v / total_tasks, 1) for m, v in totals.items()}
#             label = (config[:42] + "...") if len(config) > 45 else config
#             print(
#                 f"{label:<45} {sid:>3} {total_tasks:>6} "
#                 f"{avg.get('run', 0):>10.1f} {avg.get('gc', 0):>8.1f} "
#                 f"{int(totals.get('bytesIn', 0)):>12,} {avg.get('shWrite', 0):>10.1f}"
#             )


# def main():
#     parser = argparse.ArgumentParser(
#         description="Aggregate and compare Spark event log metrics across all groupBy runs."
#     )
#     parser.add_argument(
#         "results_dir",
#         help="Root folder containing the timestamped run directories."
#     )
#     parser.add_argument(
#         "--no-detail",
#         action="store_true",
#         help="Skip per-host detail, print only the comparison summary."
#     )
#     args = parser.parse_args()

#     root = Path(args.results_dir)
#     if not root.is_dir():
#         sys.exit(f"Error: '{root}' is not a directory.")

#     # Find all run directories (have a timestamp prefix + config name)
#     run_dirs = sorted(
#         d for d in root.iterdir()
#         if d.is_dir() and re.match(r"\d{8}_\d{6}-", d.name)
#     )

#     if not run_dirs:
#         sys.exit(f"No timestamped run directories found under '{root}'.")

#     all_runs = []

#     for run_dir in run_dirs:
#         config = parse_config_name(run_dir.name)
#         event_log = find_event_log(run_dir)

#         if event_log is None:
#             print(f"[skip] No events_ file found in {run_dir.name}", file=sys.stderr)
#             continue

#         print(f"Parsing  {config}  ({event_log.relative_to(root)})", file=sys.stderr)
#         agg, cnt = parse_event_log(event_log)

#         if not agg:
#             print(f"[skip] No SparkListenerTaskEnd events in {event_log}", file=sys.stderr)
#             continue

#         all_runs.append((config, agg, cnt))

#     if not all_runs: 
#         sys.exit("No runs could be parsed.") 

#     output_file = root / "parsed_event_logs.txt"
#     with open(output_file, "w") as f: 
#         with redirect_stdout(f): 
#             if not args.no_detail: 
#                 for config, agg, cnt in all_runs: 
#                     print_run(config, agg, cnt) # Uncomment if desired # 
#             # print_comparison(all_runs) 
    
#     print(f"\nSaved parsed output to:") 
#     print(output_file)


# if __name__ == "__main__":
#     main()

import collections
import json
import sys
from pathlib import Path

# ─── SQL operator metrics ────────────────────────────────────────────────
# (substring matched case-insensitively in accumulable "Name", short label, unit)
#   "ms" -> already milliseconds         "ns" -> nanoseconds, /1e6 to print ms
#   "n"  -> plain count / byte total
ACCUM_SPECS = [
    ("scan time",                 ("scanMs",      "ms")),
    ("time in aggregation build", ("aggBuildMs",  "ns")),
    ("aggregate time",            ("aggMs",       "ns")),
    ("sort time",                 ("sortMs",      "ns")),
    ("shuffle write time",        ("shWriteAcc",  "ns")),
    ("fetch wait time",           ("fetchWaitMs", "ms")),
    ("remote bytes read",         ("remoteBytes", "n")),
    ("local bytes read",          ("localBytes",  "n")),
    ("shuffle records read",      ("shRecRead",   "n")),
    ("peak memory",               ("peakMem",     "n")),
    ("number of output rows",     ("outRows",     "n")),
]
_LABEL_UNIT = {label: unit for _, (label, unit) in ACCUM_SPECS}
DUMP_UNMATCHED_ACCUMS = False  # True -> also tally every other SQL metric name


def _accum_label(name):
    low = name.lower()
    for needle, (label, _unit) in ACCUM_SPECS:
        if needle in low:
            return label
    return None


def parse_event_log(event_log_path: Path):
    """
    Parse a single Spark event log file.
    Returns (agg, cnt) where:
      agg[(stage_id, host)][metric] = total value
      cnt[(stage_id, host)]         = number of tasks
    """
    agg = collections.defaultdict(lambda: collections.defaultdict(float))
    cnt = collections.defaultdict(int)
    with open(event_log_path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [warn] {event_log_path}: line {lineno} JSON error: {exc}",
                      file=sys.stderr)
                continue
            if e.get("Event") != "SparkListenerTaskEnd":
                continue
            sid  = e["Stage ID"]
            host = e["Task Info"]["Host"]
            m    = e["Task Metrics"]
            key  = (sid, host)
            cnt[key] += 1

            # ── base task-level metrics ───────────────────────────────
            agg[key]["run"]     += m.get("Executor Run Time", 0)
            agg[key]["deser"]   += m.get("Executor Deserialize Time", 0)
            agg[key]["gc"]      += m.get("JVM GC Time", 0)
            agg[key]["bytesIn"] += m.get("Input Metrics", {}).get("Bytes Read", 0)
            # NB: "Shuffle Write Time" here is in NANOSECONDS (raw, as before)
            agg[key]["shWrite"] += m.get("Shuffle Write Metrics", {}) \
                                    .get("Shuffle Write Time", 0)
            # shuffle-read side (0 unless this stage reads a shuffle)
            srm = m.get("Shuffle Read Metrics", {})
            agg[key]["shReadBytes"] += (srm.get("Remote Bytes Read", 0)
                                        + srm.get("Local Bytes Read", 0))
            agg[key]["fetchWaitTM"] += srm.get("Fetch Wait Time", 0)

            # ── SQL operator-level metrics (Accumulables) ─────────────
            for ac in e["Task Info"].get("Accumulables", []):
                name = ac.get("Name")
                raw  = ac.get("Update")
                if not name or raw is None:
                    continue
                if name.startswith("internal."):      # dup of Task Metrics
                    continue
                try:
                    val = float(raw)                  # SQL updates are often strings
                except (TypeError, ValueError):
                    continue
                label = _accum_label(name)
                if label is not None:
                    agg[key][label] += val
                elif DUMP_UNMATCHED_ACCUMS:
                    agg[key][f"?{name}"] += val
    return agg, cnt


def _to_ms(label, value):
    """ns-unit accumulables -> ms; everything else unchanged."""
    return value / 1e6 if _LABEL_UNIT.get(label) == "ns" else value


def print_run(config: str, agg, cnt):
    """Pretty-print per-stage, per-host metrics for one run."""
    stages = sorted({sid for (sid, _) in agg})
    print(f"{'='*70}")
    print(f"CONFIG: {config}")
    for sid in stages:
        hosts = [(sid, h) for (s, h) in agg if s == sid]
        total_tasks = sum(cnt[k] for k in hosts)
        print(f"  --- Stage {sid}  (total tasks={total_tasks}) ---")
        for key in sorted(hosts, key=lambda k: k[1]):
            n   = cnt[key]
            per = {k: round(_to_ms(k, v) / n, 1) for k, v in agg[key].items()}
            print(f"    host={key[1]}  n={n}  {per}")


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate and compare Spark event log metrics across all groupBy runs."
    )
    parser.add_argument(
        "results_dir",
        help="Root folder containing the timestamped run directories."
    )
    parser.add_argument(
        "--no-detail",
        action="store_true",
        help="Skip per-host detail, print only the comparison summary."
    )
    args = parser.parse_args()

    root = Path(args.results_dir)
    if not root.is_dir():
        sys.exit(f"Error: '{root}' is not a directory.")

    # Find all run directories (have a timestamp prefix + config name)
    run_dirs = sorted(
        d for d in root.iterdir()
        if d.is_dir() and re.match(r"\d{8}_\d{6}-", d.name)
    )

    if not run_dirs:
        sys.exit(f"No timestamped run directories found under '{root}'.")

    all_runs = []

    for run_dir in run_dirs:
        config = parse_config_name(run_dir.name)
        event_log = find_event_log(run_dir)

        if event_log is None:
            print(f"[skip] No events_ file found in {run_dir.name}", file=sys.stderr)
            continue

        print(f"Parsing  {config}  ({event_log.relative_to(root)})", file=sys.stderr)
        agg, cnt = parse_event_log(event_log)

        if not agg:
            print(f"[skip] No SparkListenerTaskEnd events in {event_log}", file=sys.stderr)
            continue

        all_runs.append((config, agg, cnt))

    if not all_runs: 
        sys.exit("No runs could be parsed.") 

    output_file = root / "parsed_event_logs.txt"
    with open(output_file, "w") as f: 
        with redirect_stdout(f): 
            if not args.no_detail: 
                for config, agg, cnt in all_runs: 
                    print_run(config, agg, cnt) # Uncomment if desired # 
            # print_comparison(all_runs) 
    
    print(f"\nSaved parsed output to:") 
    print(output_file)


if __name__ == "__main__":
    main()