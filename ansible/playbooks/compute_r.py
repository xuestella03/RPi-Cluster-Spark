#!/usr/bin/env python3
"""
compute_r.py — derive per-operator-type slowdown ratios r = t_slow / t_fast
from the event logs produced by find-r.yml.

For each probe it picks the stage that represents that operator class:
  scan  -> the file-scan MAP stage  (does NOT read a shuffle, large input bytes)
  agg   -> the heavy shuffle-READING reduce stage
  sort  -> the heavy shuffle-READING reduce stage (range-partitioned sort)
  join  -> the heavy shuffle-READING reduce stage (sort-merge join)

r is computed as median(Executor Run Time | slow host) / median(Executor Run Time | fast host)
on the chosen stage. Run time is per-task and includes shuffle-fetch blocking, so it reflects
how long a task actually occupies a core on each host. Fetch-wait and GC medians are reported
alongside so you can see whether the ratio is compute- or fetch-bound.

Usage:
    python3 compute_r.py /tmp/find-r-<ts> \
        --slow 192.168.50.198 --fast 192.168.50.197 \
        --probes scan,agg,sort,join --shuffle-partitions 48
"""

import argparse
import collections
import json
import math
import statistics
import sys
from pathlib import Path

# probe -> the scheduler conf key whose default this probe calibrates
CONF_KEY = {
    "scan": "spark.heterogeneous.scheduling.scan.slowdown",
    "agg":  "spark.heterogeneous.scheduling.aggReduce.slowdown",
    "sort": "spark.heterogeneous.scheduling.sortReduce.slowdown",
    "join": "spark.heterogeneous.scheduling.joinReduce.slowdown",
}

# probe -> which stage to measure: the scan map stage, or the shuffle-reading reduce stage
TARGET = {"scan": "map", "agg": "reduce", "sort": "reduce", "join": "reduce"}


def find_event_log(probe_dir: Path):
    """Spark's rolling event log lives at <dir>/eventlog_v2_app-*/events_*."""
    for p in sorted(probe_dir.rglob("events_*")):
        if p.is_file():
            return p
    return None


def parse(event_log: Path):
    """
    Returns:
      runs[(stage,host)][metric] -> list of per-task values (run/gc/fetch, all ms)
      shread[(stage,host)]       -> total shuffle bytes read
      bytesin[(stage,host)]      -> total input bytes read
    Only successful tasks are counted.
    """
    runs    = collections.defaultdict(lambda: collections.defaultdict(list))
    shread  = collections.defaultdict(float)
    bytesin = collections.defaultdict(float)

    with open(event_log) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if e.get("Event") != "SparkListenerTaskEnd":
                continue
            reason = e.get("Task End Reason", {})
            if isinstance(reason, dict) and reason.get("Reason") != "Success":
                continue

            sid  = e["Stage ID"]
            host = e["Task Info"]["Host"]
            m    = e.get("Task Metrics") or {}
            key  = (sid, host)

            runs[key]["run"].append(m.get("Executor Run Time", 0))
            runs[key]["gc"].append(m.get("JVM GC Time", 0))

            srm = m.get("Shuffle Read Metrics", {}) or {}
            runs[key]["fetch"].append(srm.get("Fetch Wait Time", 0))
            shread[key] += srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)

            im = m.get("Input Metrics", {}) or {}
            bytesin[key] += im.get("Bytes Read", 0)

    return runs, shread, bytesin


def med(xs):
    return statistics.median(xs) if xs else 0.0


def analyze(probe: str, probe_dir: Path, slow: str, fast: str):
    log = find_event_log(probe_dir)
    if log is None:
        print(f"[{probe}] no event log under {probe_dir}", file=sys.stderr)
        return None

    runs, shread, bytesin = parse(log)

    by_stage = collections.defaultdict(set)
    for (sid, host) in runs:
        by_stage[sid].add(host)

    target = TARGET[probe]
    cands = []  # (stage_id, total_tasks, total_input_bytes)
    for sid, hosts in by_stage.items():
        if slow not in hosts or fast not in hosts:
            continue  # need both hosts to form a ratio
        tot_sh = sum(shread[(sid, h)] for h in hosts)
        tot_in = sum(bytesin[(sid, h)] for h in hosts)
        ntasks = sum(len(runs[(sid, h)]["run"]) for h in hosts)
        is_reduce = tot_sh > 0
        if target == "reduce" and not is_reduce:
            continue
        if target == "map" and (is_reduce or tot_in <= 0):
            continue
        cands.append((sid, ntasks, tot_in))

    if not cands:
        print(f"[{probe}] no qualifying {target} stage with tasks on both hosts", file=sys.stderr)
        return None

    # Pick the warmed, heaviest stage. Several iterations produce duplicates with rising
    # stage ids; the lexicographic max on (size, stage_id) takes the last (most warmed) one,
    # and the size term skips trivial 1-task final aggregates.
    if target == "reduce":
        sid = max(cands, key=lambda c: (c[1], c[0]))[0]   # most tasks, then latest
    else:
        sid = max(cands, key=lambda c: (c[2], c[0]))[0]   # most input bytes, then latest

    s = runs[(sid, slow)]
    f = runs[(sid, fast)]
    s_run, f_run = med(s["run"]), med(f["run"])
    r = (s_run / f_run) if f_run > 0 else float("nan")

    return {
        "probe":   probe,
        "stage":   sid,
        "r":       r,
        "s_run":   s_run, "f_run": f_run,
        "s_fetch": med(s["fetch"]), "f_fetch": med(f["fetch"]),
        "s_gc":    med(s["gc"]),    "f_gc":    med(f["gc"]),
        "n_slow":  len(s["run"]),   "n_fast":  len(f["run"]),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="temp event-log root (contains one subdir per probe)")
    ap.add_argument("--slow", required=True, help="slow host IP/name (e.g. 192.168.50.198)")
    ap.add_argument("--fast", required=True, help="fast host IP/name (e.g. 192.168.50.197)")
    ap.add_argument("--probes", default="scan,agg,sort,join")
    ap.add_argument("--shuffle-partitions", default="?")
    args = ap.parse_args()

    root = Path(args.root)
    probes = [p.strip() for p in args.probes.split(",") if p.strip()]

    results = []
    for probe in probes:
        res = analyze(probe, root / probe, args.slow, args.fast)
        if res:
            results.append(res)

    print("\n" + "=" * 92)
    print(f"r-value diagnostic   slow={args.slow}  fast={args.fast}  "
          f"shuffle.partitions={args.shuffle_partitions}")
    print("=" * 92)
    hdr = (f"{'probe':<6} {'stage':>5} {'n(s/f)':>9} {'run ms(s/f)':>18} "
           f"{'fetch ms(s/f)':>16} {'gc ms(s/f)':>14} {'r':>7}")
    print(hdr)
    print("-" * len(hdr))
    for x in results:
        warn = "  [LOW N]" if min(x["n_slow"], x["n_fast"]) < 3 else ""
        rstr = "nan" if math.isnan(x["r"]) else f"{x['r']:.2f}"
        print(f"{x['probe']:<6} {x['stage']:>5} "
              f"{x['n_slow']:>4}/{x['n_fast']:<4} "
              f"{x['s_run']:>8.0f}/{x['f_run']:<9.0f} "
              f"{x['s_fetch']:>7.0f}/{x['f_fetch']:<8.0f} "
              f"{x['s_gc']:>6.0f}/{x['f_gc']:<7.0f} "
              f"{rstr:>7}{warn}")

    print("\nRecommended --conf flags (measured r, rounded to 1 dp):")
    for x in results:
        if math.isnan(x["r"]):
            continue
        note = ""
        if x["probe"] == "join":
            note = "   # you currently force-exclude joins with 99.0; set this only if you want them includable"
        print(f"  --conf {CONF_KEY[x['probe']]}={x['r']:.1f}{note}")

    print("\nNote: r is measured with the slow host fully loaded (equal weight), so it tends to be"
          "\nconservative (slightly high) vs a one-wave deployment share. Round up for a safety margin.")


if __name__ == "__main__":
    main()