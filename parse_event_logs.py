import json
import collections
import argparse


def main():
    parser = argparse.ArgumentParser(
        description="Aggregate Spark event log metrics by stage and host"
    )
    parser.add_argument(
        "eventlog",
        help="Path to Spark event log file"
    )

    args = parser.parse_args()

    # (stage_id, host) -> metrics
    agg = collections.defaultdict(lambda: collections.defaultdict(float))
    cnt = collections.defaultdict(int)

    with open(args.eventlog) as f:
        for line in f:
            e = json.loads(line)

            if e.get("Event") != "SparkListenerTaskEnd":
                continue

            sid = e["Stage ID"]
            host = e["Task Info"]["Host"]
            m = e["Task Metrics"]

            key = (sid, host)

            cnt[key] += 1
            agg[key]["run"] += m["Executor Run Time"]
            agg[key]["deser"] += m["Executor Deserialize Time"]
            agg[key]["gc"] += m["JVM GC Time"]
            agg[key]["bytesIn"] += m["Input Metrics"]["Bytes Read"]
            agg[key]["shWrite"] += m["Shuffle Write Metrics"]["Shuffle Write Time"]

    stages = sorted({sid for (sid, _) in agg})

    for sid in stages:
        hosts = [(sid, h) for (s, h) in agg if s == sid]
        total_tasks = sum(cnt[k] for k in hosts)

        print(f"\n=== Stage {sid} (tasks={total_tasks}) ===")

        for key in sorted(hosts, key=lambda k: k[1]):
            n = cnt[key]
            per = {k: round(v / n, 1) for k, v in agg[key].items()}
            print(f"  {key[1]}  n={n}  {per}")


if __name__ == "__main__":
    main()