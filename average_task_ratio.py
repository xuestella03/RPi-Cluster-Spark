import re
import sys
from collections import defaultdict

text = """
--- Stage 7  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1283.7, 'deser': 18.4, 'gc': 20.7, 'bytesIn': 32990800.4, 'shWrite': 14959521.9}
host=192.168.50.198  n=4  {'run': 6242.8, 'deser': 126.2, 'gc': 174.0, 'bytesIn': 33619968.0, 'shWrite': 55864128.8}
--- Stage 8  (total tasks=48) ---
host=192.168.50.197  n=40  {'run': 126.2, 'deser': 12.3, 'gc': 0.2, 'bytesIn': 0.0, 'shWrite': 0.0}
host=192.168.50.198  n=8  {'run': 726.8, 'deser': 81.5, 'gc': 4.0, 'bytesIn': 0.0, 'shWrite': 0.0}
--- Stage 9  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1026.6, 'deser': 13.5, 'gc': 28.2, 'bytesIn': 32990800.4, 'shWrite': 0.0}
host=192.168.50.198  n=4  {'run': 4939.0, 'deser': 76.0, 'gc': 205.2, 'bytesIn': 33619968.0, 'shWrite': 0.0}
--- Stage 10  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1200.1, 'deser': 18.5, 'gc': 20.2, 'bytesIn': 32990800.4, 'shWrite': 13279970.8}
host=192.168.50.198  n=4  {'run': 5608.5, 'deser': 106.2, 'gc': 183.0, 'bytesIn': 33619968.0, 'shWrite': 38318839.0}
--- Stage 11  (total tasks=48) ---
host=192.168.50.197  n=40  {'run': 96.9, 'deser': 10.9, 'gc': 0.3, 'bytesIn': 0.0, 'shWrite': 0.0}
host=192.168.50.198  n=8  {'run': 483.9, 'deser': 77.5, 'gc': 8.6, 'bytesIn': 0.0, 'shWrite': 0.0}
--- Stage 12  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1088.5, 'deser': 10.5, 'gc': 28.5, 'bytesIn': 32990800.4, 'shWrite': 0.0}
host=192.168.50.198  n=4  {'run': 5647.0, 'deser': 73.2, 'gc': 608.5, 'bytesIn': 33619968.0, 'shWrite': 0.0}
--- Stage 13  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1215.6, 'deser': 13.7, 'gc': 20.1, 'bytesIn': 32990800.4, 'shWrite': 15140966.8}
host=192.168.50.198  n=4  {'run': 5567.8, 'deser': 111.8, 'gc': 173.8, 'bytesIn': 33619968.0, 'shWrite': 38396277.5}
--- Stage 14  (total tasks=48) ---
host=192.168.50.197  n=40  {'run': 97.6, 'deser': 9.8, 'gc': 0.5, 'bytesIn': 0.0, 'shWrite': 0.0}
host=192.168.50.198  n=8  {'run': 505.8, 'deser': 63.1, 'gc': 22.5, 'bytesIn': 0.0, 'shWrite': 0.0}
--- Stage 15  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1010.9, 'deser': 9.1, 'gc': 27.5, 'bytesIn': 32990800.4, 'shWrite': 0.0}
host=192.168.50.198  n=4  {'run': 4844.2, 'deser': 67.2, 'gc': 221.8, 'bytesIn': 33619968.0, 'shWrite': 0.0}
--- Stage 16  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1213.0, 'deser': 13.8, 'gc': 18.8, 'bytesIn': 32990800.4, 'shWrite': 13833314.6}
host=192.168.50.198  n=4  {'run': 5889.0, 'deser': 76.0, 'gc': 184.8, 'bytesIn': 33619968.0, 'shWrite': 48574546.5}
--- Stage 17  (total tasks=48) ---
host=192.168.50.197  n=39  {'run': 98.2, 'deser': 8.9, 'gc': 0.4, 'bytesIn': 0.0, 'shWrite': 0.0}
host=192.168.50.198  n=9  {'run': 445.0, 'deser': 50.4, 'gc': 5.3, 'bytesIn': 0.0, 'shWrite': 0.0}
--- Stage 18  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1019.7, 'deser': 10.5, 'gc': 28.6, 'bytesIn': 32990800.4, 'shWrite': 0.0}
host=192.168.50.198  n=4  {'run': 5178.0, 'deser': 74.5, 'gc': 625.8, 'bytesIn': 33619968.0, 'shWrite': 0.0}
--- Stage 19  (total tasks=23) ---
host=192.168.50.197  n=19  {'run': 1194.9, 'deser': 10.3, 'gc': 19.6, 'bytesIn': 32990800.4, 'shWrite': 12678035.1}
host=192.168.50.198  n=4  {'run': 5501.0, 'deser': 80.5, 'gc': 175.0, 'bytesIn': 33619968.0, 'shWrite': 36483502.2}
--- Stage 20  (total tasks=48) ---
host=192.168.50.197  n=39  {'run': 90.1, 'deser': 10.3, 'gc': 0.3, 'bytesIn': 0.0, 'shWrite': 0.0}
host=192.168.50.198  n=9  {'run': 439.7, 'deser': 56.6, 'gc': 21.8, 'bytesIn': 0.0, 'shWrite': 0.0}
"""

modulus = int(sys.argv[1])
stage_pattern = re.compile(
    r"--- Stage (\d+).*?---\n(.*?)(?=\n--- Stage|\Z)",
    re.S
)

run_pattern = re.compile(
    r"192\.168\.50\.(197|198).*?'run':\s*([\d.]+)"
)

groups = defaultdict(list)
group_stages = defaultdict(list)

for stage_num, stage_text in stage_pattern.findall(text):
    stage_num = int(stage_num)

    runs = {}

    for host, run in run_pattern.findall(stage_text):
        runs[host] = float(run)

    if "197" in runs and "198" in runs:
        ratio = runs["198"] / runs["197"]

        bucket = stage_num % modulus

        groups[bucket].append(ratio)
        group_stages[bucket].append(stage_num)

        print(
            f"Stage {stage_num}: "
            f"{runs['198']:.1f}/{runs['197']:.1f} "
            f"= {ratio:.3f} "
            f"(mod {bucket})"
        )


print("\nGrouped averages:\n")

for bucket in sorted(groups):
    avg = sum(groups[bucket]) / len(groups[bucket])

    stages = ", ".join(
        map(str, group_stages[bucket])
    )

    print(
        f"Group mod {bucket} "
        f"(stages: {stages})"
    )
    print(
        f"Average ratio (.198/.197): "
        f"{avg:.3f}\n"
    )
