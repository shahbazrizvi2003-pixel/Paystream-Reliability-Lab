import sys, re

path, bootstrap = sys.argv[1], sys.argv[2]
topics = []
current = None
in_config = False
with open(path, encoding="utf-8") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        m = re.match(r"^  - name: (\S+)$", line)
        if m:
            current = {"name": m.group(1), "partitions": None, "rf": None, "config": []}
            topics.append(current)
            in_config = False
            continue
        if current is None:
            continue
        m = re.match(r"^    partitions: (\d+)$", line)
        if m:
            current["partitions"] = m.group(1); continue
        m = re.match(r"^    replication-factor: (\d+)$", line)
        if m:
            current["rf"] = m.group(1); continue
        if re.match(r"^    config:\s*$", line):
            in_config = True; continue
        m = re.match(r"^      ([a-z.]+):\s*\"?([^\"]+)\"?\s*$", line)
        if m and in_config:
            value = m.group(2).split(" #")[0].strip()
            current["config"].append(f"{m.group(1)}={value}")

for t in topics:
    cmd = (f"/opt/kafka/bin/kafka-topics.sh --bootstrap-server {bootstrap} --create --if-not-exists "
           f"--topic {t['name']} --partitions {t['partitions']} "
           f"--replication-factor {t['rf']}")
    for c in t["config"]:
        cmd += f" --config {c}"
    print(cmd)
