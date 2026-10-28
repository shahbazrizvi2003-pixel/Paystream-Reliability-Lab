#!/usr/bin/env bash
# Apply every topic in kafka-config/topics.yaml to the running cluster.
# Idempotent: uses --if-not-exists, safe to re-run.
# Usage: kafka-config/scripts/apply-topics.sh [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="infra/compose/docker-compose.yml"
ENV_FILE=".env"
BROKER="kafka-1"
BOOTSTRAP="localhost:29092"
# The Apache Kafka 4.0 image doesn't put the CLI on $PATH; commands generated
# below run inside the broker (or kafka-setup) container, so use absolute paths.
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
COMMAND_CONFIG=""
DIRECT=0

usage() {
  cat <<'EOF'
apply-topics.sh - create all topics declared in kafka-config/topics.yaml

Topics are declared as code (FR-02, FR-16). This script is idempotent and can
be re-run safely; it never deletes topics not present in the YAML (use the
drift check for that).

Options:
  -h, --help            Show this help.
  --dry-run             Print the kafka-topics commands without executing.
  --bootstrap URL       Bootstrap server to use (default: localhost:29092).
  --command-config FILE Client properties for authenticated clusters
                        (SASL/SSL), e.g. the svc-admin-ci properties file.
  --direct              Run kafka-topics.sh in the current environment
                        instead of via docker compose (used inside the
                        kafka-setup container by bootstrap-security.sh).
EOF
}

DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --dry-run) DRY_RUN=1; shift ;;
    --bootstrap) BOOTSTRAP="$2"; shift 2 ;;
    --command-config) COMMAND_CONFIG="$2"; shift 2 ;;
    --direct) DIRECT=1 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"

# Git Bash (MSYS2) rewrites POSIX-looking args of native executables, turning
# "$KAFKA_BIN/kafka-topics.sh" into "C:/Program Files/Git/opt/kafka/..." at the
# docker call site. Docker ignores this variable on other platforms.
export MSYS_NO_PATHCONV=1

# topics.yaml uses a small, regular subset of YAML; parse with an embedded
# python script to avoid a PyYAML dependency on the host. Windows ships a
# non-functional python3 alias, so verify execution, not just presence.
PYTHON=""
for cand in python3 python; do
  if command -v "$cand" >/dev/null 2>&1 && "$cand" -c "import sys" >/dev/null 2>&1; then
    PYTHON="$cand"
    break
  fi
done
if [ -z "$PYTHON" ]; then
  echo "ERROR: no working python found in PATH" >&2
  exit 1
fi
CMDS="$("$PYTHON" - kafka-config/topics.yaml "$BOOTSTRAP" "$KAFKA_BIN" <<'PY' | tr -d '\r'
import sys, re

path, bootstrap, kafka_bin = sys.argv[1], sys.argv[2], sys.argv[3]
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
    if t["name"] is None or t["partitions"] is None or t["rf"] is None:
        sys.exit(f"incomplete topic entry: {t}")
    cmd = (f"{kafka_bin}/kafka-topics.sh --bootstrap-server {bootstrap} --create --if-not-exists "
           f"--topic {t['name']} --partitions {t['partitions']} "
           f"--replication-factor {t['rf']}")
    for c in t["config"]:
        cmd += f" --config {c}"
    print(cmd)

PY
)"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "$CMDS"
  exit 0
fi

# Build the auth suffix once; empty for the plaintext MVP path.
AUTH_ARGS=""
if [ -n "$COMMAND_CONFIG" ]; then
  AUTH_ARGS="--command-config $COMMAND_CONFIG"
fi

run_kafka_cmd() {
  if [ "$DIRECT" -eq 1 ]; then
    $1
  else
    # </dev/null: docker compose exec -T otherwise drains the caller's stdin
    # and swallows the remaining commands in the `while read` loop below.
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" exec -T "$BROKER" $1 </dev/null
  fi
}

echo "Applying topics to $BROKER ($BOOTSTRAP)..."
echo "$CMDS" | while IFS= read -r cmd; do
  echo "  + ${cmd#*kafka-topics.sh }"
  run_kafka_cmd "$cmd $AUTH_ARGS"
done

echo "Current topics in cluster:"
run_kafka_cmd "$KAFKA_BIN/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list"
