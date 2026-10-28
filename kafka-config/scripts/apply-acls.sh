#!/usr/bin/env bash
# Apply every ACL in kafka-config/acls.yaml to the running cluster (WP2.2).
# Idempotent: kafka-acls --add is declarative, safe to re-run. The script
# never deletes ACLs missing from the YAML (use check-drift.sh for that).
# Usage: kafka-config/scripts/apply-acls.sh [--help] [--dry-run]
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
apply-acls.sh - apply the Document 03 ACL matrix from kafka-config/acls.yaml

The matrix is the source of truth (FR-09): default deny, no wildcard
principals, no ALL on '*'. Applying is idempotent and can be re-run safely.

Options:
  -h, --help            Show this help.
  --dry-run             Print the kafka-acls commands without executing.
  --bootstrap URL       Bootstrap server (default: localhost:29092).
  --command-config FILE Client properties for the admin principal on
                        authenticated clusters (SASL/SSL).
  --direct              Run kafka-acls.sh in the current environment instead
                        of via docker compose (used inside the kafka-setup
                        container by bootstrap-security.sh).
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
# "$KAFKA_BIN/kafka-acls.sh" into "C:/Program Files/Git/opt/kafka/..." at the
# docker call site. Docker ignores this variable on other platforms.
export MSYS_NO_PATHCONV=1

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

CMDS="$("$PYTHON" - kafka-config/acls.yaml "$BOOTSTRAP" "$KAFKA_BIN" <<'PY' | tr -d '\r'
import sys

path, bootstrap, kafka_bin = sys.argv[1], sys.argv[2], sys.argv[3]
acls = []
current = None
in_resource = False
with open(path, encoding="utf-8") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        if line.startswith("  - principal: "):
            current = {"principal": line.split("  - principal: ", 1)[1].strip(),
                       "type": None, "name": None, "pattern": "literal", "ops": []}
            acls.append(current)
            in_resource = False
            continue
        if current is None:
            continue
        if line.startswith("    resource:"):
            in_resource = True
            continue
        m = line.strip()
        if m.startswith("operations: [") and m.endswith("]"):
            current["ops"] = [op.strip() for op in m[len("operations: ["):-1].split(",") if op.strip()]
            in_resource = False
            continue
        if in_resource and ":" in m:
            key, value = m.split(":", 1)
            key, value = key.strip(), value.strip()
            if key == "type":
                current["type"] = value
            elif key == "name":
                current["name"] = value
            elif key == "pattern":
                current["pattern"] = value

flag_for_type = {"topic": "--topic", "group": "--group",
                 "cluster": "--cluster", "transactional_id": "--transactional-id"}

for a in acls:
    if not all(a[k] for k in ("principal", "type", "name")) or not a["ops"]:
        sys.exit(f"incomplete ACL entry: {a}")
    cmd = f"{kafka_bin}/kafka-acls.sh --bootstrap-server {bootstrap}"
    if a["pattern"] == "prefixed":
        cmd += f" {flag_for_type[a['type']]} prefixed:{a['name']}"
    elif a["type"] == "cluster":
        cmd += " --cluster"
    else:
        cmd += f" {flag_for_type[a['type']]} {a['name']}"
    ops = " ".join(f"--operation {op}" for op in a["ops"])
    cmd += f" --add --allow-principal {a['principal']} {ops}"
    print(cmd)

PY
)"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "$CMDS"
  exit 0
fi

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

echo "Applying ACLs to $BROKER ($BOOTSTRAP)..."
echo "$CMDS" | while IFS= read -r cmd; do
  echo "  + ${cmd#*kafka-acls.sh }"
  run_kafka_cmd "$cmd $AUTH_ARGS"
done

echo "Current ACLs in cluster:"
run_kafka_cmd "$KAFKA_BIN/kafka-acls.sh --bootstrap-server $BOOTSTRAP $AUTH_ARGS --list"
