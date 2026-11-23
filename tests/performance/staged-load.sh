#!/usr/bin/env bash
# T10/T14 - staged throughput test (WP2.9, FR-04).
# Runs kafka-producer-perf-test at 1,000 / 2,500 / 5,000 msg/s stages,
# records p99 latency and throughput per stage.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAGES="${STAGED_STAGES:-1000 2500 5000}"
DURATION=120
RECORD_SIZE=512
TOPIC="bank.transactions.v1"

usage() {
  cat <<'EOF'
T10 - staged producer perf test at configurable msg/s stages
Options:
  -h, --help      Show this help.
  --stages LIST   Space-separated msg/s stages (default: "1000 2500 5000").
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --stages) STAGES="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
RESULTS_DIR="docs/performance/results"
mkdir -p "$RESULTS_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"

echo "== T10 staged load - $(date -u) =="
echo "host: $(uname -s) / $(uname -m), stages: $STAGES" | tee "$RESULTS_DIR/staged-$STAMP.txt"

# stop the app generator so perf-test traffic is the only load
curl -fsS -X POST "http://txn-producer:8080/api/generation/stop" >/dev/null 2>&1 || true

for rate in $STAGES; do
  echo "-- stage ${rate} msg/s (${DURATION}s)..."
  docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
    --topic "$TOPIC" \
    --num-records $((rate * DURATION)) \
    --record-size "$RECORD_SIZE" \
    --throughput "$rate" \
    --producer-props bootstrap.servers=localhost:29092 \
      acks=all enable.idempotence=true compression.type=lz4 linger.ms=10 \
    2>&1 | tee "$RESULTS_DIR/producer-${rate}-$STAMP.txt"

  echo "-- consumer catch-up for stage ${rate}..."
  docker exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
    --bootstrap-server localhost:29092 \
    --topic "$TOPIC" \
    --messages $((rate * DURATION)) \
    --timeout 120000 \
    2>&1 | tail -3 | tee "$RESULTS_DIR/consumer-${rate}-$STAMP.txt"

  echo ""
done

echo "== T10 done. Results in $RESULTS_DIR/ =="
cat "$RESULTS_DIR/staged-$STAMP.txt"
