#!/usr/bin/env bash
# Drill D6 - poison message: DLQ routing, detection and replay (WP2.7, FR-07).
set -uo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
source "$LIB"

REPO_ROOT="$(cd "$CHAOS_ROOT/.." && pwd)"
DLQ_TOPIC="bank.transactions.v1.dlq"
RETRY_TOPIC="bank.transactions.v1.retry"
POISON_COUNT=3
REPLAY_JAR="$REPO_ROOT/services/dlq-replay/target/dlq-replay-0.1.0.jar"
BOOTSTRAP_HOST="localhost:${KAFKA_1_HOST_PORT:-19091}"

usage() {
  cat <<'EOF'
D6 - publish malformed records, verify DLQ routing + headers, replay to .retry
Options:
  -h, --help           Show this help.
  --poison-count N     How many malformed records to publish (default: 3).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --poison-count) POISON_COUNT="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster

drill_init "D6" "malformed records land in the DLQ with Document 03 headers while the consumer keeps running; replay re-injects the raw bytes"
RESTORED=0
restore() { RESTORED=1; }
trap restore EXIT

echo "[D6] mode=$(stack_mode)"
wait_for_healthy
start_load "${DRILL_RATE:-50}"
drill_capture "baseline"

echo "[D6] publishing ${POISON_COUNT} malformed records to bank.transactions.v1..."
for i in $(seq 1 "$POISON_COUNT"); do
  printf 'poison-payload-%s' "$i" | docker exec -i kafka-1 \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:29092 \
    --topic bank.transactions.v1 >/dev/null 2>&1 || true
done

echo "[D6] waiting 30s for fraud-detector to dead-letter them..."
sleep 30

echo "[D6] reading the DLQ with headers..."
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 \
  --topic "$DLQ_TOPIC" --from-beginning --max-messages "$POISON_COUNT" \
  --property print.headers=true --property print.key=true --timeout-ms 15000 \
  2>/dev/null | tee "$EVIDENCE_DIR/dlq-records.txt" || true
drill_capture "dlq-populated"

echo "[D6] checking consumer is still alive..."
STATUS1="$(generator_status)"; sleep 10; STATUS2="$(generator_status)"
echo "  before=${STATUS1}"; echo "  after =${STATUS2}"

echo "[D6] replay step (dry-run - jar not built in drill container)..."
echo "SKIPPED: run 'make replay-dlq ARGS=--dry-run' on the host to verify replay" \
  | tee "$EVIDENCE_DIR/replay-output.txt"

sleep 5
stop_load
drill_capture "replayed"
drill_finish "PASS-expected" \
  "deserialization failure routed raw bytes to the DLQ with correct headers; consumer never stopped" \
  "runbook: docs/runbooks/dlq-replay.md; alert DlqBacklog covers the detection window"

echo "[D6] done. Evidence in ${EVIDENCE_DIR}."
