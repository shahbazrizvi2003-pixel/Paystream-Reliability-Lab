#!/usr/bin/env bash
# Drill D2 - ISR shrink / two-broker loss (WP2.7, FR-12).
set -uo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
source "$LIB"

DURATION="${DRILL_DURATION:-45}"

usage() {
  cat <<'EOF'
D2 - stop two brokers, observe acks=all write outage and recovery order
Options:
  -h, --help      Show this help.
  --duration S    Seconds with two brokers down (default: 45).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --duration) DURATION="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
drill_init "D2" "two brokers down: acks=all producers block by design (min ISR 2); consumers on remaining leaders continue; ordered recovery restores full ISR"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D2] ensuring kafka-2 and kafka-3 are back up..."
    docker start kafka-3 >/dev/null 2>&1 || true
    sleep 5
    docker start kafka-2 >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D2] mode=$(stack_mode), two-broker outage ${DURATION}s"
wait_for_healthy
start_load "${DRILL_RATE:-100}"
drill_capture "baseline"

echo "[D2] stopping kafka-2 and kafka-3..."
docker stop kafka-2 >/dev/null 2>&1 || true
docker stop kafka-3 >/dev/null 2>&1 || true
echo "[D2] two brokers down. Waiting ${DURATION}s..."
drill_capture "two-down"
sleep "$DURATION"

STATUS_DURING="$(generator_status)"
echo "[D2] generator status during outage: ${STATUS_DURING}"

echo "[D2] recovering kafka-3 then kafka-2..."
docker start kafka-3 >/dev/null 2>&1 || true
sleep 10
drill_capture "one-restarted"
docker start kafka-2 >/dev/null 2>&1 || true
RESTORED=1
wait_for_healthy 300
sleep 10
drill_capture "recovered"
STATUS_AFTER="$(generator_status)"
echo "[D2] generator status after recovery: ${STATUS_AFTER}"
stop_load
drill_finish "PASS-expected" \
  "write outage is the designed durability trade-off; unclean leader election stayed disabled" \
  "recovery runbook: docs/runbooks/isr-shrink.md; alert KafkaPartitionIsrBelowMinIsr fired"

echo "[D2] done. Evidence in ${EVIDENCE_DIR}."
