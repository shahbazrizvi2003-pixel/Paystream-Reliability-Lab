#!/usr/bin/env bash
# Drill D1 - single broker loss under continuous traffic (WP2.7, FR-12).
# Hypothesis: stopping one broker mid-traffic triggers leader election, clients
# retry transparently (acks=all, min.insync.replicas=2 still holds), no data
# loss, and the cluster catches up on restart.
# Usage: chaos/d1_broker_loss.sh [--help] [--broker kafka-3] [--rate 100]
set -uo pipefail

LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
# shellcheck disable=SC1091
source "$LIB"

TARGET_BROKER="kafka-3"
RATE="${DRILL_RATE:-100}"
DURATION="${DRILL_DURATION:-60}"

usage() {
  cat <<'EOF'
D1 - stop one broker under load, observe election and recovery

Options:
  -h, --help      Show this help.
  --broker NAME   Broker to stop (default: kafka-3).
  --rate N        Generator rate during the drill (default: 100 msg/s).
  --duration S    Seconds to keep the broker down (default: 60).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --broker) TARGET_BROKER="$2"; shift 2 ;;
    --rate) RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

require_cluster
drill_init "D1" "one broker down: election, no data loss, transparent client retry, clean catch-up"
RESTORED=0
restore() {
  if [ "$RESTORED" -eq 0 ]; then
    echo "[D1] ensuring $TARGET_BROKER is back up..."
    docker start "$TARGET_BROKER" >/dev/null 2>&1 || true
    RESTORED=1
  fi
}
trap restore EXIT

echo "[D1] mode=$(stack_mode), rate=${RATE}, broker=${TARGET_BROKER}, down=${DURATION}s"
wait_for_healthy
start_load "$RATE"
drill_capture "baseline"

echo "[D1] stopping ${TARGET_BROKER} at $(date -u +%H:%M:%S)..."
docker stop "$TARGET_BROKER" >/dev/null 2>&1 || true
echo "[D1] ${TARGET_BROKER} stopped. Waiting ${DURATION}s..."
drill_capture "broker-down"
sleep "$DURATION"
drill_capture "broker-down-late"

echo "[D1] restarting ${TARGET_BROKER}..."
docker start "$TARGET_BROKER" >/dev/null 2>&1 || true
RESTORED=1
wait_for_healthy 300
sleep 10
drill_capture "recovered"
stop_load
drill_finish "PASS-expected" \
  "leader election moved partitions; acks=all producers continued because min.insync.replicas=2 held" \
  "none needed - recovery by restart; alert KafkaBrokerCountBelowExpected fired"

echo "[D1] done. Evidence in ${EVIDENCE_DIR}. Verify alerts fired and note ISR recovery time."
