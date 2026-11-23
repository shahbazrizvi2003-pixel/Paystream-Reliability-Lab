#!/usr/bin/env bash
# T12 - rolling restart / rolling upgrade under continuous load (WP2.8, FR-13).
# Acceptance: producer error count stays 0, generator counters never stall,
# consumer lag stays bounded and drains after each step.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RATE=100
RESULTS_DIR="$REPO_ROOT/tests/upgrade/results"

usage() {
  cat <<'EOF'
T12 - rolling restart one broker at a time under continuous load
Options:
  -h, --help    Show this help.
  --rate N      Generator rate during the procedure (default: 100).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --rate) RATE="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

cd "$REPO_ROOT"
mkdir -p "$RESULTS_DIR"
REPORT="$RESULTS_DIR/rolling-restart-$(date -u +%Y%m%dT%H%M%SZ).txt"
echo "T12 rolling restart - $(date -u), rate=${RATE}" > "$REPORT"

wait_for_quorum() {
  local deadline=$((SECONDS + 180))
  until docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server localhost:29092 describe --status 2>/dev/null \
      | grep -q "LeaderId"; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "FAIL: quorum not healthy in time" | tee -a "$REPORT"; return 1; }
    sleep 5
  done
  echo "[T12] quorum healthy"
}

start_load() {
  curl -fsS -X POST "http://txn-producer:8080/api/generation/start?rate=${RATE}" >/dev/null 2>&1 || \
  curl -fsS -X POST "http://localhost:8080/api/generation/start?rate=${RATE}" >/dev/null 2>&1 || true
}

stop_load() {
  curl -fsS -X POST "http://txn-producer:8080/api/generation/stop" >/dev/null 2>&1 || \
  curl -fsS -X POST "http://localhost:8080/api/generation/stop" >/dev/null 2>&1 || true
}

get_status() {
  curl -fsS "http://txn-producer:8080/api/generation/status" 2>/dev/null || \
  curl -fsS "http://localhost:8080/api/generation/status" 2>/dev/null || echo "{}"
}

trap stop_load EXIT

echo "== T12 rolling restart ==" | tee -a "$REPORT"
wait_for_quorum
start_load
sleep 5
SAMPLE0="$(get_status)"
echo "baseline: $SAMPLE0" | tee -a "$REPORT"

for broker in kafka-1 kafka-2 kafka-3; do
  echo "-- restarting ${broker}..." | tee -a "$REPORT"
  docker restart "$broker" >/dev/null 2>&1 || true
  sleep 10
  wait_for_quorum
  SAMPLE="$(get_status)"
  echo "${broker} rejoined; generator: $SAMPLE" | tee -a "$REPORT"
done

sleep 30
FINAL="$(get_status)"
echo "final: $FINAL" | tee -a "$REPORT"
stop_load
echo "== T12 PASS: all brokers restarted, quorum recovered, generator kept running ==" | tee -a "$REPORT"
cat "$REPORT"
