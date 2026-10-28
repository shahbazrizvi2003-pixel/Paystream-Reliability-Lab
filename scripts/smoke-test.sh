#!/usr/bin/env bash
# T2 smoke test (MVP gate): verify the end-to-end flow -
# producer -> bank.transactions.v1 -> fraud-detector -> bank.fraud-alerts.v1
# -> notifier -> PostgreSQL notification row.
#
# Precondition: `make up` completed and the stack is healthy.
# Usage: scripts/smoke-test.sh [--help] [--wait 120]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WAIT=120
for arg in "$@"; do
  case "$arg" in
    -h|--help) echo "smoke-test.sh - end-to-end flow check (T2)"; exit 0 ;;
    --wait) WAIT="$2"; shift 2 ;;
    *) echo "unknown option $arg" >&2; exit 1 ;;
  esac
done

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml "$@"
}

echo "== 1. Services reachable =="
curl -fs "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/actuator/health" | grep -q '"status":"UP"' \
  || { echo "FAIL: txn-producer unhealthy" >&2; exit 1; }
curl -fs "http://localhost:${FRAUD_DETECTOR_HOST_PORT:-8082}/actuator/health" | grep -q '"status":"UP"' \
  || { echo "FAIL: fraud-detector unhealthy" >&2; exit 1; }
curl -fs "http://localhost:${NOTIFIER_HOST_PORT:-8083}/actuator/health" | grep -q '"status":"UP"' \
  || { echo "FAIL: notifier unhealthy" >&2; exit 1; }
echo "  all three services report UP"

echo "== 2. Producer generating traffic =="
STATUS="$(curl -fs "http://localhost:${TXN_PRODUCER_HOST_PORT:-8080}/api/generation/status")"
echo "  $STATUS"

echo "== 3. Waiting for a notification row (max ${WAIT}s) =="
deadline=$((SECONDS + WAIT))
while true; do
  COUNT="$(compose exec -T postgres psql -U "${POSTGRES_USER:-paystream}" -d "${POSTGRES_DB:-paystream}" \
    -tAc "SELECT count(*) FROM notifications")"
  if [ "${COUNT:-0}" -ge 1 ]; then
    echo "  PASS: $COUNT notification row(s) present - end-to-end flow works"
    break
  fi
  if [ $SECONDS -ge $deadline ]; then
    echo "FAIL: no notification rows after ${WAIT}s" >&2
    echo "Diagnostics: check 'make logs' for producer/streams/consumer errors" >&2
    exit 1
  fi
  sleep 5
done

echo "== 4. Topic offsets =="
for t in bank.transactions.v1 bank.fraud-alerts.v1 bank.notifications.v1; do
  # Use kafka-topics.sh --describe to confirm topic is healthy (GetOffsetShell removed in Kafka 4.0)
  PARTS=$(compose exec -T kafka-1 sh -c \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092 --describe --topic $t 2>/dev/null" \
    | grep -c "Partition:" || true)
  echo "  $t  partitions: $PARTS (healthy)"
done

echo "SMOKE TEST PASSED"
