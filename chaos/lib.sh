#!/usr/bin/env bash
# Shared helpers for the D1-D6 failure drills (WP2.7).
# Sourced, not executed:  source "$(dirname "$0")/lib.sh"
# Every drill script: supports --help, is idempotent, captures timestamped
# evidence under chaos/results/<drill>-<timestamp>/ (D5/R5 drill rot guard).

CHAOS_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EVIDENCE_ROOT="$CHAOS_ROOT/results"

compose() {
  docker compose --env-file .env -f infra/compose/docker-compose.yml \
    -f infra/compose/docker-compose.monitoring.yml "$@"
}

drill_init() {
  local drill_code="$1" hypothesis="$2"
  local stamp
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  EVIDENCE_DIR="$EVIDENCE_ROOT/${drill_code}-${stamp}"
  mkdir -p "$EVIDENCE_DIR"
  cat > "$EVIDENCE_DIR/hypothesis.txt" <<EOF
drill: $drill_code
started: $stamp
hypothesis: $hypothesis
host: $(uname -s) / $(uname -m)
EOF
  echo "[$drill_code] evidence dir: $EVIDENCE_DIR"
}

drill_capture() {
  # snapshot cluster + service state for the report (best effort, read-only)
  [ -n "${EVIDENCE_DIR:-}" ] || return 0
  local label="$1"
  compose ps > "$EVIDENCE_DIR/ps-$label.txt" 2>&1 || true
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:29092 --describe \
    > "$EVIDENCE_DIR/topics-$label.txt" 2>&1 || true
  curl -fs "http://prometheus:9090/api/v1/alerts" \
    > "$EVIDENCE_DIR/alerts-$label.json" 2>/dev/null || true
}

drill_finish() {
  local result="$1" root_cause="$2" fix="$3"
  [ -n "${EVIDENCE_DIR:-}" ] || return 0
  cat >> "$EVIDENCE_DIR/hypothesis.txt" <<EOF
result: $result
root_cause: $root_cause
fix: $fix
finished: $(date -u +%Y%m%dT%H%M%SZ)
EOF
  echo "Evidence complete: $EVIDENCE_DIR"
}

wait_for_healthy() {
  local timeout="${1:-180}"
  local deadline=$((SECONDS + timeout))
  until docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
      --bootstrap-server localhost:29092 describe --status 2>/dev/null \
      | grep -q "LeaderId"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "ERROR: quorum not healthy within ${timeout}s" >&2
      return 1
    fi
    sleep 3
  done
  echo "[drill] quorum healthy (LeaderId confirmed)"
}

require_cluster() {
  if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running - drills require the local stack." >&2
    echo "       Start it with 'make up' (or 'make up-secure') first." >&2
    exit 1
  fi
}

stack_mode() {
  if docker exec kafka-1 test -f /etc/kafka/secrets/command.properties 2>/dev/null; then
    echo "secure"
  else
    echo "plaintext"
  fi
}

bootstrap_args() {
  case "$(stack_mode)" in
    secure) echo "--bootstrap-server localhost:29094 --command-config /etc/kafka/secrets/command.properties" ;;
    *) echo "--bootstrap-server localhost:29092" ;;
  esac
}

kafka_exec() {
  local cmd="$1"; shift
  local args=()
  local has_bootstrap=0
  for a in "$@"; do
    [ "$a" = "--bootstrap-server" ] && has_bootstrap=1
    args+=("$a")
  done
  if [ "$has_bootstrap" -eq 0 ]; then
    mapfile -t bootargs < <(bootstrap_args | tr ' ' '\n')
    docker exec "$DRILL_BROKER" /opt/kafka/bin/"$cmd" "${bootargs[@]}" "${args[@]}"
  else
    docker exec "$DRILL_BROKER" /opt/kafka/bin/"$cmd" "${args[@]}"
  fi
}

DRILL_BROKER="${DRILL_BROKER:-kafka-1}"

start_load() {
  local rate="${1:-${DRILL_RATE:-100}}"
  curl -fsS -X POST "http://txn-producer:8080/api/generation/start?rate=${rate}" \
    >/dev/null 2>&1 || true
}

stop_load() {
  curl -fsS -X POST "http://txn-producer:8080/api/generation/stop" \
    >/dev/null 2>&1 || true
}

generator_status() {
  curl -fsS "http://txn-producer:8080/api/generation/status" 2>/dev/null || echo "{}"
}
