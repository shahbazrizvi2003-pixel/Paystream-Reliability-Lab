#!/usr/bin/env bash
# One-shot security bootstrap executed inside the kafka-setup container of
# docker-compose.security.yml (WP2.1/WP2.2). Sequence:
#   1. wait for the KRaft quorum over the mTLS BROKER listener
#   2. register every SCRAM-SHA-512 service credential
#      (identity: mTLS certificate CN=kafka-setup, a cluster superuser)
#   3. apply the ACL matrix and the topic catalogue as User:svc-admin-ci
# Runs to completion once; the compose service is
# `condition: service_completed_successfully` for everything that depends on
# credentials existing.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="/opt/paystream/security/secrets/scram-credentials.env"
# apache/kafka:4.0.0 does NOT put the CLI on PATH (verified with the 2026-10-28
# first secured run: bare kafka-metadata-quorum.sh led to a fake "quorum not
# reachable"); every CLI below is called unqualified, so prepend the bin dir.
export PATH="/opt/kafka/bin:$PATH"
KAFKA_SETUP_CERTS="/etc/kafka/certs/kafka-setup"
CA_TRUSTSTORE="/etc/kafka/certs/ca/truststore.p12"
BROKER_MTLS="${KAFKA_SETUP_MTLS_BOOTSTRAP:-kafka-1:29094}"
BROKER_SASL="${KAFKA_SETUP_SASL_BOOTSTRAP:-kafka-1:29092}"
STORE_PASSWORD="${KAFKA_SSL_PASSWORD:-paystream-lab}"
TIMEOUT="${KAFKA_SETUP_TIMEOUT:-180}"

# The principal list must match create-scram-users.sh (order: admin first).
PRINCIPALS=(svc-admin-ci svc-schema-registry svc-txn-producer svc-fraud svc-notify svc-connect svc-monitor)

if [ ! -f "$SECRETS_FILE" ]; then
  echo "ERROR: $SECRETS_FILE missing - run on the host first:" >&2
  echo "  security/scripts/create-scram-users.sh --generate-only" >&2
  exit 1
fi
# NOTE: scram-credentials.env deliberately is NOT sourced: keys are
# principal names with hyphens ("svc-admin-ci=...", invalid shell
# identifiers) and every consumer below parses with grep/cut instead
# (the raw `source` aborted the 2026-10-28 bootstrap run with
# "command not found" on line 1).

MTLS_PROPS="$(mktemp)"
ADMIN_PROPS="$(mktemp)"
trap 'rm -f "$MTLS_PROPS" "$ADMIN_PROPS"' EXIT

cat > "$MTLS_PROPS" <<EOF
security.protocol=SSL
ssl.keystore.location=$KAFKA_SETUP_CERTS/kafka-setup.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=$STORE_PASSWORD
ssl.truststore.location=$CA_TRUSTSTORE
ssl.truststore.type=PKCS12
ssl.truststore.password=$STORE_PASSWORD
EOF

echo "[1/4] Waiting for KRaft quorum via $BROKER_MTLS (timeout ${TIMEOUT}s)..."
deadline=$((SECONDS + TIMEOUT))
until kafka-metadata-quorum.sh --bootstrap-server "$BROKER_MTLS" \
        --command-config "$MTLS_PROPS" describe --status 2>/dev/null | grep -q "LeaderId"; do
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "ERROR: quorum not reachable within ${TIMEOUT}s" >&2
    exit 1
  fi
  sleep 3
done
echo "  quorum healthy."

echo "[2/4] Registering SCRAM-SHA-512 credentials..."
for principal in "${PRINCIPALS[@]}"; do
  password="$(grep "^${principal}=" "$SECRETS_FILE" | head -1 | cut -d= -f2-)"
  if [ -z "$password" ]; then
    echo "ERROR: no credential for $principal in $SECRETS_FILE" >&2
    exit 1
  fi
  kafka-configs.sh --bootstrap-server "$BROKER_MTLS" --command-config "$MTLS_PROPS" \
    --alter --entity-type users --entity-name "$principal" \
    --add-config "SCRAM-SHA-512=[password=$password]"
  echo "  + $principal"
done

ADMIN_PASSWORD="$(grep '^svc-admin-ci=' "$SECRETS_FILE" | head -1 | cut -d= -f2-)"
cat > "$ADMIN_PROPS" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="svc-admin-ci" password="$ADMIN_PASSWORD";
ssl.truststore.location=$CA_TRUSTSTORE
ssl.truststore.type=PKCS12
ssl.truststore.password=$STORE_PASSWORD
EOF

echo "[3/4] Applying ACL matrix (FR-09)..."
"$SCRIPT_DIR/../../kafka-config/scripts/apply-acls.sh" --direct \
  --bootstrap "$BROKER_SASL" --command-config "$ADMIN_PROPS"

echo "[4/4] Applying topic catalogue (FR-02)..."
"$SCRIPT_DIR/../../kafka-config/scripts/apply-topics.sh" --direct \
  --bootstrap "$BROKER_SASL" --command-config "$ADMIN_PROPS"

echo "Security bootstrap complete."
