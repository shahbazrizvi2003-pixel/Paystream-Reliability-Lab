#!/usr/bin/env bash
# Generate (and optionally register) the SCRAM-SHA-512 service credentials
# (WP2.2, FR-08). Registration uses the mTLS-only BROKER listener as the
# kafka-setup bootstrap identity, so it works on a fully secured cluster.
# Usage: security/scripts/create-scram-users.sh [--help] [--generate-only]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SECRETS_DIR="$REPO_ROOT/security/secrets"
CREDS_FILE="$SECRETS_DIR/scram-credentials.env"

# Principal order matters: svc-admin-ci is applied first so later steps can
# use it; svc-monitor/svc-schema-registry are Doc 03 matrix additions
# (recorded in ADR-0006).
PRINCIPALS=(svc-admin-ci svc-schema-registry svc-txn-producer svc-fraud svc-notify svc-connect svc-monitor)

BOOTSTRAP_MTLS="localhost:${KAFKA_BROKER_HOST_PORT:-19094}"
CLIENT_CERT_DIR="$REPO_ROOT/security/certs/clients/kafka-setup"
STORE_PASSWORD="${KAFKA_SSL_PASSWORD:-paystream-lab}"
DRY_RUN=0
GENERATE_ONLY=0

usage() {
  cat <<'EOF'
create-scram-users.sh - create SCRAM-SHA-512 credentials for service principals

--generate-only writes security/secrets/scram-credentials.env with a random
password per principal (git-ignored). Passwords are stable: re-runs reuse
existing entries so credentials never silently rotate.

--apply registers the credentials on the running cluster via the mTLS-only
BROKER listener (host port 19094) using the kafka-setup bootstrap identity
(Kafka 4.0: --add-scram was removed from kafka-configs.sh, credentials go
through --add-config per KIP-554):
  kafka-configs --alter --add-scram 'SCRAM-SHA-512=[password=...]'

Principals (Document 03 section 6 + ADR-0006 additions):
  svc-admin-ci, svc-schema-registry, svc-txn-producer, svc-fraud,
  svc-notify, svc-connect, svc-monitor

Options:
  -h, --help          Show this help.
  --generate-only     Only write the credentials file; no cluster required.
  --dry-run           With --apply: print the registration commands.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --generate-only) GENERATE_ONLY=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }

# ---- generate credentials file ------------------------------------------------
mkdir -p "$SECRETS_DIR"
if [ ! -f "$CREDS_FILE" ]; then
  : > "$CREDS_FILE"
  chmod 600 "$CREDS_FILE" 2>/dev/null || true
fi

changed=0
for principal in "${PRINCIPALS[@]}"; do
  if ! grep -q "^${principal}=" "$CREDS_FILE"; then
    password="$(openssl rand -base64 24 | tr -d '\n')"
    printf '%s=%s\n' "$principal" "$password" >> "$CREDS_FILE"
    echo "  generated credential for $principal"
    changed=1
  fi
done
[ "$changed" -eq 1 ] || echo "  credentials file up to date ($CREDS_FILE)"

# ---- derive per-service JAAS env files (compose env_file inputs) -------------
# One file per principal-consuming container; only the JAAS line is secret,
# protocol/mechanism stay in the committed security overlay.
JAAS_DIR="$SECRETS_DIR/jaas"
mkdir -p "$JAAS_DIR"

jaas_line() {
  local principal="$1" password="$2"
  printf 'org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";' \
    "$principal" "$password"
}

password_of() {
  grep "^$1=" "$CREDS_FILE" | head -1 | cut -d= -f2-
}

write_jaas() {
  local file="$1" var="$2" principal="$3"
  local pw
  pw="$(password_of "$principal")"
  printf '%s=%s\n' "$var" "$(jaas_line "$principal" "$pw")" > "$JAAS_DIR/$file"
  chmod 600 "$JAAS_DIR/$file" 2>/dev/null || true
}

write_jaas "schema-registry.env" "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG" "svc-schema-registry"
write_jaas "txn-producer.env"   "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG"     "svc-txn-producer"
write_jaas "fraud-detector.env" "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG"     "svc-fraud"
write_jaas "notifier.env"       "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG"     "svc-notify"

# ---- broker server-side JAAS -------------------------------------------------
# Even though SCRAM verification reads from the metadata log, Kafka requires a
# JAAS context for EVERY SASL listener: without a '<listener>.KafkaServer'
# entry the broker aborts with "Could not find a 'KafkaServer' ... entry in
# the JAAS configuration" (seen on the 2026-10-28 first secured run). The
# listener can't express the hyphenated config key via a KAFKA_* env var, so
# the file is mounted into /etc/kafka/secrets/kafka-server.conf and wired in
# through KAFKA_OPTS=-Djava.security.auth.login.config=... in the overlay.
# Identity: svc-admin-ci (the SCRAM admin per ADR-0002). One entry per SASL
# listener: the file loader looks up '<listener>.KafkaServer' exactly
# (PLAINTEXT_HOST was added on the 2026-10-28 first secured run).
{
  {
    printf 'plaintext.KafkaServer {\n      '; jaas_line svc-admin-ci "$(password_of svc-admin-ci)"; printf '\n};\n';
    printf 'plaintext_host.KafkaServer {\n      '; jaas_line svc-admin-ci "$(password_of svc-admin-ci)"; printf '\n};\n';
  } > "$JAAS_DIR/kafka-server.conf"
}
chmod 600 "$JAAS_DIR/kafka-server.conf" 2>/dev/null || true

# POSIX-safe names for compose ${} interpolation (kafka-exporter command).
{
  printf 'SVC_MONITOR_PASSWORD=%s\n' "$(password_of svc-monitor)"
} > "$SECRETS_DIR/interpolation.env"
chmod 600 "$SECRETS_DIR/interpolation.env" 2>/dev/null || true

# Host-side admin properties for apply-acls.sh / apply-topics.sh against the
# secured cluster (connects as svc-admin-ci over SASL_SSL on kafka-1's
# PLAINTEXT_HOST listener, host port 19091 by default).
{
  printf 'security.protocol=SASL_SSL\n'
  printf 'sasl.mechanism=SCRAM-SHA-512\n'
  printf 'sasl.jaas.config=%s\n' "$(jaas_line svc-admin-ci "$(password_of svc-admin-ci)")"
  printf 'ssl.truststore.location=%s/security/certs/ca/truststore.p12\n' "$REPO_ROOT"
  printf 'ssl.truststore.type=PKCS12\n'
  printf 'ssl.truststore.password=%s\n' "${KAFKA_SSL_PASSWORD:-paystream-lab}"
} > "$SECRETS_DIR/admin.properties"
chmod 600 "$SECRETS_DIR/admin.properties" 2>/dev/null || true

if [ "$GENERATE_ONLY" -eq 1 ]; then
  echo "Credentials generated:"
  echo "  $CREDS_FILE (principals + passwords, git-ignored)"
  echo "  $JAAS_DIR/*.env (per-service JAAS, compose env_file)"
  echo "  $SECRETS_DIR/interpolation.env (compose interpolation vars)"
  echo "Apply to the cluster with: $0 --apply"
  exit 0
fi

# ---- register on the cluster --------------------------------------------------
PROPS_FILE="$(mktemp)"
trap 'rm -f "$PROPS_FILE"' EXIT
cat > "$PROPS_FILE" <<EOF
security.protocol=SSL
ssl.keystore.location=$CLIENT_CERT_DIR/kafka-setup.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=$STORE_PASSWORD
ssl.truststore.location=$REPO_ROOT/security/certs/ca/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=$STORE_PASSWORD
EOF

apply_user() {
  local principal="$1" password="$2"
  local cmd
  cmd="kafka-configs.sh --bootstrap-server $BOOTSTRAP_MTLS --command-config $PROPS_FILE --alter --entity-type users --entity-name $principal --add-config 'SCRAM-SHA-512=[password=$password]'"
  if [ "$DRY_RUN" -eq 1 ]; then
    echo "  + ${cmd%%--add-config*} --add-config 'SCRAM-SHA-512=[password=***]'"
  else
    eval "$cmd"
    echo "  + registered $principal"
  fi
}

echo "Registering SCRAM users via $BOOTSTRAP_MTLS (mTLS, User:CN=kafka-setup)..."
for principal in "${PRINCIPALS[@]}"; do
  password="$(grep "^${principal}=" "$CREDS_FILE" | head -1 | cut -d= -f2-)"
  apply_user "$principal" "$password"
done
echo "Done. Service JAAS configs read passwords from $CREDS_FILE (compose env_file)."
