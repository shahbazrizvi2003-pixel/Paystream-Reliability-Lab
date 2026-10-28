#!/usr/bin/env bash
# Issue a broker certificate signed by the lab CA (WP2.1, FR-08).
# Produces the PKCS12 keystore Kafka mounts for the SSL listeners plus the
# truststore the broker uses to validate inter-broker/controller peers (mTLS).
# Certificates are deliberately short-lived (default 30 days) so drill D5 can
# exercise real rotation via rotate-certs.sh.
# Usage: security/scripts/gen-broker-cert.sh --name broker-1 [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CA_DIR="$REPO_ROOT/security/certs/ca"
CERTS_ROOT="$REPO_ROOT/security/certs"

NAME=""
VALIDITY_DAYS=30
STORE_PASSWORD="paystream-lab"

usage() {
  cat <<'EOF'
gen-broker-cert.sh - issue a short-lived broker certificate (mTLS server)

Creates security/certs/<name>/{<name>.key,<name>.crt,<name>.p12,
truststore.p12}. The keystore carries the full SAN set for the Compose
network (kafka-1..3, localhost, 127.0.0.1) so a per-broker SAN mistake can
never break a lab handshake. The truststore contains the lab CA and is what
Kafka uses to validate peer (broker/controller) certificates.

Options:
  -h, --help            Show this help.
  --name NAME           Broker cert name (default: broker-1).
  --validity-days N     Certificate validity (default: 30 - short-lived on
                        purpose; rotate with rotate-certs.sh).
  --password PASS       PKCS12 store password (default: paystream-lab; lab
                        only, never reuse outside the sandbox).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --name) NAME="$2"; shift 2 ;;
    --validity-days) VALIDITY_DAYS="$2"; shift 2 ;;
    --password) STORE_PASSWORD="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

[ -n "$NAME" ] || { echo "ERROR: --name is required" >&2; usage; exit 1; }
# MSYS (Git Bash on Windows) mangles the -subj DN ("/C=US/..." looks like a
# POSIX path). Excluding just the DN prefix keeps file-path conversion intact;
# on Linux this variable is simply unused.
export MSYS2_ARG_CONV_EXCL="/C=US"
command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }
[ -f "$CA_DIR/ca.crt" ] || { echo "ERROR: no CA found - run gen-ca.sh first" >&2; exit 1; }

OUT_DIR="$CERTS_ROOT/$NAME"
mkdir -p "$OUT_DIR"

KEY="$OUT_DIR/$NAME.key"
CRT="$OUT_DIR/$NAME.crt"
CSR="$OUT_DIR/$NAME.csr"
P12="$OUT_DIR/$NAME.p12"
TRUSTSTORE="$OUT_DIR/truststore.p12"
EXT_FILE="$OUT_DIR/san.ext"

openssl req -new -newkey rsa:2048 -nodes \
  -keyout "$KEY" -out "$CSR" \
  -subj "/C=US/O=PayStream Lab/OU=Brokers/CN=$NAME" \
  >/dev/null 2>&1

cat > "$EXT_FILE" <<EOF
basicConstraints = CA:FALSE
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:kafka-1,DNS:kafka-2,DNS:kafka-3,DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -in "$CSR" -CA "$CA_DIR/ca.crt" -CAkey "$CA_DIR/ca.key" \
  -CAcreateserial -days "$VALIDITY_DAYS" -sha256 \
  -extfile "$EXT_FILE" -out "$CRT" >/dev/null 2>&1
rm -f "$EXT_FILE"

openssl pkcs12 -export -in "$CRT" -inkey "$KEY" -certfile "$CA_DIR/ca.crt" \
  -name "$NAME" -out "$P12" -passout "pass:$STORE_PASSWORD" >/dev/null 2>&1

rm -f "$TRUSTSTORE"
# Same keytool-not-openssl rule as gen-ca.sh (Java TrustedCertEntry).
keytool -importcert -alias paystream-lab-ca -file "$CA_DIR/ca.crt" \
  -keystore "$TRUSTSTORE" -storetype PKCS12 \
  -storepass "$STORE_PASSWORD" -noprompt >/dev/null 2>&1

# Client properties for host-side mTLS tooling and the container healthcheck.
# In-container keystore paths are normalized by the security overlay mounts.
cat > "$OUT_DIR/command.properties" <<EOF
security.protocol=SSL
ssl.keystore.location=/etc/kafka/secrets/broker.p12
ssl.keystore.type=PKCS12
ssl.keystore.password=$STORE_PASSWORD
ssl.truststore.location=/etc/kafka/secrets/truststore.p12
ssl.truststore.type=PKCS12
ssl.truststore.password=$STORE_PASSWORD
EOF
HOST_PROPS="$OUT_DIR/host-command.properties"
sed 's|/etc/kafka/secrets/broker.p12|'"$P12"'|; s|/etc/kafka/secrets/truststore.p12|'"$TRUSTSTORE"'|' \
  "$OUT_DIR/command.properties" > "$HOST_PROPS"

rm -f "$CSR"

echo "Broker certificate issued:"
echo "  keystore:    $P12 (PKCS12, name=$NAME)"
echo "  truststore:  $TRUSTSTORE (PKCS12, lab CA)"
echo "  mTLS props:  $HOST_PROPS (host-side admin tooling)"
echo "  validity:    ${VALIDITY_DAYS} days"
openssl x509 -in "$CRT" -noout -subject -enddate
