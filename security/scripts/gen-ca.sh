#!/usr/bin/env bash
# Generate the PayStream lab Certificate Authority (WP2.1, FR-08).
# The CA signs broker and client certificates; its private key never leaves
# security/certs/ca/ (git-ignored). Idempotent: re-running without --force
# keeps the existing CA so already-issued certificates stay valid.
# Usage: security/scripts/gen-ca.sh [--help]
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CA_DIR="$REPO_ROOT/security/certs/ca"

CA_CN="paystream-lab-ca"
VALIDITY_DAYS=3650
FORCE=0
STORE_PASSWORD="${KAFKA_SSL_PASSWORD:-paystream-lab}"

usage() {
  cat <<'EOF'
gen-ca.sh - create the lab root CA used for all Kafka listeners

Produces security/certs/ca/{ca.key,ca.crt,truststore.p12}. The CA is
long-lived; broker and client certificates are deliberately short-lived
(see gen-broker-cert.sh and gen-client-cert.sh). Re-running is a no-op
unless --force is given, so rotation never silently invalidates the trust
chain. truststore.p12 is the shared client truststore every service copies.

Options:
  -h, --help            Show this help.
  --force               Replace an existing CA (invalidates every issued cert).
  --cn NAME             CA common name (default: paystream-lab-ca).
  --validity-days N     CA validity in days (default: 3650).
  --password PASS       PKCS12 truststore password (default: paystream-lab).
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --force) FORCE=1; shift ;;
    --cn) CA_CN="$2"; shift 2 ;;
    --validity-days) VALIDITY_DAYS="$2"; shift 2 ;;
    --password) STORE_PASSWORD="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

# MSYS (Git Bash on Windows) mangles the -subj DN ("/C=US/..." looks like a
# POSIX path). Excluding just the DN prefix keeps file-path conversion intact;
# on Linux this variable is simply unused.
export MSYS2_ARG_CONV_EXCL="/C=US"
command -v openssl >/dev/null 2>&1 || { echo "ERROR: openssl is required" >&2; exit 1; }

if [ -f "$CA_DIR/ca.crt" ] && [ "$FORCE" -ne 1 ]; then
  echo "CA already exists at $CA_DIR (re-run with --force to replace)."
  exit 0
fi

mkdir -p "$CA_DIR"
if [ "$FORCE" -eq 1 ]; then
  rm -f "$CA_DIR/ca.key" "$CA_DIR/ca.crt" "$CA_DIR/ca.srl" "$CA_DIR/truststore.p12"
fi
rm -f "$CA_DIR/truststore.p12"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$CA_DIR/ca.key" -out "$CA_DIR/ca.crt" \
  -days "$VALIDITY_DAYS" \
  -subj "/C=US/O=PayStream Lab/OU=Platform/CN=$CA_CN" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -addext "subjectKeyIdentifier=hash" \
  >/dev/null 2>&1

# Build the truststore with keytool, NOT openssl pkcs12: JDK's PKCS12 parser
# counts a store assembled from openssl cert bags as 0 entries (no Java
# TrustedCertEntry marking), which makes every TLS client fail with
# "the trustAnchors parameter must be non-empty" (seen on the first secured
# run 2026-10-28). keytool writes proper trusted entries.
keytool -importcert -alias paystream-lab-ca -file "$CA_DIR/ca.crt" \
  -keystore "$CA_DIR/truststore.p12" -storetype PKCS12 \
  -storepass "$STORE_PASSWORD" -noprompt >/dev/null 2>&1

echo "CA created:"
echo "  cert:        $CA_DIR/ca.crt"
echo "  key:         $CA_DIR/ca.key"
echo "  truststore:  $CA_DIR/truststore.p12 (PKCS12, clients)"
echo "  CN:          $CA_CN"
echo "  validity:    ${VALIDITY_DAYS} days"
