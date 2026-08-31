#!/usr/bin/env sh
#
# Generates a local development CA plus a server and a client certificate for
# fq TLS and mutual TLS. Development only: never ship these to production.
#
#   CERT_DIR    output directory                (default ./certs)
#   CERT_HOSTS  comma separated server SAN list (default localhost,127.0.0.1,::1)
#   CERT_DAYS   validity in days                (default 365)
#   CERT_FORCE  set to 1 to overwrite           (default 0)

set -eu

CERT_DIR="${CERT_DIR:-./certs}"
CERT_HOSTS="${CERT_HOSTS:-localhost,127.0.0.1,::1}"
CERT_DAYS="${CERT_DAYS:-365}"
CERT_FORCE="${CERT_FORCE:-0}"

if ! command -v openssl >/dev/null 2>&1; then
	echo "openssl not found in PATH" >&2
	exit 1
fi

if [ -e "$CERT_DIR/server.crt" ] && [ "$CERT_FORCE" != "1" ]; then
	echo "$CERT_DIR/server.crt already exists; re-run with CERT_FORCE=1 to overwrite" >&2
	exit 1
fi

san_entry() {
	case "$1" in
	*:*) printf 'IP:%s' "$1" ;;
	*[!0-9.]*) printf 'DNS:%s' "$1" ;;
	*) printf 'IP:%s' "$1" ;;
	esac
}

san=""
for host in $(echo "$CERT_HOSTS" | tr ',' ' '); do
	[ -n "$host" ] || continue
	if [ -n "$san" ]; then
		san="$san,$(san_entry "$host")"
	else
		san="$(san_entry "$host")"
	fi
done

if [ -z "$san" ]; then
	echo "CERT_HOSTS resolved to an empty subjectAltName list" >&2
	exit 1
fi

mkdir -p "$CERT_DIR"

tmp="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf '$tmp'" EXIT INT TERM

cat >"$tmp/ca.cnf" <<EOF
[req]
distinguished_name = dn
prompt = no
x509_extensions = ext

[dn]
CN = fq development CA

[ext]
basicConstraints = critical,CA:TRUE,pathlen:0
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF

cat >"$tmp/server.cnf" <<EOF
[req]
distinguished_name = dn
prompt = no

[dn]
CN = fq server
EOF

cat >"$tmp/server.ext" <<EOF
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = $san
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
EOF

cat >"$tmp/client.cnf" <<EOF
[req]
distinguished_name = dn
prompt = no

[dn]
CN = fq client
EOF

cat >"$tmp/client.ext" <<EOF
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
subjectAltName = DNS:fq-client
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
EOF

openssl req -x509 -newkey rsa:2048 -nodes -sha256 \
	-keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" \
	-days "$CERT_DAYS" -config "$tmp/ca.cnf" 2>/dev/null

issue() {
	name="$1"

	openssl req -new -newkey rsa:2048 -nodes -sha256 \
		-keyout "$CERT_DIR/$name.key" -out "$tmp/$name.csr" \
		-config "$tmp/$name.cnf" 2>/dev/null

	openssl x509 -req -sha256 -in "$tmp/$name.csr" \
		-CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
		-out "$CERT_DIR/$name.crt" -days "$CERT_DAYS" \
		-extfile "$tmp/$name.ext" 2>/dev/null
}

issue server
issue client

chmod 600 "$CERT_DIR/ca.key" "$CERT_DIR/server.key" "$CERT_DIR/client.key"
chmod 644 "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt" "$CERT_DIR/client.crt"

openssl verify -CAfile "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt" "$CERT_DIR/client.crt" >/dev/null

echo "-> Wrote development certificates to $CERT_DIR"
echo "   ca.crt      trust anchor for clients and for client_ca_file"
echo "   server.crt  server certificate, SAN: $san"
echo "   client.crt  client certificate for mutual TLS"
echo "   valid for $CERT_DAYS days; development use only"
