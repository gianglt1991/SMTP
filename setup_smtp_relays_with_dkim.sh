#!/bin/bash

BASE_DIR="smtp-relay-full"
DOMAIN="vetc.com.vn"
SELECTOR_PREFIX="dk"
PORTS=(2525 2526 2527 2528 2529)

mkdir -p "$BASE_DIR"

# Tạo docker-compose.yml
cat <<EOF > "$BASE_DIR/docker-compose.yml"
version: "3.8"
services:
EOF

# Lặp qua 5 SMTP container
for i in {1..5}; do
  NAME="smtp$i"
  SELECTOR="$SELECTOR_PREFIX$i"
  PORT="${PORTS[$((i-1))]}"
  DIR="$BASE_DIR/$NAME"

  mkdir -p "$DIR/config/keys/$SELECTOR"

  # Sinh DKIM key thật bằng opendkim-genkey
  opendkim-genkey -D "$DIR/config/keys/$SELECTOR" -d "$DOMAIN" -s "$SELECTOR" --nosubdomains
  chmod 600 "$DIR/config/keys/$SELECTOR/$SELECTOR.private"

  # Dockerfile
  cat <<EOF > "$DIR/Dockerfile"
FROM alpine
RUN apk add --no-cache postfix opendkim
COPY config/main.cf /etc/postfix/main.cf
COPY config/opendkim.conf /etc/opendkim.conf
COPY config/SigningTable /etc/opendkim/SigningTable
COPY config/KeyTable /etc/opendkim/KeyTable
COPY config/trustedhosts /etc/opendkim/trustedhosts
COPY config/keys/ /etc/opendkim/keys/
RUN adduser -D opendkim && chown -R opendkim /etc/opendkim
CMD sh -c 'opendkim -x /etc/opendkim.conf && postfix start-fg'
EOF

  # main.cf (Postfix)
  cat <<EOF > "$DIR/config/main.cf"
myhostname = $NAME.$DOMAIN
debug_peer_level = 2
relayhost =
inet_interfaces = all
inet_protocols = all
mynetworks = 127.0.0.0/8
relay_domains = *
smtpd_recipient_restrictions = permit_mynetworks, reject_unauth_destination
smtpd_banner = $NAME.$DOMAIN ESMTP
milter_default_action = accept
milter_protocol = 6
smtpd_milters = inet:localhost:8891
non_smtpd_milters = inet:localhost:8891
EOF

  # opendkim.conf
  cat <<EOF > "$DIR/config/opendkim.conf"
Syslog yes
UMask 002
Canonicalization relaxed/simple
Mode sv
SubDomains no
AutoRestart yes
AutoRestartRate 10/1h
Background yes
DNSTimeout 5
SignatureAlgorithm rsa-sha256
UserID opendkim
Socket inet:8891@localhost
PidFile /var/run/opendkim/opendkim.pid
OversignHeaders From
TrustAnchorFile /usr/share/dns/root.key
KeyTable /etc/opendkim/KeyTable
SigningTable refile:/etc/opendkim/SigningTable
ExternalIgnoreList /etc/opendkim/trustedhosts
InternalHosts /etc/opendkim/trustedhosts
EOF

  # SigningTable
  echo "*@$DOMAIN $SELECTOR._domainkey.$DOMAIN" > "$DIR/config/SigningTable"

  # KeyTable
  echo "$SELECTOR._domainkey.$DOMAIN $DOMAIN:$SELECTOR:/etc/opendkim/keys/$SELECTOR/$SELECTOR.private" > "$DIR/config/KeyTable"

  # Trusted Hosts
  echo -e "127.0.0.1\nlocalhost" > "$DIR/config/trustedhosts"

  # Ghi DNS TXT DKIM (hướng dẫn add DNS)
  echo "# === DKIM DNS Record for $NAME ==="
  cat "$DIR/config/keys/$SELECTOR/$SELECTOR.txt"
  echo ""

  # Ghi service vào docker-compose.yml
  cat <<EOF >> "$BASE_DIR/docker-compose.yml"
  $NAME:
    build:
      context: ./$NAME
    hostname: $NAME.$DOMAIN
    ports:
      - "$PORT:25"

EOF

done

echo "✅ Đã tạo thư mục SMTP relay và sinh DKIM key cho $DOMAIN"
echo "📌 Hãy cấu hình các bản ghi DKIM TXT trong DNS theo output ở trên."
