FROM alpine:latest

# Install Postfix, OpenDKIM, and Supervisor
RUN apk add --no-cache postfix opendkim supervisor \
    && rm -rf /var/cache/apk/*
	
RUN apk add --no-cache postfix-exporter

# Create necessary directories and set permissions
RUN mkdir -p /etc/opendkim/keys /var/run/opendkim /var/log/supervisor \
    && chown opendkim:opendkim /etc/opendkim /etc/opendkim/keys /var/run/opendkim \
    && chmod 700 /etc/opendkim/keys

# Create opendkim user if not exists
RUN getent passwd opendkim || adduser -D opendkim

# Copy configuration files
COPY main.cf /etc/postfix/main.cf
COPY opendkim.conf /etc/opendkim/opendkim.conf
COPY trustedhosts /etc/opendkim/trustedhosts
COPY KeyTable /etc/opendkim/KeyTable
COPY SigningTable /etc/opendkim/SigningTable
COPY keys /etc/opendkim/keys
COPY supervisord.conf /etc/supervisord.conf

# Expose SMTP port
EXPOSE 25

# Start services with Supervisor
CMD ["supervisord", "-c", "/etc/supervisord.conf"]

