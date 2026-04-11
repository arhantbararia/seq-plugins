#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# Sequels Plugin Engine — Unified Start Script
# Dynamically assigns ports, generates nginx.conf, and starts all plugins.
# ─────────────────────────────────────────────────────────────────────────────

set -e

BASE_PORT=8081

# ── Plugin Registry ──────────────────────────────────────────────────────────
# Format: binary_name:route_prefix
# Binary names match the output of the Dockerfile build stage.
PLUGINS="datetime_bin:datetime spotify_bin:spotify telegram_bin:telegram youtube_bin:youtube"

# ── Generate nginx.conf ──────────────────────────────────────────────────────
echo "Generating nginx.conf..."

cat > /etc/nginx/nginx.conf <<'NGINX_HEADER'
events {
    worker_connections 128;
}

http {
    server {
        listen 7860;

        # Global uptime endpoint
        location /health {
            return 200 'Sequels Plugin Engine is alive';
            add_header Content-Type text/plain;
        }

NGINX_HEADER

PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    PREFIX=$(echo "$ENTRY" | cut -d: -f2)

    cat >> /etc/nginx/nginx.conf <<NGINX_LOCATION
        # Route for ${PREFIX}
        location /${PREFIX}/ {
            proxy_pass http://127.0.0.1:${PORT}/;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 120s;
            proxy_connect_timeout 10s;
        }

NGINX_LOCATION

    PORT=$((PORT + 1))
done

cat >> /etc/nginx/nginx.conf <<'NGINX_FOOTER'
    }
}
NGINX_FOOTER

echo "nginx.conf generated successfully."
cat /etc/nginx/nginx.conf

# ── Start Plugin Binaries ────────────────────────────────────────────────────
PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    PREFIX=$(echo "$ENTRY" | cut -d: -f2)

    echo "Starting ${BINARY} on port ${PORT} (route: /${PREFIX}/)..."
    PLUGIN_LISTEN_PORT=$PORT /app/${BINARY} &

    PORT=$((PORT + 1))
done

# ── Give binaries a moment to bind their ports ───────────────────────────────
sleep 2

# ── Start Nginx in the foreground ────────────────────────────────────────────
echo "Starting Nginx on port 7860..."
nginx -g 'daemon off;'
