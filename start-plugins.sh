#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# Sequels Plugin Engine — Plugins-Only Start Script (Docker Compose mode)
# Launches Go plugins + Nginx. OpenWA runs in a separate container.
# ─────────────────────────────────────────────────────────────────────────────

set -e

BASE_PORT=8081

# OpenWA host — in Docker Compose this resolves to the 'openwa' service
OPENWA_HOST="${OPENWA_HOST:-openwa}"
OPENWA_PORT="${OPENWA_PORT:-2785}"

echo "══════════════════════════════════════════════════════════════"
echo "  Sequels Plugin Engine (Plugins-Only Mode)"
echo "  OpenWA server expected at: ${OPENWA_HOST}:${OPENWA_PORT}"
echo "══════════════════════════════════════════════════════════════"

# ── Plugin Registry (Automated) ──────────────────────────────────────────────
PLUGINS=""
if [ -f active_plugins.txt ]; then
    echo "Dynamically loading plugins from active_plugins.txt..."
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines or comments
        [ -z "$line" ] || [ "$(echo "$line" | cut -c1)" = "#" ] && continue
        
        # Derive binary name and route prefix components
        BINARY="${line}_bin"
        PROVIDER=$(echo "$line" | sed -E 's/_(trigger|action)//')
        TYPE=$(echo "$line" | grep -oE '(trigger|action)' || echo "plugin")
        
        # New 2-level prefix: /youtube/trigger or /spotify/action
        ROUTE_PREFIX="/${PROVIDER}/${TYPE}"
        
        PLUGINS="$PLUGINS ${BINARY}:${ROUTE_PREFIX}"
    done < active_plugins.txt
    PLUGINS=$(echo $PLUGINS | xargs) # trim
else
    # Fallback to hardcoded list if file is missing
    PLUGINS="datetime_trigger_bin:/datetime/trigger telegram_action_bin:/telegram/action youtube_trigger_bin:/youtube/trigger spotify_action_bin:/spotify/action"
fi

# ── Generate nginx.conf ──────────────────────────────────────────────────────
echo "Generating nginx.conf..."

cat > /etc/nginx/nginx.conf <<NGINX_HEADER
pid /tmp/nginx.pid;

events {
    worker_connections 128;
}

http {
    client_body_temp_path /tmp/client_temp;
    proxy_temp_path       /tmp/proxy_temp_path;
    fastcgi_temp_path     /tmp/fastcgi_temp;
    uwsgi_temp_path       /tmp/uwsgi_temp;
    scgi_temp_path        /tmp/scgi_temp;

    server {
        listen 7860;

        # Root status endpoint
        location / {
            return 200 '{"status": "ok"}';
            add_header Content-Type application/json;
        }

        # Global uptime endpoint
        location /health {
            return 200 'Sequels Plugin Engine is alive';
            add_header Content-Type text/plain;
        }

        # ── OpenWA Server reverse proxy ──────────────────────────────
        # Proxies to the OpenWA container via Docker Compose network.
        # External callers (goat-backend) use: https://<domain>/openwa-server/api/...
        location /openwa-server/ {
            proxy_pass http://${OPENWA_HOST}:${OPENWA_PORT}/;
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 120s;
            proxy_connect_timeout 10s;
        }
        location = /openwa-server {
            return 301 \$scheme://\$host\$request_uri/;
        }

NGINX_HEADER

PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    ROUTE_PREFIX=$(echo "$ENTRY" | cut -d: -f2)

    cat >> /etc/nginx/nginx.conf <<NGINX_LOCATION
        # Route for ${BINARY} -> ${ROUTE_PREFIX}
        location ${ROUTE_PREFIX}/ {
            proxy_pass http://127.0.0.1:${PORT};
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 120s;
            proxy_connect_timeout 10s;
        }

        location = ${ROUTE_PREFIX} {
            return 301 \$scheme://\$host\$request_uri/;
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

# ── Start Proxy Rotator & Tinyproxy ──────────────────────────────────────────
echo "Initializing first proxy..."
python3 proxy_rotator.py --init

echo "Starting tinyproxy..."
tinyproxy -c /app/tinyproxy.conf

export HTTP_PROXY=http://127.0.0.1:8888
export HTTPS_PROXY=http://127.0.0.1:8888

echo "Starting background proxy rotator..."
python3 proxy_rotator.py &

# ── Start Plugin Binaries ────────────────────────────────────────────────────
PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    ROUTE_PREFIX=$(echo "$ENTRY" | cut -d: -f2)
    
    echo "Starting plugin ${BINARY} with prefix ${ROUTE_PREFIX} on internal port ${PORT}..."
    PLUGIN_LISTEN_PORT=$PORT /app/${BINARY} &

    PORT=$((PORT + 1))
done

# ── Dynamic OAuth Setup ──────────────────────────────────────────────────────
(
    echo "Waiting 15 seconds for plugin binaries to stabilize before running database setup..."
    sleep 15
    python3 setup_oauth.py
) &

# ── Start Nginx in the foreground ────────────────────────────────────────────
echo "Starting Nginx on port 7860..."
nginx -g 'daemon off;'
