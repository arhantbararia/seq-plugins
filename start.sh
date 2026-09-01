#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# Sequels Plugin Engine — Unified Start Script
# Dynamically assigns ports, generates nginx.conf, and starts all plugins.
# Now also launches the bundled OpenWA WhatsApp server.
# ─────────────────────────────────────────────────────────────────────────────

set -e

BASE_PORT=8081
OPENWA_PORT=2785

# ── OpenWA Server Startup ────────────────────────────────────────────────────
echo "══════════════════════════════════════════════════════════════"
echo "  Starting OpenWA WhatsApp Server on port ${OPENWA_PORT}..."
echo "══════════════════════════════════════════════════════════════"

# Create writable directories for OpenWA data, sessions, and Chromium
mkdir -p /app/openwa-data/sessions /app/openwa-data/media /tmp/.config /tmp/.cache

# Clean stale Chromium singleton locks from previous unclean shutdowns
rm -f /app/openwa-data/sessions/*/Singleton* 2>/dev/null || true

# Launch the OpenWA Node.js server in the background.
# Environment variables (API_MASTER_KEY, ENGINE_TYPE, etc.) are set via HF Secrets.
# Defaults are provided here for safety.
PORT=${OPENWA_PORT} \
NODE_ENV="${NODE_ENV:-production}" \
DATABASE_TYPE="${DATABASE_TYPE:-sqlite}" \
DATABASE_NAME="${DATABASE_NAME:-/app/openwa-data/openwa.sqlite}" \
DATABASE_SYNCHRONIZE="${DATABASE_SYNCHRONIZE:-true}" \
ENGINE_TYPE="${ENGINE_TYPE:-whatsapp-web.js}" \
SESSION_DATA_PATH="${SESSION_DATA_PATH:-/app/openwa-data/sessions}" \
PUPPETEER_HEADLESS="${PUPPETEER_HEADLESS:-true}" \
PUPPETEER_ARGS="${PUPPETEER_ARGS:---no-sandbox,--disable-setuid-sandbox,--disable-dev-shm-usage,--disable-gpu}" \
AUTO_START_SESSIONS="${AUTO_START_SESSIONS:-true}" \
STORAGE_TYPE="${STORAGE_TYPE:-local}" \
STORAGE_LOCAL_PATH="${STORAGE_LOCAL_PATH:-/app/openwa-data/media}" \
REDIS_ENABLED="${REDIS_ENABLED:-false}" \
HOME=/app/openwa-data \
XDG_CONFIG_HOME=/tmp/.config \
XDG_CACHE_HOME=/tmp/.cache \
  node /app/openwa-server/dist/main &

OPENWA_PID=$!
echo "OpenWA server launched with PID ${OPENWA_PID}"

# Wait for the OpenWA server to become healthy (max 120 seconds for Chromium startup)
echo "Waiting for OpenWA server to become ready..."
OPENWA_READY=false
for i in $(seq 1 60); do
    if curl -sf http://127.0.0.1:${OPENWA_PORT}/api/health/ready > /dev/null 2>&1; then
        OPENWA_READY=true
        echo "✓ OpenWA server is ready! (took ~${i}×2 seconds)"
        break
    fi
    sleep 2
done

if [ "$OPENWA_READY" = false ]; then
    echo "⚠ WARNING: OpenWA server did not report ready within 120s. Continuing anyway..."
    echo "  (The server may still be initializing Chromium. It will become available shortly.)"
fi

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

cat > /etc/nginx/nginx.conf <<'NGINX_HEADER'
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
        # Strips /openwa-server prefix and forwards to the local Node.js process.
        # External callers (goat-backend) use: https://<hf-space>/openwa-server/api/...
        location /openwa-server/ {
            proxy_pass http://127.0.0.1:2785/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_read_timeout 120s;
            proxy_connect_timeout 10s;
        }
        location = /openwa-server {
            return 301 $scheme://$host$request_uri/;
        }

NGINX_HEADER

PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    # The second part is now the full prefix like /youtube/trigger
    ROUTE_PREFIX=$(echo "$ENTRY" | cut -d: -f2)

    cat >> /etc/nginx/nginx.conf <<NGINX_LOCATION
        # Route for ${BINARY} -> ${ROUTE_PREFIX}
        location ${ROUTE_PREFIX}/ {
            proxy_pass http://127.0.0.1:${PORT}; # No trailing slash preserves the full path
            proxy_set_header Host \$host;
            proxy_set_header X-Real-IP \$remote_addr;
            proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto \$scheme;
            proxy_read_timeout 120s;
            proxy_connect_timeout 10s;
        }

        # Redirect ${ROUTE_PREFIX} to ${ROUTE_PREFIX}/
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
# 1. Initialize the first proxy config
echo "Initializing first proxy..."
python3 proxy_rotator.py --init

# 2. Start tinyproxy in background
echo "Starting tinyproxy..."
tinyproxy -c /app/tinyproxy.conf

# 3. Set global proxy for all plugins started below
export HTTP_PROXY=http://127.0.0.1:8888
export HTTPS_PROXY=http://127.0.0.1:8888

# 4. Start the endless rotation in background
echo "Starting background proxy rotator..."
python3 proxy_rotator.py &

# ── Start Plugin Binaries ────────────────────────────────────────────────────
PORT=$BASE_PORT
for ENTRY in $PLUGINS; do
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    PREFIX=$(echo "$ENTRY" | cut -d: -f2)

    echo "Starting ${BINARY} on port ${PORT} (route: /${PREFIX}/)..."
    BINARY=$(echo "$ENTRY" | cut -d: -f1)
    ROUTE_PREFIX=$(echo "$ENTRY" | cut -d: -f2)
    
    echo "Starting plugin ${BINARY} with prefix ${ROUTE_PREFIX} on internal port ${PORT}..."
    PLUGIN_LISTEN_PORT=$PORT /app/${BINARY} &

    PORT=$((PORT + 1))
done

# ── Dynamic OAuth Setup ──────────────────────────────────────────────────────
# Run after binaries have started to ensure they are ready for configuration
echo "Waiting 15 seconds for plugin binaries to stabilize before running database setup..."
sleep 15
python3 setup_oauth.py

# ── Start Nginx in the foreground ────────────────────────────────────────────
echo "Starting Nginx on port 7860..."
nginx -g 'daemon off;'

