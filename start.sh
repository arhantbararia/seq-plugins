#!/bin/sh
# ─────────────────────────────────────────────────────────────────────────────
# Sequels Plugin Engine — Unified Start Script
# Dynamically assigns ports, generates nginx.conf, and starts all plugins.
# ─────────────────────────────────────────────────────────────────────────────

set -e

# ── Dynamic OAuth Setup ──────────────────────────────────────────────────────
echo "Waiting 15 seconds before running database setup..."
sleep 15
python3 setup_oauth.py

BASE_PORT=8081

# ── Plugin Registry (Automated) ──────────────────────────────────────────────
PLUGINS=""
if [ -f active_plugins.txt ]; then
    echo "Dynamically loading plugins from active_plugins.txt..."
    while IFS= read -r line || [ -n "$line" ]; do
        # Skip empty lines or comments
        [ -z "$line" ] || [ "$(echo "$line" | cut -c1)" = "#" ] && continue
        
        # Derive binary name and prefix
        # active_plugins.txt names: datetime_trigger, telegram_action, etc.
        BINARY="${line}_bin"
        PREFIX=$(echo "$line" | sed -E 's/_(trigger|action)//')
        
        PLUGINS="$PLUGINS ${BINARY}:${PREFIX}"
    done < active_plugins.txt
    PLUGINS=$(echo $PLUGINS | xargs) # trim
else
    # Fallback to hardcoded list if file is missing
    PLUGINS="datetime_trigger_bin:datetime telegram_action_bin:telegram youtube_trigger_bin:youtube spotify_action_bin:spotify"
fi

# ── Generate nginx.conf ──────────────────────────────────────────────────────
echo "Generating nginx.conf..."

cat > /etc/nginx/nginx.conf <<'NGINX_HEADER'
events {
    worker_connections 128;
}

http {
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

        # Redirect /${PREFIX} to /${PREFIX}/
        location = /${PREFIX} {
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
