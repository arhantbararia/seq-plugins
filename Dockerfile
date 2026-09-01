# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: Build all Go plugin binaries
# ═══════════════════════════════════════════════════════════════════════════════
FROM golang:1.25-alpine AS builder

RUN apk --no-cache add ca-certificates

WORKDIR /src

# ── datetime_trigger ──────────────────────────────────────────────────────────
COPY datetime_trigger/ ./datetime_trigger/
RUN cd datetime_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/datetime_trigger_bin .

# ── spotify_action ────────────────────────────────────────────────────────────
COPY spotify_action/ ./spotify_action/
RUN cd spotify_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/spotify_action_bin .

# ── telegram_action ───────────────────────────────────────────────────────────
COPY telegram_action/ ./telegram_action/
RUN cd telegram_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/telegram_action_bin .

# ── slack_action ────────────────────────────────────────────────────────────
COPY slack_action/ ./slack_action/
RUN cd slack_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/slack_action_bin .

# ── youtube_trigger ───────────────────────────────────────────────────────────
COPY youtube_trigger/ ./youtube_trigger/
RUN cd youtube_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/youtube_trigger_bin .

# ── github_trigger ────────────────────────────────────────────────────────────
COPY github_trigger/ ./github_trigger/
RUN cd github_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/github_trigger_bin .

# ── googlesheets_action ───────────────────────────────────────────────────────
COPY googlesheets_action/ ./googlesheets_action/
RUN cd googlesheets_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/googlesheets_action_bin .

# ── instagram_trigger ─────────────────────────────────────────────────────────
COPY instagram_trigger/ ./instagram_trigger/
RUN cd instagram_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/instagram_trigger_bin .

# ── rss_trigger ───────────────────────────────────────────────────────────────
COPY rss_trigger/ ./rss_trigger/
RUN cd rss_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/rss_trigger_bin .

# ── x_action ──────────────────────────────────────────────────────────────────
COPY x_action/ ./x_action/
RUN cd x_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/x_action_bin .

# # ── whatsapp_action TO DO──────────────────────────────────────────────────────────
# COPY whatsapp_action/ ./whatsapp_action/
# RUN cd whatsapp_action && \
#     go mod download && \
#     CGO_ENABLED=0 GOOS=linux go build -o /app/whatsapp_action_bin .

# ── openWA_action ────────────────────────────────────────────────────────────
COPY openWA_action/ ./openWA_action/
RUN cd openWA_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/openWA_action_bin .


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: Build the OpenWA Node.js server
# ═══════════════════════════════════════════════════════════════════════════════
FROM node:22-slim AS openwa-builder

# Build tools for native npm modules (bcrypt, better-sqlite3, etc.)
RUN apt-get update && apt-get install -y \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /openwa

# Copy package files first for layer caching
COPY internal/OpenWA/package*.json ./

# Install ALL dependencies (devDependencies needed for nest build + vite build)
RUN npm ci --include=dev

# Copy source code
COPY internal/OpenWA/ .

# Build NestJS API (dist/) and dashboard SPA (dashboard/dist/)
RUN npm run build && \
    cd dashboard && npm ci --include=dev && npm run build

# Prune devDependencies for the production copy
RUN npm ci --omit=dev && npm cache clean --force


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 3: Runtime — Node.js + Chromium + Go binaries + Nginx
# ═══════════════════════════════════════════════════════════════════════════════
FROM node:22-slim

# Install Chromium, Nginx, Python, and runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    fonts-liberation \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    dumb-init \
    curl \
    procps \
    nginx \
    python3 \
    python3-pip \
    tinyproxy \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Chromium/Puppeteer configuration
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true

# Hugging Face requirement: run as non-root user with UID 1000
# (node:22-slim already has a 'node' user but not at UID 1000)
RUN useradd -m -u 1000 user || true

# Prepare writable directories for Nginx and OpenWA
RUN mkdir -p /var/lib/nginx/body /var/log/nginx /run /app/openwa-server /app/openwa-data/sessions /app/openwa-data/media && \
    chown -R user:user /var/lib/nginx /var/log/nginx /run /etc/nginx /app

WORKDIR /app

# ── Copy Go plugin binaries (statically linked, work on any Linux) ────────────
COPY --from=builder /app/datetime_trigger_bin .
COPY --from=builder /app/spotify_action_bin .
COPY --from=builder /app/telegram_action_bin .
COPY --from=builder /app/slack_action_bin .
COPY --from=builder /app/youtube_trigger_bin .
COPY --from=builder /app/github_trigger_bin .
COPY --from=builder /app/googlesheets_action_bin .
COPY --from=builder /app/instagram_trigger_bin .
COPY --from=builder /app/rss_trigger_bin .
COPY --from=builder /app/x_action_bin .
# COPY --from=builder /app/whatsapp_action_bin .
COPY --from=builder /app/openWA_action_bin .

# ── Copy OpenWA server from builder ──────────────────────────────────────────
COPY --from=openwa-builder /openwa/dist ./openwa-server/dist
COPY --from=openwa-builder /openwa/dashboard/dist ./openwa-server/dashboard/dist
COPY --from=openwa-builder /openwa/node_modules ./openwa-server/node_modules
COPY --from=openwa-builder /openwa/package.json ./openwa-server/package.json

# ── Copy startup scripts and config ─────────────────────────────────────────
COPY start.sh .
COPY active_plugins.txt .
COPY setup_oauth.py .
COPY requirements.txt .
COPY proxy_rotator.py .
COPY tinyproxy.conf.template .

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages

RUN chmod +x start.sh && chown -R user:user /app

# OpenWA environment defaults (can be overridden via HF Secrets)
ENV HOME=/app/openwa-data
ENV XDG_CONFIG_HOME=/tmp/.config
ENV XDG_CACHE_HOME=/tmp/.cache

# Switch to non-root user
USER user

EXPOSE 7860

CMD ["./start.sh"]
