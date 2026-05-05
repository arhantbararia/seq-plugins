# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 1: Build all plugin binaries
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


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: Runtime — lightweight Alpine with Nginx
# ═══════════════════════════════════════════════════════════════════════════════
FROM alpine:latest

RUN apk --no-cache add ca-certificates nginx python3 py3-pip

# Hugging Face requirement: run as non-root user with UID 1000
RUN adduser -D -u 1000 user

# Prepare Nginx directories writable by non-root user
RUN mkdir -p /var/lib/nginx/tmp /var/log/nginx /run/nginx && \
    chown -R user:user /var/lib/nginx /var/log/nginx /run/nginx /etc/nginx

WORKDIR /app

# Copy compiled binaries from builder
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

# Copy startup script, setup script, requirements, and config
COPY start.sh .
COPY active_plugins.txt .
COPY setup_oauth.py .
COPY requirements.txt .

# Install Python requirements
RUN pip install --no-cache-dir -r requirements.txt --break-system-packages

RUN chmod +x start.sh && chown user:user start.sh

# Switch to non-root user
USER user

EXPOSE 7860

CMD ["./start.sh"]
