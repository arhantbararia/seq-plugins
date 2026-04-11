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
    CGO_ENABLED=0 GOOS=linux go build -o /app/datetime_bin .

# ── spotify_action ────────────────────────────────────────────────────────────
COPY spotify_action/ ./spotify_action/
RUN cd spotify_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/spotify_bin .

# ── telegram_action ───────────────────────────────────────────────────────────
COPY telegram_action/ ./telegram_action/
RUN cd telegram_action && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/telegram_bin .

# ── youtube_trigger ───────────────────────────────────────────────────────────
COPY youtube_trigger/ ./youtube_trigger/
RUN cd youtube_trigger && \
    go mod download && \
    CGO_ENABLED=0 GOOS=linux go build -o /app/youtube_bin .


# ═══════════════════════════════════════════════════════════════════════════════
# STAGE 2: Runtime — lightweight Alpine with Nginx
# ═══════════════════════════════════════════════════════════════════════════════
FROM alpine:latest

RUN apk --no-cache add ca-certificates nginx

# Hugging Face requirement: run as non-root user with UID 1000
RUN adduser -D -u 1000 user

# Prepare Nginx directories writable by non-root user
RUN mkdir -p /var/lib/nginx/tmp /var/log/nginx /run/nginx && \
    chown -R user:user /var/lib/nginx /var/log/nginx /run/nginx /etc/nginx

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /app/datetime_bin .
COPY --from=builder /app/spotify_bin .
COPY --from=builder /app/telegram_bin .
COPY --from=builder /app/youtube_bin .

# Copy startup script
COPY start.sh .
RUN chmod +x start.sh && chown user:user start.sh

# Switch to non-root user
USER user

EXPOSE 7860

CMD ["./start.sh"]
