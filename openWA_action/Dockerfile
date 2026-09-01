# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Install dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -o openwa_action main.go

# Final runtime stage
FROM alpine:latest

# Add ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /app/openwa_action .

# OpenWA Action plugin listens on PLUGIN_LISTEN_PORT or 8080 by default
EXPOSE 8080

# Command to run the executable
CMD ["./openwa_action"]
