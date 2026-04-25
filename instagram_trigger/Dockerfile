FROM golang:alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o plugin_bin .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/plugin_bin .

ENV RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
ENV WORKFLOW_EXECUTOR_URL="http://localhost:8082"
ENV PLUGIN_HOST="localhost"
ENV PLUGIN_PORT="8057"
ENV INSTAGRAM_CLIENT_ID=""
ENV INSTAGRAM_CLIENT_SECRET=""


EXPOSE 8057
CMD ["./plugin_bin"]
