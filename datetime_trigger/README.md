# Date & Time Trigger Plugin

A production-ready **cron/time-based trigger plugin** for the `workflow_executor` orchestration platform. Written in Go. No external auth required.

---

## Capabilities

| Capability Key | Name | Config Fields | Output |
|---|---|---|---|
| `datetime_every_day_at` | Every day at | `scheduled_at`: `"HH:MM"` (UTC) | `check_time` |
| `datetime_every_hour_at` | Every hour at | `scheduled_at`: `"MM"` (minute, 00–59) | `check_time` |
| `datetime_every_day_of_week_at` | Every day of the week at | `day_of_week`: `"Monday"`, `scheduled_at`: `"HH:MM"` | `check_time` |
| `datetime_every_month_on` | Every month on the | `day_of_month`: `1–31`, `scheduled_at`: `"HH:MM"` | `check_time` |
| `datetime_every_year_on` | Every year on | `scheduled_at`: `"MM-DD HH:MM"` | `check_time` |

All times are **UTC**. The `check_time` payload field is an RFC 3339 timestamp.

---

## Architecture

```
workflow_executor  ──POST /setup──►  datetime_trigger
                                          │
                              (ticks every minute)
                                          │
                              shouldFire? ─── YES ──► RabbitMQ
                                                      Exchange: EVENT_MESSAGE
                                                      Routing key: workflow_id
```

The scheduler aligns itself to the start of the next whole minute after `/setup` is called, then checks once per minute whether the current time matches the configured schedule.

---

## Directory Structure

```
datetime_trigger/
├── main.go                   # HTTP server + global state
├── go.mod
├── Dockerfile
├── models/
│   └── models.go             # TriggerConfig, TriggerEvent, RegistrationRequest, …
├── services/
│   ├── registration.go       # Self-registration with workflow_executor
│   └── scheduler.go          # Per-instance scheduling logic
└── worker/
    └── publisher.go          # RabbitMQ publisher
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `WORKFLOW_EXECUTOR_URL` | `http://localhost:8082` | URL of the workflow_executor |
| `PLUGIN_HOST` | `localhost` | Host the executor uses to reach this plugin |
| `PLUGIN_PORT` | `8085` | Port this plugin listens on |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ connection string |

---

## HTTP Endpoints

| Route | Method | Purpose |
|---|---|---|
| `/setup` | POST | Activate a trigger for a workflow |
| `/remove` | POST | Deactivate a trigger |
| `/health` | GET | Health check + active trigger count |

### POST /setup — Example Payloads

**Every day at 09:30 UTC:**
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid",
  "capability_key": "datetime_every_day_at",
  "config": {
    "scheduled_at": "09:30"
  }
}
```

**Every hour at minute 15:**
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid",
  "capability_key": "datetime_every_hour_at",
  "config": {
    "scheduled_at": "15"
  }
}
```

**Every Wednesday at 14:00 UTC:**
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid",
  "capability_key": "datetime_every_day_of_week_at",
  "config": {
    "day_of_week": "Wednesday",
    "scheduled_at": "14:00"
  }
}
```

**Every month on the 1st at 00:00 UTC:**
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid",
  "capability_key": "datetime_every_month_on",
  "config": {
    "day_of_month": 1,
    "scheduled_at": "00:00"
  }
}
```

**Every year on July 4th at 12:00 UTC:**
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid",
  "capability_key": "datetime_every_year_on",
  "config": {
    "scheduled_at": "07-04 12:00"
  }
}
```

### POST /remove
```json
{
  "id": "trigger-uuid",
  "workflow_id": "workflow-uuid"
}
```

### GET /health — Response
```json
{
  "status": "ok",
  "active_triggers": 3,
  "timestamp": "2026-02-20T10:00:00Z"
}
```

---

## Running Locally

```bash
# 1. Start dependencies (RabbitMQ + workflow_executor)
docker run -d -p 5672:5672 rabbitmq:3-management

# 2. Download Go dependencies
cd datetime_trigger
go mod tidy

# 3. Run
PLUGIN_PORT=8085 \
WORKFLOW_EXECUTOR_URL=http://localhost:8082 \
RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
go run .
```

## Running with Docker

```bash
docker build -t datetime_trigger .

docker run -d \
  -p 8085:8085 \
  -e WORKFLOW_EXECUTOR_URL=http://host.docker.internal:8082 \
  -e RABBITMQ_URL=amqp://guest:guest@host.docker.internal:5672/ \
  datetime_trigger
```

---

## Design Notes

- **One goroutine per trigger instance** — each `/setup` call creates an isolated `Scheduler`. Multiple workflows can register simultaneously without interference.
- **Minute-level precision** — the ticker aligns to the next `:00` second on startup, then fires once per minute. This matches typical automation platform behaviour.
- **Day-of-month clamping** — if `day_of_month = 31` and the current month has only 30 days, the trigger fires on day 30.
- **Idempotent re-setup** — calling `/setup` with the same `id` stops the existing scheduler before starting a new one, so config changes take effect immediately.
- **RabbitMQ reconnection** — the publisher detects a closed connection and reconnects lazily on the next publish attempt.
- **No auth required** — the Date & Time trigger is self-contained; `_auth_context` is accepted but ignored.
