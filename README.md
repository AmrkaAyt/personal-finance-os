# personal-finance-os

Monorepo for the core services of a personal finance operating system.

## Core services
- api-gateway
- auth-service
- ingest-service
- parser-service
- ledger-service
- rule-engine
- notification-service
- analytics-writer
- realtime-gateway

## Real gateway -> ingest -> parser -> ledger -> alert -> analytics -> realtime pipeline
The repository now includes a real local pipeline for `api-gateway`, `auth-service`, `ingest-service`, `parser-service`, `ledger-service`, `rule-engine`, `notification-service`, `analytics-writer`, and `realtime-gateway`:
- `api-gateway` is the external entrypoint for REST and WebSocket traffic
- `auth-service` issues JWT pairs and stores refresh sessions in `Redis`
- `MongoDB` stores raw imports and parsed projections
- `RabbitMQ` carries parse jobs on `parse.statement`
- `Kafka` emits `statement.uploaded` and `statement.parsed` events
- `PostgreSQL` persists ledger transactions and categories
- `ledger-service` consumes `statement.parsed` and emits `transaction.upserted`
- `rule-engine` consumes `transaction.upserted`, evaluates alerts, and publishes `send.telegram`
- `notification-service` consumes `send.telegram` with retry and DLQ support
- `notification-service` also polls Telegram for basic commands and statement document intake
- `analytics-writer` consumes `transaction.upserted` and `alert.created`
- `ClickHouse` stores analytical projections for spend and alerts
- `realtime-gateway` consumes `transaction.upserted` and `alert.created`
- malformed Kafka payloads are quarantined into `event.quarantine` without killing consumers
- `Redis` stores WebSocket presence and subscription state

### Start with Docker Compose
```bash
docker compose -f deploy/docker-compose.yml up --build api-gateway auth-service ingest-service parser-service ledger-service rule-engine notification-service analytics-writer realtime-gateway mongodb rabbitmq kafka postgres redis clickhouse
```

### Run schema migrations explicitly
```bash
docker compose -f deploy/docker-compose.yml run --rm migrate
```

### Run sensitive-data maintenance explicitly
```bash
docker compose -f deploy/docker-compose.yml run --rm sensitive-data-maintenance
```

### Run integration tests against the gateway
```bash
INTEGRATION_TESTS=1 INTEGRATION_BASE_URL=http://localhost:8080 go test -tags=integration ./tests/integration/...
```

### Login through the gateway
```bash
curl -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"demo","password":"demo"}'
```

### Upload a sample statement through the gateway
```bash
curl -F "file=@examples/sample-statement.csv" \
  -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/imports/raw
```

### Upload a PDF statement through the gateway
```bash
curl -F "file=@/path/to/statement.pdf" \
  -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/imports/raw
```

### Check raw import status
```bash
curl -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/imports/<import_id>
```

### Check parsed result
```bash
curl -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/parser/results/<import_id>
```

### Check ledger transactions
```bash
curl -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/api/v1/transactions
```

### Inspect rule-engine config
```bash
curl http://localhost:8085/api/v1/rules/config
```

### Inspect notification worker status
```bash
curl -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/api/v1/notifications/status
```

### Queue a Telegram demo notification
```bash
curl -X POST \
  -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/api/v1/notifications/telegram/demo
```

### Trigger one Telegram polling cycle
```bash
curl -X POST \
  -H "Authorization: Bearer <access_token>" \
  http://localhost:8080/api/v1/notifications/telegram/poll/once
```

### Telegram bot capabilities in V1
- `/help`, `/status`, `/report`, `/alerts`, `/transactions`
- accepts `CSV` and text-based `PDF` statement documents
- forwards documents to `ingest-service`
- sends follow-up parse summary after `parser-service` completes

### Query spend analytics
```bash
curl -H "Authorization: Bearer <access_token>" \
  "http://localhost:8080/api/v1/analytics/projections/daily-spend?from=2026-03-01&to=2026-03-31"
```

### Query alert analytics
```bash
curl -H "Authorization: Bearer <access_token>" \
  "http://localhost:8080/api/v1/analytics/projections/alerts?from=2026-03-01&to=2026-03-31"
```

### Inspect realtime presence
```bash
curl -H "Authorization: Bearer <access_token>" \
  "http://localhost:8080/api/v1/presence"
```

### Connect to realtime WebSocket
```bash
wscat -c "ws://localhost:8080/ws?access_token=<access_token>&channels=dashboard,alerts,transactions"
```

## Local run without containers
1. Start infrastructure from `deploy/docker-compose.yml`.
2. Run a service with `go run ./cmd/<service-name>`.
3. Configure service-specific env vars as needed.

## Environment
1. Copy `.env.example` to `.env`
2. Keep shared settings in `.env`
3. Keep service defaults in `env/<service>.env`
4. Put machine-specific or secret overrides in `.env.local` or `env/<service>.local.env`

Details:
- [Environment Structure](docs/environment.md)

## Current scope
This bootstrap includes shared platform code, OpenAPI, graceful shutdown, startup retry/backoff for external dependencies, versioned database migrations, sensitive-data maintenance, and a working Docker-backed event pipeline for auth, gateway routing, import, parsing, ledger persistence, rule evaluation, notification dispatch, analytics projections, and realtime fan-out. Gateway-level integration tests cover `login -> import -> parse` and `login -> create transaction -> analytics/alerts`. Telegram V1 now supports real outbound delivery, basic polling commands, and statement document intake for `CSV` and text-based `PDF`.

## Documentation
- [Current Implementation Status](docs/implementation-status.md)
- [Technical Debt Register](docs/technical-debt-register.md)
- [Master Documentation Index](docs/master-spec.md)
- [Product Charter](docs/product-charter.md)
- [Product Architecture Specification](docs/product-architecture-spec.md)
- [Domain Specification](docs/domain-spec.md)
- [Environment Structure](docs/environment.md)
- [V1 Specification](docs/v1-spec.md)


