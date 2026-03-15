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

## Real ingest -> parser -> ledger pipeline
The repository now includes a real local pipeline for `ingest-service`, `parser-service`, and `ledger-service`:
- `MongoDB` stores raw imports and parsed projections
- `RabbitMQ` carries parse jobs on `parse.statement`
- `Kafka` emits `statement.uploaded` and `statement.parsed` events
- `PostgreSQL` persists ledger transactions and categories
- `ledger-service` consumes `statement.parsed` and emits `transaction.upserted`

### Start with Docker Compose
```bash
docker compose -f deploy/docker-compose.yml up --build ingest-service parser-service ledger-service mongodb rabbitmq kafka postgres
```

### Upload a sample statement
```bash
curl -F "file=@examples/sample-statement.csv" http://localhost:8082/imports/raw
```

### Check raw import status
```bash
curl http://localhost:8082/imports/<import_id>
```

### Check parsed result
```bash
curl http://localhost:8083/parser/results/<import_id>
```

### Check ledger transactions
```bash
curl http://localhost:8084/api/v1/transactions
```

## Local run without containers
1. Start infrastructure from `deploy/docker-compose.yml`.
2. Run a service with `go run ./cmd/<service-name>`.
3. Configure service-specific env vars as needed.

## Current scope
This bootstrap includes compileable core service skeletons, shared platform code, OpenAPI, graceful shutdown, and a working Docker-backed ingest/parser pipeline. The remaining services still use scaffolds and can now be wired incrementally on the same foundation.

## Documentation
- [Master Documentation Index](docs/master-spec.md)
- [Product Charter](docs/product-charter.md)
- [Product Architecture Specification](docs/product-architecture-spec.md)
- [Domain Specification](docs/domain-spec.md)
- [V1 Specification](docs/v1-spec.md)


