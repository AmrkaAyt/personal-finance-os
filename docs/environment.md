# Environment Structure

## Overview

The project uses a two-layer environment model:

1. Root `.env`
   - shared secrets
   - shared infrastructure endpoints
   - Docker Compose host ports
   - shared queue/topic names

2. `env/<service>.env`
   - service-specific defaults
   - `HTTP_ADDR`
   - consumer groups
   - service-local prefixes and limits

Go services auto-load:
- root `.env`
- root `.env.local`
- `env/<service>.env`
- `env/<service>.local.env`

Shell-provided environment variables still win over file values.

## Files

- [.env.example](/W:/Projects/personal-finance-os/.env.example)
- `env/api-gateway.env`
- `env/auth-service.env`
- `env/ingest-service.env`
- `env/parser-service.env`
- `env/ledger-service.env`
- `env/rule-engine.env`
- `env/notification-service.env`
- `env/analytics-writer.env`
- `env/realtime-gateway.env`

## Local Development

1. Copy `.env.example` to `.env`
2. Adjust secrets and host ports if needed
3. Run infra with Docker Compose
4. Run a service with `go run ./cmd/<service>`

Example:

```bash
cp .env.example .env
docker compose -f deploy/docker-compose.yml up -d postgres redis mongodb rabbitmq kafka clickhouse
go run ./cmd/auth-service
```

## Local Overrides

Use one of:

- root `.env.local`
- `env/<service>.local.env`
- shell env vars

Good examples:
- `env/notification-service.local.env` for Telegram bot credentials
- `env/notification-service.local.env` for `INGEST_SERVICE_URL` / `PARSER_SERVICE_URL` overrides during bot development
- `.env.local` for machine-specific ports

## Compose

`deploy/docker-compose.yml` reads:

- shared values from root `.env`
- service defaults from `env/<service>.env`
- container-specific overrides inline in Compose for internal hostnames such as `postgres`, `redis`, `mongodb`, `rabbitmq`, `kafka`, `clickhouse`
