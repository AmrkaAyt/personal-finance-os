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
   - Kafka quarantine / retry policy
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
- `env/kafka-bootstrap.env`
- `env/ledger-service.env`
- `env/rule-engine.env`
- `env/notification-service.env`
- `env/analytics-writer.env`
- `env/realtime-gateway.env`
- `env/quarantine-operator.env`
- `env/sensitive-data-maintenance.env`

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
- `env/notification-service.local.env` for `API_GATEWAY_EXTERNAL_URL`, `INGEST_SERVICE_URL`, and `PARSER_SERVICE_URL` overrides during bot development
- `.env.local` for machine-specific ports
- `env/sensitive-data-maintenance.local.env` for one-shot data hygiene runs

## Compose

`deploy/docker-compose.yml` reads:

- shared values from root `.env`
- service defaults from `env/<service>.env`
- container-specific overrides inline in Compose for internal hostnames such as `postgres`, `redis`, `mongodb`, `rabbitmq`, `kafka`, `clickhouse`

## Sensitive Data Keys

The encryption layer uses:

- `DATA_ENCRYPTION_KEY_ID`
- `DATA_ENCRYPTION_KEY_REF`
- `DATA_ENCRYPTION_KEY_B64`
- `DATA_ENCRYPTION_LEGACY_KEYS_REF`
- `DATA_ENCRYPTION_LEGACY_KEYS`

Supported secret ref schemes:

```text
env:ENV_VAR_NAME
file:/absolute/or/relative/path
```

Local development can use:

```text
DATA_ENCRYPTION_KEY_REF=env:DATA_ENCRYPTION_KEY_B64
```

Non-local environments should use file-backed secret refs rather than raw env key material.

`DATA_ENCRYPTION_LEGACY_KEYS` format when referenced:

```text
old-v1=BASE64_KEY_1,old-v2=BASE64_KEY_2
```

Use this when decrypting historical data during key rotation or maintenance.

## Application Environment

The root `.env` should define:

- `APP_ENV=local` for local development

Non-local modes such as `production` or `staging` activate fail-fast validation for security-critical defaults.

Current examples:
- `JWT_SECRET=dev-secret` is rejected outside local-like modes
- `AUTH_ALLOW_SEEDED_USERS=true` is rejected outside local-like modes
- placeholder encryption keys are rejected outside local-like modes
- obvious insecure DSN fragments such as `finance:finance@` and `sslmode=disable` are rejected outside local-like modes

## Telegram Link Binding

Notification service can use:

- `TELEGRAM_LINK_CODE_TTL`
- `API_GATEWAY_EXTERNAL_URL`
- `NOTIFICATION_DIGEST_ENABLED`
- `NOTIFICATION_DIGEST_WINDOW`
- `NOTIFICATION_DIGEST_POLL_INTERVAL`
- `NOTIFICATION_DIGEST_MAX_ITEMS`
- `NOTIFICATION_DIGEST_PREFIX`

These are used for the safer bot binding flow:

1. user sends `/link` in Telegram,
2. bot returns a one-time code,
3. user confirms the code through `POST /api/v1/notifications/telegram/link/confirm` under JWT,
4. service stores durable `chat_id -> user_id` binding in Redis.

The digest settings control Telegram anti-spam batching for non-critical alerts.

Notification preferences are persisted in PostgreSQL and exposed through:

- `GET /api/v1/notifications/preferences`
- `PUT /api/v1/notifications/preferences`

Supported preference fields:

- `telegram_enabled`
- `batch_non_critical`
- `quiet_hours_enabled`
- `quiet_start_minute`
- `quiet_end_minute`
- `quiet_timezone`
- `disabled_alert_types`

## Kafka Consumer Recovery

Kafka consumer services can define:

- `KAFKA_QUARANTINE_TOPIC`
- `KAFKA_CONSUMER_RETRY_BACKOFF`
- `KAFKA_CONSUMER_RETRY_MAX_ATTEMPTS`

Current default quarantine topic:

```text
event.quarantine
```

These values are currently defined in:

- `env/ledger-service.env`
- `env/rule-engine.env`
- `env/analytics-writer.env`
- `env/realtime-gateway.env`

## Quarantine Operator

The one-shot quarantine tooling uses:

- `QUARANTINE_ACTION`
- `QUARANTINE_LIMIT`
- `QUARANTINE_SCAN_TIMEOUT`
- `QUARANTINE_DRY_RUN`
- `QUARANTINE_REPLAY_TOPIC_OVERRIDE`
- `QUARANTINE_FILTER_ID`
- `QUARANTINE_FILTER_SERVICE`
- `QUARANTINE_FILTER_SOURCE_TOPIC`
- `QUARANTINE_FILTER_ERROR_KIND`

Example:

```bash
docker compose -f deploy/docker-compose.yml run --rm quarantine-operator
docker compose -f deploy/docker-compose.yml run --rm -e QUARANTINE_ACTION=list -e QUARANTINE_LIMIT=10 quarantine-operator
docker compose -f deploy/docker-compose.yml run --rm -e QUARANTINE_ACTION=replay -e QUARANTINE_DRY_RUN=true -e QUARANTINE_FILTER_ID=<event_id> quarantine-operator
```
