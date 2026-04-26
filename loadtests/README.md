# Load Testing

The repository includes `k6` scenarios for the current `V1` runtime:

- `loadtests/k6/scenarios/auth-login.js`
- `loadtests/k6/scenarios/transactions-write.js`
- `loadtests/k6/scenarios/read-heavy.js`
- `loadtests/k6/scenarios/import-pipeline.js`
- `loadtests/k6/scenarios/ws-connect.js`
- `loadtests/k6/scenarios/mixed-v1.js`

## What Each Scenario Measures

- `auth-login.js`
  - login throughput
  - JWT issue path latency

- `transactions-write.js`
  - gateway -> ledger write path
  - idempotency key handling under concurrency
  - outbox enqueue pressure

- `read-heavy.js`
  - ledger read path
  - analytics query latency
  - gateway fan-in on read endpoints

- `import-pipeline.js`
  - ingest API throughput
  - Mongo write + Rabbit publish + Kafka emit path

- `ws-connect.js`
  - websocket handshake and connect churn

- `mixed-v1.js`
  - concurrent auth, writes, reads, imports, and websocket connects
  - best entrypoint for broad local stress testing

## Default Runtime Assumptions

The `k6` service uses:

- `K6_BASE_URL=http://api-gateway:8080`
- `K6_USERNAME=demo`
- `K6_PASSWORD=demo`

These values can be overridden with env vars.

## Run Through Docker Compose

Start the application stack first:

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

Run a single scenario:

```bash
docker compose -f deploy/docker-compose.yml run --rm k6 run /work/loadtests/k6/scenarios/read-heavy.js
```

Run the mixed suite:

```bash
docker compose -f deploy/docker-compose.yml run --rm k6 run /work/loadtests/k6/scenarios/mixed-v1.js
```

Run the mixed suite and persist the exported summary:

```bash
docker compose -f deploy/docker-compose.yml run --rm k6 run --summary-export=/work/loadtests/out/mixed-summary.json /work/loadtests/k6/scenarios/mixed-v1.js
```

Override concurrency:

```bash
docker compose -f deploy/docker-compose.yml run --rm \
  -e K6_READ_VUS=100 \
  -e K6_WRITE_VUS=50 \
  -e K6_IMPORT_VUS=15 \
  -e K6_WS_VUS=40 \
  k6 run /work/loadtests/k6/scenarios/mixed-v1.js
```

Run a short smoke locally:

```bash
docker compose -f deploy/docker-compose.yml run --rm \
  -e K6_VUS=5 \
  -e K6_DURATION=10s \
  k6 run /work/loadtests/k6/scenarios/transactions-write.js
```

## What To Capture

For showcase-quality results, keep:

- total request rate
- `p50/p95/p99` latency
- failure rate
- container CPU / memory
- Kafka lag
- Rabbit queue depth
- Postgres connections
- ClickHouse query latency

Store screenshots from:

- Prometheus targets
- Grafana dashboards
- `k6` CLI summary

The current mixed baseline is documented in:

- [`docs/load-test-baseline.md`](../docs/load-test-baseline.md)

## Interpretation Guidance

- `auth-login` should stay fast and stable with near-zero errors.
- `transactions-write` should show whether `ledger-service` or `PostgreSQL` becomes the bottleneck.
- `import-pipeline` stresses `MongoDB`, `RabbitMQ`, and parser concurrency.
- `read-heavy` exposes query hot spots in `ledger-service` and `analytics-writer`.
- `mixed-v1` is the closest approximation to real platform traffic.
