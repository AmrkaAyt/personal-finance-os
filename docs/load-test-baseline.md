# Personal Finance OS: Load-Test Baseline

Date: 2026-03-19
Environment: local single-node `docker compose` stack
Scenario: [`loadtests/k6/scenarios/mixed-v1.js`](../loadtests/k6/scenarios/mixed-v1.js)
Exported summary: [`loadtests/out/mixed-summary.json`](../loadtests/out/mixed-summary.json)

## 1. Purpose

This document captures the first stable mixed-traffic baseline for the V1 stack after fixing the
`api-gateway` reverse-proxy connection churn that previously caused:

- `connect: cannot assign requested address`
- failed `ledger-service` write and read requests under mixed load
- crossed `checks` and `http_req_failed` thresholds in `k6`

The baseline is meant to be:

- reproducible,
- source-controlled,
- usable as a regression target for future performance work.

It is not a cross-machine benchmark and should not be treated as a universal throughput claim.

## 2. Command

```powershell
docker compose -f deploy/docker-compose.yml run --rm `
  k6 run --summary-export=/work/loadtests/out/mixed-summary.json `
  /work/loadtests/k6/scenarios/mixed-v1.js
```

## 3. Scenario Shape

The baseline uses the scenario defaults embedded in `mixed-v1.js`:

| Scenario | VUs | Duration |
| --- | ---: | --- |
| `auth_login` | 10 | `30s` |
| `transaction_write` | 20 | `45s` |
| `read_heavy` | 40 | `60s` |
| `import_pipeline` | 5 | `30s` |
| `websocket_connect` | 10 | `30s` |

Maximum concurrent virtual users across all scenarios:

- `85 VUs`

## 4. Thresholds

Configured in the scenario:

- `http_req_failed: rate < 0.03`
- `http_req_duration: p(95) < 1200`
- `checks: rate > 0.98`

Observed result for this baseline:

- thresholds passed

## 5. Baseline Results

### 5.1 HTTP

| Metric | Value |
| --- | ---: |
| `http_reqs` | `214,817` |
| request rate | `3,576.98 req/s` |
| `http_req_failed` | `0.00%` |
| `http_req_duration avg` | `17.84 ms` |
| `http_req_duration p(90)` | `17.65 ms` |
| `http_req_duration p(95)` | `124.07 ms` |
| `http_req_duration max` | `809.91 ms` |

### 5.2 Checks

| Check group | Result |
| --- | --- |
| `login status is 200` | pass |
| `login returned access token` | pass |
| `mixed login is 200` | pass |
| `mixed transaction is 201` | pass |
| `mixed transactions list is 200` | pass |
| `mixed categories list is 200` | pass |
| `mixed analytics summary is 200` | pass |
| `mixed import accepted` | pass |
| `mixed import status is 200` | pass |
| `mixed websocket upgrade is 101` | pass |

Aggregate:

- `checks passed: 215,118`
- `checks failed: 0`

### 5.3 Execution

| Metric | Value |
| --- | ---: |
| `iterations` | `190,815` |
| iteration rate | `3,178.86 iter/s` |
| `vus_max` | `85` |

### 5.4 WebSocket

| Metric | Value |
| --- | ---: |
| `ws_sessions` | `300` |
| `ws_msgs_received` | `1,717` |
| `ws_connecting p(95)` | `18.18 ms` |

## 6. Root Cause of the Previous Failure

The previous mixed run failed not because of PostgreSQL writes or ledger business logic, but because
`api-gateway` created too many short-lived outbound connections to `ledger-service`.

Observed symptom:

- reverse proxy errors with `connect: cannot assign requested address`

Fix applied:

- custom reverse-proxy `Transport`
- tuned keep-alive and connection pooling in [`cmd/api-gateway/main.go`](../cmd/api-gateway/main.go)
- env-backed proxy pool settings in [`env/api-gateway.env`](../env/api-gateway.env)

## 7. Interpretation

What this baseline proves:

- the local V1 stack can sustain mixed auth, writes, reads, imports, and websocket churn without
  request failures,
- the previous gateway transport bottleneck has been removed,
- the current thresholds are realistic and reproducible for the local Docker profile.

What it does not prove:

- multi-node horizontal scalability,
- broker saturation limits under long soak,
- production-grade latency on larger infrastructure,
- OCR or heavier parser workloads.

## 8. Next Performance Steps

1. capture Prometheus/Grafana screenshots and metric snapshots for the same run
2. add per-endpoint tagged thresholds to `k6`
3. run stepped baselines for `100`, `150`, and `200` effective VUs
4. add soak tests longer than `10-15 minutes`
5. add bottleneck notes for `Kafka lag`, `RabbitMQ queue depth`, `PostgreSQL pool saturation`, and `ClickHouse write rate`
