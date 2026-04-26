# Personal Finance OS: Event Contracts

Date: 2026-04-26

## Purpose

This document captures the first explicit Kafka event contract baseline.

The wire payloads are still JSON objects for V1 compatibility. Producers now validate required
fields before publishing, and published messages include:

- `x-event-type`
- `x-event-version`

The source-of-truth contract registry lives in:

- [`internal/eventcontracts/contracts.go`](../internal/eventcontracts/contracts.go)

## Contracts

| Topic | Type | Version | Producer | Consumers |
| --- | --- | --- | --- | --- |
| `statement.uploaded` | `statement.uploaded` | `v1` | `ingest-service` | audit / observability |
| `statement.parsed` | `statement.parsed` | `v1` | `parser-service` | `ledger-service` |
| `transaction.upserted` | `transaction.upserted` | `v1` | `ledger-service` | `rule-engine`, `analytics-writer`, `realtime-gateway` |
| `alert.created` | `alert.created` | `v1` | `rule-engine` | `analytics-writer`, `realtime-gateway` |
| `event.quarantine` | `event.quarantine` | `v1` | Kafka consumer runtime | `quarantine-operator` |

## Compatibility Rules

- Required fields must remain present for the life of `v1`.
- New optional fields may be added without changing the version.
- Removing or renaming a required field requires a new version.
- Consumers should ignore unknown fields.
- Replay from quarantine preserves the original payload and adds replay audit headers.

## Quarantine Replay Commit

Replay defaults to dry-run. A real replay commit requires all of:

- `QUARANTINE_DRY_RUN=false`
- `QUARANTINE_REPLAY_APPROVED=true`
- non-empty `QUARANTINE_REPLAY_REASON`
- non-empty `QUARANTINE_REPLAY_OPERATOR`

Replay messages include:

- `x-quarantine-replay-id`
- `x-quarantine-replayed-at`
- `x-quarantine-replay-approved-at`
- `x-quarantine-replay-operator`
- `x-quarantine-replay-reason`
