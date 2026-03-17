# Personal Finance OS: Technical Debt Register

Version: 0.1.0  
Date: 2026-03-17  
Status: active quality backlog

## 1. Purpose

This document captures the current technical debt of `Personal Finance OS`.

The goal is not just to list problems.
The goal is to make clear:
- what is still weak,
- why it matters,
- why it matters specifically for a showcase-grade backend project,
- in what order it should be fixed.

## 2. Quality Bar for This Project

This repository is intended to demonstrate:
- strong backend architecture,
- sound financial-data handling,
- event-driven design,
- operational maturity,
- correctness under failure,
- clean service boundaries,
- ability to work with a broad stack intentionally, not cosmetically.

For this project, a feature is not considered "done" if it only works in the happy path.
A feature is considered complete only if:
- failure handling is defined,
- security boundaries are explicit,
- data integrity is protected,
- contracts are versioned and understandable,
- operational behavior is predictable,
- tests exist for the risky path, not only for the sunny path.

## 3. Priority Scale

- `P0`: critical integrity/security debt. Should be fixed before expanding product scope.
- `P1`: major architecture/maintainability debt. Strongly affects project quality and showcase value.
- `P2`: important polish/operational debt. Does not block progress but weakens maturity.
- `P3`: nice-to-have or later-stage refinement.

## 4. P0: Critical Integrity and Security Debt

### Recently Resolved

- `2026-03-17`: `ledger-service` was moved to a transactional outbox model for `transaction.upserted`.
- `2026-03-17`: Kafka consumer loops were hardened with permanent/transient error classification, bounded retry, and `event.quarantine` handling.
- `2026-03-17`: legacy plaintext raw imports and historical `raw_line` remnants were migrated/scrubbed with a dedicated maintenance command.

### 4.1 Kafka Poison Message Handling Is Implemented; Quarantine Ops Still Need Maturity

Priority: `P0`

Current state:
- main Kafka consumers now classify permanent vs transient failures,
- malformed payloads are published to `event.quarantine`,
- consumers stay alive on isolated poison messages,
- transient failures are retried with bounded backoff.

Why this matters:
- quarantine exists, but operator tooling around it is still thin,
- there is no dedicated triage view or replay workflow yet.

Why this matters for showcase quality:
- the core failure-isolation story is now present,
- but production-grade event operations still need visible recovery tooling.

Current scope:
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [cmd/rule-engine/main.go](../cmd/rule-engine/main.go)
- [cmd/analytics-writer/main.go](../cmd/analytics-writer/main.go)
- [cmd/realtime-gateway/main.go](../cmd/realtime-gateway/main.go)
- [internal/platform/kafkax/consumer.go](../internal/platform/kafkax/consumer.go)

Required fix:
- add operator-facing quarantine inspection and replay path,
- add metrics/alerts for quarantine volume,
- define retention and replay policy for `event.quarantine`.

### 4.2 Sensitive Data Protection Is Improved but Not Yet End-to-End

Priority: `P0`

Current state:
- new raw imports are encrypted in MongoDB,
- legacy plaintext raw imports can now be migrated and scrubbed with a dedicated maintenance command,
- key-aware encryption metadata exists through `content_kid`,
- legacy keys are supported for decryption,
- normalized transaction data in PostgreSQL, Mongo projections, and ClickHouse is still not field-encrypted,
- key management is still env-based and not KMS-backed.

Why this matters:
- financial statements and banking data are highly sensitive,
- protection should not stop at raw file bytes only,
- data classification by storage is still incomplete.

Why this matters for showcase quality:
- for a finance project, data protection is part of the core design, not an add-on.

Current scope:
- [cmd/ingest-service/main.go](../cmd/ingest-service/main.go)
- [cmd/parser-service/main.go](../cmd/parser-service/main.go)
- [internal/platform/cryptox/cryptox.go](../internal/platform/cryptox/cryptox.go)
- [internal/platform/cryptox/keyring.go](../internal/platform/cryptox/keyring.go)
- [internal/imports/models.go](../internal/imports/models.go)
- [cmd/sensitive-data-maintenance/main.go](../cmd/sensitive-data-maintenance/main.go)

Required fix:
- define which fields must be encrypted at application level,
- define key rotation policy,
- decide between field-level encryption and storage-level encryption per datastore.

### 4.3 Kafka Topic Provisioning Still Performs Admin Work in Runtime

Priority: `P0`

Current state:
- database schema creation was moved to migrations,
- but Kafka topic creation is still performed by service startup code.

Why this matters:
- services still need admin-like broker capabilities,
- startup path still contains infra provisioning concerns.

Why this matters for showcase quality:
- a polished backend should separate runtime from broker/bootstrap provisioning too.

Current scope:
- [internal/platform/kafkax/kafka.go](../internal/platform/kafkax/kafka.go)
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [cmd/analytics-writer/main.go](../cmd/analytics-writer/main.go)
- [cmd/parser-service/main.go](../cmd/parser-service/main.go)
- [cmd/rule-engine/main.go](../cmd/rule-engine/main.go)
- [cmd/realtime-gateway/main.go](../cmd/realtime-gateway/main.go)

Required fix:
- move Kafka topic provisioning to infra/bootstrap,
- remove broker admin operations from normal service startup.

### 4.4 Insecure Defaults Still Exist in the Runtime Path

Priority: `P0`

Current state:
- local/demo secrets and credentials still exist in standard runtime configuration,
- services are still willing to boot with development-grade values.

Why this matters:
- accidental production-like deployment with insecure settings is too easy,
- demo convenience leaks into the main runtime path.

Why this matters for showcase quality:
- strong engineers separate local convenience from deployable defaults.

Current scope:
- [cmd/api-gateway/main.go](../cmd/api-gateway/main.go)
- [cmd/auth-service/main.go](../cmd/auth-service/main.go)
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [env/](../env)
- [.env.example](../.env.example)

Required fix:
- add `APP_ENV`,
- fail fast outside `local` if security-critical env vars are missing or unsafe,
- move seeded users and demo credentials behind explicit dev-only flags.

## 5. P1: Major Architecture and Maintainability Debt

### Recently Resolved

- `2026-03-17`: versioned SQL migrations were added for PostgreSQL and ClickHouse, with a dedicated `migrate` command and Compose wiring.

### 5.1 Service Boundaries Are Still Too Thin; cmd/* Contains Too Much Logic

Priority: `P1`

Current state:
- many `cmd/*/main.go` files still contain transport logic plus orchestration logic,
- application service layer is not yet explicit.

Why this matters:
- testing is harder,
- transport concerns and business rules are mixed,
- codebase becomes harder to scale as features grow.

Why this matters for showcase quality:
- a project meant to demonstrate engineering depth should show clear layering, not only working code.

Current scope:
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [cmd/ingest-service/main.go](../cmd/ingest-service/main.go)
- [cmd/parser-service/main.go](../cmd/parser-service/main.go)
- [cmd/notification-service/main.go](../cmd/notification-service/main.go)

Required fix:
- introduce explicit application/use-case layer,
- leave `cmd/*` for wiring, handlers, startup only,
- move orchestration into dedicated services with interfaces.

### 5.2 Event Contracts Are Ad Hoc JSON, Not Versioned Contracts

Priority: `P1`

Current state:
- Kafka events are Go structs serialized as JSON,
- there is no formal event schema/versioning strategy.

Why this matters:
- hard to evolve contracts safely,
- consumers can silently drift,
- compatibility guarantees are weak.

Why this matters for showcase quality:
- event-driven systems are stronger when contracts are explicit and versioned.

Required fix:
- define event envelopes and versions,
- document schema per topic,
- optionally add protobuf/Avro/JSON schema discipline,
- add consumer compatibility tests.

### 5.3 gRPC Is Planned but Not Actually Used Yet

Priority: `P1`

Current state:
- original project target includes `gRPC`,
- current implementation is almost entirely REST + direct broker integration.

Why this matters:
- stack breadth is part of the showcase,
- one of the chosen technologies is not represented in the actual design yet.

Required fix:
- introduce internal gRPC contracts for one or two clear paths,
- good candidates:
  - `auth-service` internal identity verification,
  - `ledger-service` read API for internal consumers,
  - `notification-service` command/report fetches.

### 5.4 Categories Are Global, Not Tenant-Scoped

Priority: `P1`

Current state:
- derived categories go into a shared category table,
- user-specific taxonomy can leak into a global namespace.

Why this matters:
- cross-tenant metadata leak,
- polluted taxonomy,
- unclear ownership semantics.

Current scope:
- [internal/ledger/postgres.go](../internal/ledger/postgres.go)
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)

Required fix:
- split `system categories` and `user categories`,
- or add `user_id + scope`,
- adjust listing and derived-category creation rules.

### 5.5 Current Money Model Assumes Fixed 2-Decimal Minor Units

Priority: `P1`

Current state:
- money is stored as `amount_cents`,
- this is correct for many currencies,
- but currency scale is implicit and fixed to `2`.

Why this matters:
- not all currencies use the same scale,
- it leaks `USD/EUR-style` assumptions into a multi-currency system.

Why this matters for showcase quality:
- explicit monetary modeling shows attention to detail.

Required fix:
- evolve toward:
  - `amount_minor`,
  - `currency`,
  - `currency_scale`,
- expose human-readable decimal values at API layer without changing canonical storage semantics.

### 5.6 Legacy raw_line Debt Still Exists in Schema and History

Priority: `P1`

Current state:
- new parser output no longer keeps `raw_line`,
- but the field still exists in schema/history.

Why this matters:
- keeps unnecessary sensitive fragments,
- muddies the real privacy boundary.

Required fix:
- migration to null/drop historical `raw_line`,
- remove field from storage model where no longer needed.

### 5.7 Missing Operational Reprocess/Maintenance Flows

Priority: `P1`

Current state:
- reprocessing and recovery exist partially in code,
- but there is no clean operator/admin path for:
  - reparse,
  - republish,
  - replay projection,
  - reconcile failed imports.

Why this matters:
- production-like maintenance becomes manual,
- hard to demonstrate operational maturity.

Required fix:
- add explicit maintenance commands or admin endpoints,
- document safe replay and reprocess strategy.

## 6. P1: Product and Channel Debt

### 6.1 Telegram Auth Flow Is Functional but Not Safe Enough

Priority: `P1`

Current state:
- Telegram login currently uses `/login <username> <password>`.

Why this matters:
- password handling in chat is a weak UX and a weak security posture,
- acceptable only as a temporary technical bridge.

Required fix:
- replace with link-code or magic-link style binding,
- confirm ownership through the web/API side under JWT,
- store durable `chat_id -> user_id` binding after explicit confirmation.

### 6.2 Notification Delivery Still Needs Digest/Batching Strategy

Priority: `P1`

Current state:
- anti-spam tuning exists,
- but delivery still tends toward event-per-message rather than digest-per-context.

Why this matters:
- statement import can generate noisy alert behavior,
- Telegram rate limits are easier to hit,
- UX becomes fatiguing.

Required fix:
- group alerts by `user_id`, `import_id`, and time window,
- send digest summaries,
- separate critical immediate alerts from batchable alerts.

### 6.3 PDF Support Covers Text-Based Files Only

Priority: `P1`

Current state:
- text-based PDFs work,
- scanned PDFs and OCR-heavy statements are not supported.

Why this matters:
- real bank statements often include scans or difficult layouts,
- current parsing capability is good but not broad.

Required fix:
- add OCR path as an explicit later parser mode,
- keep it separate from the clean text-PDF parser.

## 7. P2: Performance and Scalability Debt

### 7.1 Upsert Path in ledger-service Is Not Batch-Optimized

Priority: `P2`

Current state:
- import path still performs per-transaction existence lookup and upsert,
- category dedupe logic is simple and not optimized.

Why this matters:
- slow on larger statements,
- uses DB roundtrips inefficiently,
- weakens the highload story.

Current scope:
- [internal/ledger/postgres.go](../internal/ledger/postgres.go)

Required fix:
- use batched writes,
- reduce roundtrips,
- use map-based category dedupe,
- add import benchmark.

### 7.2 No Load and Stress Validation Yet

Priority: `P2`

Current state:
- functional tests exist,
- but there is no meaningful load-test evidence for throughput or latency.

Why this matters:
- the project claims a broad backend/highload orientation,
- without measured results this remains mostly architectural.

Required fix:
- add `k6` or equivalent scenarios,
- measure:
  - import throughput,
  - Kafka consumer lag,
  - alert throughput,
  - websocket fan-out behavior.

## 8. P2: Observability and Operations Debt

### 8.1 Prometheus and Grafana Exist, but Observability Is Still Thin

Priority: `P2`

Current state:
- containers are present,
- but the project does not yet demonstrate a strong metrics taxonomy and ready dashboards.

Why this matters:
- observability is part of the stack you intentionally selected,
- merely running containers is not enough.

Required fix:
- expose richer service metrics,
- define dashboards for:
  - import latency,
  - parser failures,
  - Kafka lag,
  - Rabbit queue depth,
  - Telegram delivery results,
  - websocket connections.

### 8.2 No Distributed Tracing Yet

Priority: `P2`

Current state:
- logs exist,
- but there is no request/event trace across services.

Why this matters:
- tracing is one of the clearest ways to show maturity in a microservice/event-driven system.

Required fix:
- add trace IDs propagation,
- add OpenTelemetry,
- optionally add Jaeger/Tempo locally.

### 8.3 Missing Backup, Retention, and Data Lifecycle Policy

Priority: `P2`

Current state:
- no explicit retention/export/delete policy is modeled yet.

Why this matters:
- finance data systems need clear data lifecycle semantics,
- especially with sensitive documents and analytics copies.

Required fix:
- define retention by store,
- define delete/export path,
- define backup/restore expectations.

## 9. P2: Testing Debt

### 9.1 Missing Failure-Path Integration Tests

Priority: `P2`

Current state:
- happy-path integration exists,
- but several risky failure cases are not covered.

Missing tests:
- Kafka unavailable during ledger publish,
- poisoned Kafka message,
- Telegram rate-limit handling,
- replay/reprocess behavior,
- migration safety,
- category tenant isolation.

### 9.2 Missing Contract Tests

Priority: `P2`

Current state:
- OpenAPI exists,
- event contracts exist informally,
- but contract drift is not actively checked.

Required fix:
- validate OpenAPI against handlers,
- add event payload fixture tests,
- add consumer-producer compatibility tests.

## 10. P2: Showcase Gaps Relative to the Chosen Stack

These are not necessarily bugs, but they weaken the "broad-stack backend engineer" story.

### 10.1 TCP/UDP Are Not Represented Yet

Priority: `P2`

Current state:
- original target stack included `TCP/UDP`,
- current V1 implementation does not use them meaningfully.

Implication:
- if the project claims these technologies, they need a justified role later,
- otherwise they should stay out of the main architecture story.

### 10.2 OpenAPI Is Present but Needs Tight Synchronization

Priority: `P2`

Current state:
- OpenAPI baseline exists,
- but it still needs to stay in lockstep with handlers and auth requirements.

### 10.3 CI Is Useful but Not Yet Full Maturity CI

Priority: `P2`

Current state:
- baseline CI exists,
- but deeper quality gates are still missing.

Missing examples:
- integration stage with compose,
- migration checks,
- load-test smoke,
- security/config linting,
- contract drift validation.

## 11. P3: Design and Cleanup Debt

### 11.1 Repo Still Contains Temporary/Incidental Artifacts

Priority: `P3`

Examples:
- temporary utilities or local-only helper files should not remain in a polished showcase repo,
- generated binaries should not live in repo root.

Why this matters:
- cleanliness affects first impression.

### 11.2 Directory Map and Layering Documentation Can Be Stronger

Priority: `P3`

Current state:
- architecture docs exist,
- but a dedicated repo-map/layer-map doc would help explain the codebase quickly.

## 12. Recommended Execution Order

The recommended order is:

1. transactional outbox,
2. migration framework,
3. sensitive-data migration and data-protection policy,
4. poison-message strategy,
5. category tenancy fix,
6. Telegram secure binding flow,
7. alert digesting,
8. richer observability,
9. load testing,
10. gRPC introduction,
11. contract hardening,
12. optional TCP/UDP justification or removal from target story.

## 13. What Should Be Marketed as Already Strong

These parts are already good and should be presented confidently:
- real event-driven pipeline,
- mixed broker usage with clear roles,
- JWT + refresh + Redis session model,
- Telegram integration beyond simple outbound messages,
- raw import encryption at rest,
- realtime fan-out,
- ClickHouse analytical projection path,
- Docker-backed local environment,
- direct handling of security issues such as identity override and strict input contracts.

## 14. What Must Be Finished Before Calling the Project "Production-Grade"

The minimum set is:

1. transactional outbox,
2. schema migrations,
3. sensitive data migration and key policy,
4. poison-message handling,
5. safer Telegram auth binding,
6. observability beyond basic containers.
