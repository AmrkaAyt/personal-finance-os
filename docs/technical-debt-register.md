# Personal Finance OS: Technical Debt Register

Version: 0.1.0  
Date: 2026-04-28
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
- `2026-03-18`: Kafka topic provisioning was moved out of service startup into one-shot `kafka-bootstrap`.
- `2026-03-18`: non-local fail-fast checks were added for security-critical defaults such as `JWT_SECRET`, seeded demo users, encryption keys, and insecure DB DSNs.
- `2026-03-18`: categories were split into system-scoped and tenant-scoped user categories.
- `2026-03-18`: Telegram password-in-chat login was replaced with one-time link-code binding confirmed under JWT.
- `2026-03-19`: a stable mixed `k6` load-test baseline was captured after fixing gateway reverse-proxy connection churn.
- `2026-04-28`: `api-gateway` now serves a first browser cockpit for login, import, ledger, analytics, notification preferences, Telegram code confirmation, and realtime updates.

### 4.1 Kafka Poison Message Handling Is Implemented; Quarantine Ops Still Need Maturity

Priority: `P0`

Current state:
- main Kafka consumers now classify permanent vs transient failures,
- malformed payloads are published to `event.quarantine`,
- new quarantine events include original payload for replay tooling,
- consumers stay alive on isolated poison messages,
- transient failures are retried with bounded backoff,
- one-shot `quarantine-operator` can summarize, list, and dry-run replay matching events.

Why this matters:
- quarantine exists and basic operator tooling is present,
- but there is still no approval/commit workflow, triage UI, or replay audit trail.

Why this matters for showcase quality:
- the core failure-isolation story is now present,
- but production-grade event operations still need visible recovery tooling.

Current scope:
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [cmd/rule-engine/main.go](../cmd/rule-engine/main.go)
- [cmd/analytics-writer/main.go](../cmd/analytics-writer/main.go)
- [cmd/realtime-gateway/main.go](../cmd/realtime-gateway/main.go)
- [cmd/quarantine-operator/main.go](../cmd/quarantine-operator/main.go)
- [internal/platform/kafkax/consumer.go](../internal/platform/kafkax/consumer.go)

Required fix:
- keep the current operator path and add replay approval/commit workflow,
- add metrics/alerts for quarantine volume,
- define retention and replay policy for `event.quarantine`.

### 4.2 Sensitive Data Protection Is Improved but Not Yet End-to-End

Priority: `P0`

Current state:
- new raw imports are encrypted in MongoDB,
- legacy plaintext raw imports can now be migrated and scrubbed with a dedicated maintenance command,
- key-aware encryption metadata exists through `content_kid`,
- legacy keys are supported for decryption,
- key material can now be resolved through explicit secret refs (`env:` / `file:`),
- non-local policy can reject env-backed key refs,
- normalized transaction data in PostgreSQL, Mongo projections, and ClickHouse is still not field-encrypted,
- key management is still not KMS-backed.

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
- decide between field-level encryption and storage-level encryption per datastore,
- move from secret refs to managed secret ownership / KMS integration for non-local environments.

### 4.3 Kafka Bootstrap Is Separated; Infra Ownership Is Still Basic

Priority: `P0`

Current state:
- Kafka topic creation is no longer performed by normal runtime services,
- provisioning is now handled by one-shot `kafka-bootstrap`,
- Compose wiring ensures Kafka-dependent services wait for bootstrap completion.

Why this matters:
- runtime services no longer need broker admin calls,
- but Kafka bootstrap still lives as application code rather than external IaC/Terraform/Helm ownership.

Why this matters for showcase quality:
- the runtime/admin boundary is now explicit,
- the remaining gap is infra maturity, not service correctness.

Current scope:
- [internal/platform/kafkax/kafka.go](../internal/platform/kafkax/kafka.go)
- [cmd/kafka-bootstrap/main.go](../cmd/kafka-bootstrap/main.go)
- [deploy/docker-compose.yml](../deploy/docker-compose.yml)

Required fix:
- optionally move Kafka bootstrap ownership to infra tooling,
- define per-topic partition/replication policy per environment.

### 4.4 Non-Local Fail-Fast Exists; Coverage Still Needs Expansion

Priority: `P0`

Current state:
- security-critical services now fail fast outside `local/dev/test` when:
  - `JWT_SECRET` is left as `dev-secret`,
  - seeded demo users remain enabled,
  - encryption key placeholders are still present,
  - PostgreSQL / ClickHouse DSNs still use obvious insecure defaults.

Why this matters:
- accidental non-local boot with demo-grade secrets is now blocked,
- but policy coverage is still not fully centralized across every service and deployment target.

Why this matters for showcase quality:
- the safety baseline is now present in code,
- the next step is broader policy depth and explicit deployment profiles.

Current scope:
- [cmd/api-gateway/main.go](../cmd/api-gateway/main.go)
- [cmd/auth-service/main.go](../cmd/auth-service/main.go)
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [cmd/ingest-service/main.go](../cmd/ingest-service/main.go)
- [cmd/parser-service/main.go](../cmd/parser-service/main.go)
- [cmd/analytics-writer/main.go](../cmd/analytics-writer/main.go)
- [cmd/migrate/main.go](../cmd/migrate/main.go)
- [cmd/sensitive-data-maintenance/main.go](../cmd/sensitive-data-maintenance/main.go)
- [internal/platform/secureenv/secureenv.go](../internal/platform/secureenv/secureenv.go)
- [env/](../env)
- [.env.example](../.env.example)

Required fix:
- extend checks to every relevant entrypoint,
- add explicit staging/production profiles in deployment artifacts,
- move seeded users and demo-only routes deeper behind dev-only toggles.

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

### 5.2 Event Contract Baseline Exists; Schema Evolution Still Needs Maturity

Priority: `P1`

Current state:
- Kafka events are still JSON objects for V1 compatibility,
- an explicit contract registry now exists in [internal/eventcontracts](../internal/eventcontracts),
- producers validate required fields before publishing direct Kafka events,
- published direct events include `x-event-type` and `x-event-version` headers,
- contract fixture tests cover the current V1 event payloads.

Why this matters:
- compatibility is now more visible,
- but schema evolution policy and consumer-driven compatibility checks are still basic.

Why this matters for showcase quality:
- event-driven systems are stronger when contracts are explicit and versioned.

Required fix:
- add consumer-driven compatibility tests,
- define version bump policy with examples,
- optionally add protobuf/Avro/JSON schema discipline,
- validate OpenAPI and event contract drift in CI.

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

### 5.4 Tenant-Scoped Categories Are Implemented; Taxonomy Governance Still Needs Maturity

Priority: `P1`

Current state:
- categories now support:
  - system scope,
  - tenant-owned scope,
- derived categories are created per user and are no longer globally leaked.

Why this matters:
- isolation at the metadata layer is now explicit,
- but taxonomy governance is still basic.

Current scope:
- [internal/ledger/postgres.go](../internal/ledger/postgres.go)
- [cmd/ledger-service/main.go](../cmd/ledger-service/main.go)
- [migrations/postgres/000004_categories_tenant_scope.sql](../migrations/postgres/000004_categories_tenant_scope.sql)

Required fix:
- add admin/operator rules for category normalization and merge,
- decide whether user categories should remain free-form or move toward controlled mapping.

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

### 6.0 Browser Cockpit Exists; Insight Actions Are Still Thin

Priority: `P1`

Current state:
- `api-gateway` serves an embedded web cockpit at `/app/`,
- the cockpit can authenticate, import statements, show ledger/analytics/signals, add manual transactions, manage notification preferences, confirm Telegram link codes, and connect to realtime channels,
- there is still no full action lifecycle for insights.

Why this matters:
- the project is no longer backend-only,
- but the product promise requires every important signal to map to a concrete user action.

Required fix:
- add actions for alert acknowledgement, snooze, resolve, and suppress similar future alerts,
- add recurring confirmation/rejection lifecycle,
- add recategorization flow tied to transactions and future classifier rules,
- add a compact first-run/import readiness state for real personal use.

### 6.1 Telegram Link Flow Is Safer, but Still API-Centric

Priority: `P1`

Current state:
- password-in-chat login is gone,
- bot now issues a one-time code,
- binding is confirmed only through a JWT-authenticated API call,
- stored `chat_id -> user_id` bindings are durable.

Why this matters:
- this is materially safer than sending credentials in chat,
- but the UX still assumes direct API access rather than a richer browser/device-link flow.

Required fix:
- add a proper web/device-link confirmation page,
- let users inspect and revoke bound chats from the main product UI.

### 6.2 Notification Delivery Has Digest Batching, but Policy Is Still Basic

Priority: `P1`

Current state:
- anti-spam tuning exists,
- non-critical Telegram alerts are already batched into digest notifications,
- user-level notification preferences and quiet windows now exist,
- but batching policy is still heuristic rather than a full notification rules engine.

Why this matters:
- statement import can generate noisy alert behavior,
- Telegram rate limits are easier to hit,
- UX becomes fatiguing.

Required fix:
- keep the current `critical now / warning digest` split,
- add richer user-level rate policies,
- add per-category quiet windows and rate policies,
- add multi-channel preferences beyond Telegram,
- add operator visibility into dropped, merged, and delayed alerts.

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

### 7.2 Load Baseline Exists; Stepped Stress and Soak Validation Still Need Work

Priority: `P2`

Current state:
- functional tests exist,
- `k6` scenarios now exist for auth, writes, reads, imports, websocket churn, and mixed traffic,
- a documented mixed-traffic baseline exists in [docs/load-test-baseline.md](load-test-baseline.md),
- the captured baseline passed with zero HTTP failures under the default local mixed scenario,
- there are still no stepped `100/150/200 VU` baselines or long-soak results.

Why this matters:
- the project claims a broad backend/highload orientation,
- the first measured baseline exists,
- the remaining gap is proving behavior under higher concurrency and longer-running load.

Required fix:
- capture stepped mixed-load baselines,
- add long-soak runs,
- measure import throughput, Kafka lag, alert throughput, and websocket fan-out behavior,
- keep exported summaries and Grafana/Prometheus evidence with the docs.

## 8. P2: Observability and Operations Debt

### 8.1 Prometheus and Grafana Exist, but Observability Is Still Thin

Priority: `P2`

Current state:
- containers are present,
- Prometheus scraping works,
- Grafana dashboard provisioning works,
- but the metrics taxonomy is still thin and the dashboards are still only baseline-level.

Why this matters:
- observability is part of the stack you intentionally selected,
- merely running containers is not enough.

Required fix:
- expose richer service metrics,
- expand dashboards for:
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
- event contract registry and fixture tests exist,
- but consumer-producer compatibility and OpenAPI handler drift are not fully checked.

Required fix:
- validate OpenAPI against handlers,
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

The recommended order from the current state is:

1. insight action lifecycle in the browser cockpit,
2. consumer-driven event compatibility tests,
3. quarantine retention and replay audit reporting,
4. richer observability and tracing,
5. stepped load baselines and soak runs,
6. alert delivery policy hardening beyond Telegram-only rules,
7. ledger import batch optimization,
8. gRPC introduction for a narrow internal path,
9. Telegram device-link UX,
10. OCR parser mode for scanned PDFs,
11. optional TCP/UDP justification or removal from target story.

## 13. What Should Be Marketed as Already Strong

These parts are already good and should be presented confidently:
- real event-driven pipeline,
- mixed broker usage with clear roles,
- JWT + refresh + Redis session model,
- Telegram integration beyond simple outbound messages,
- browser cockpit over the live gateway APIs,
- raw import encryption at rest,
- realtime fan-out,
- ClickHouse analytical projection path,
- Docker-backed local environment,
- direct handling of security issues such as identity override and strict input contracts.

## 14. What Must Be Finished Before Calling the Project "Production-Grade"

The remaining minimum set is:

1. formal event contracts and compatibility checks,
2. auditable quarantine replay operations,
3. stronger secret/key ownership beyond local secret refs,
4. observability beyond basic containers,
5. backup, retention, export, and delete policy,
6. broader failure-path integration coverage.
