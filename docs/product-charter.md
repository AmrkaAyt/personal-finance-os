# Personal Finance OS Product Charter

Version: 0.2.0  
Date: 2026-03-15  
Status: Strategic product authority

## 1. Purpose

This document defines what product `Personal Finance OS` is trying to become, what its core wedge is, what outcomes matter, and which capabilities are committed versus merely planned.

## 2. Product Wedge

`PFOS` is an operational personal finance cockpit.
Its primary wedge is:

`explainable obligation control and spending planning from imported financial data`

The product earns the right to expand only if it first helps a user reliably answer:
1. What changed?
2. What matters in the next `7-30` days?
3. What action should I take now?

## 3. Product Promise and Non-Promise

### 3.1 Product Promise

The product promise is not generic "financial clarity".
The operational promise is:

- imported money activity is visible in one place,
- upcoming obligations are surfaced before they become misses,
- overspending and anomalies are explainable,
- the user can take a concrete next action from each important insight,
- later planning and wealth features reuse the same trusted money model.

### 3.2 Product Non-Promise

The product is not:
- small-business bookkeeping,
- a broker-dealer,
- an auto-trading bot,
- a black-box investment adviser,
- a research/news terminal in early versions.

## 4. Personas

### 4.1 Owner

Primary person managing personal finances, obligations, and short- to medium-term planning.

### 4.2 Household Admin

User coordinating shared budgets, goals, or obligations after explicit sharing is enabled.

### 4.3 Advisor Read-Only Viewer

Trusted reviewer with scoped visibility and no write access to financial truth.

### 4.4 Power User

User who wants automation, Telegram workflows, exports, rule tuning, and scenario planning.

## 5. Product Principles

| Principle | Product meaning |
| --- | --- |
| `Operational before decorative` | Every major insight must map to at least one user action. |
| `Explainability before magic` | No alert, category, forecast, or recommendation is complete without a reasoning envelope. |
| `Safety before aggressiveness` | If balances, obligations, or reserve data are incomplete, the system withholds strong planning claims instead of guessing. |
| `Progressive sophistication` | V1 must be useful without goals, wealth, or research modules. |
| `Architecture is allowed, not primary` | Showcase-quality backend architecture is a valid project goal, but it cannot override the product wedge or introduce conflicting money logic. |

## 6. Module Status by Stage

| Module | Role in product | Status |
| --- | --- | --- |
| Core ingestion and parsing | bring external finance data into the system | `Committed` for V1 |
| Ledger and categories | canonical transaction truth | `Committed` for V1 |
| Recurring detection and obligation alerts | early operational value through inferred recurring charges and reminders | `Committed` for V1 |
| Budget baseline and overspend alerts | spending control | `Committed` for V1 |
| Realtime dashboard foundation | live visibility of core workflows | `Committed` for V1 |
| Goals and savings planning | medium-term planning | `Planned` for V2 |
| Cashflow forecast | short-term planning decisions | `Planned` for V2 |
| Household collaboration | shared workflows and scoped access | `Planned` for V2 |
| Telegram control plane | delegated operational interface | `Planned` for V2 |
| Calendar integration | external reminder execution | `Planned` for V2 |
| Wealth readiness | reserve and free-capital assessment | `Planned` for V3, gated by domain readiness |
| Broker aggregation | portfolio visibility | `Exploratory` for V3 |
| Research digest | external market/research enrichment | `Exploratory` for V3 |

## 7. Outcome Metrics

### 7.1 North-Star Metric

`Actioned financial signal rate`

Definition:
Percentage of monthly active users who complete at least one tracked action after a PFOS insight.

Tracked actions include:
- acknowledging or paying an obligation,
- adjusting a budget or category,
- confirming or rejecting a recurring pattern,
- resolving an anomaly,
- updating a goal or forecast assumption.

Guardrails:
- only meaningful signals count in the denominator,
- repeated acknowledgement of the same unresolved item does not count as a new action,
- suppressed noisy alerts do not improve the metric,
- the action must happen within the configured attribution window after the signal,
- action rate must be reviewed alongside signal precision and alert volume.

### 7.2 Supporting Metrics

- `first import completion rate`
- `categorized transaction coverage`
- `recurring obligation precision`
- `missed obligation rate`
- `alert action rate`
- `monthly active planners`
- `forecast usefulness score`
- `Telegram linked-user engagement rate`

## 8. Required User Actions After Insights

Every major insight type must support an action path.

| Insight | Required actions |
| --- | --- |
| Obligation reminder | acknowledge, mark paid, snooze, edit due date |
| Overspend alert | ignore, adjust budget, recategorize source transactions, create watch rule |
| Anomaly | mark legitimate, suppress similar future alerts, create merchant watch rule, recategorize |
| Recurring detection | confirm recurring, reject recurring, create reminder, assign owner |
| Goal gap | pause goal, move target date, change contribution pace, change priority |
| Forecast shortfall | add planned expense or income, update obligation timing, accept risk explicitly |

If an insight cannot trigger a user decision, it belongs in backlog or analytics, not in the core product loop.

## 8.1 V1 Obligation Control Semantics

Before planner-owned obligations exist, V1 obligation control means:
- detect candidate recurring charges from imported transaction history,
- allow the user to confirm or reject recurring candidates,
- emit alerts or reminders for confirmed or high-confidence recurring obligations,
- avoid claiming full planner-grade obligation lifecycle before V2.

V1 does not require a complete obligation scheduler to justify the wedge, but it must not pretend inferred recurring charges are already full obligation truth.

## 9. Roadmap Themes

### 9.1 V1 Foundation

Deliver reliable ingestion, ledger truth, recurring detection, alerts, Telegram outbound delivery, realtime updates, and analytical projections.

### 9.2 V2 Planning Layer

Add goals, forecast, digests, Telegram command workflows, household sharing, and calendar-linked operations on top of the canonical money model.

### 9.3 V3 Capital Layer

Add wealth readiness, net worth, broker sync, and limited research surfaces only after V2 planning semantics are stable and explainable.

## 10. Promotion Rules for New Scope

No capability may move into `Committed` status until it has all of the following:

- defined actor and user value,
- defined trigger and decision rule,
- canonical input data source,
- owner service or bounded context,
- lifecycle or state model,
- permission model,
- user action path,
- success metric,
- freshness or failure semantics where applicable.
