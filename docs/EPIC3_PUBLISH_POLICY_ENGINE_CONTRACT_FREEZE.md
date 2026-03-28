# Epic 3 — Publish Policy Engine: Contract Freeze (blockers/risks closure)

Статус: **Contract-freeze proposal** (без runtime-реализации Epic 3).

Основание: только repo evidence + зафиксированный REQ.

## 0) Scope и stop-boundary

- Этот документ фиксирует контракты и product blockers для дальнейшей декомпозиции E3-MF1..E3-MF6.
- Runtime owner не меняется: lifecycle owner у runtime остаётся `job` (существующий jobs state-machine).
- Metadata/planning owner остаётся `release`/`planned_releases`.
- Отдельная publish-entity **не вводится**.
- Epic 3 стартует после существующей стадии private upload (`UPLOADING` -> `WAIT_APPROVAL` + `youtube_uploads.privacy='private'`).

---

## 1) Raw repo evidence summary

### 1.1 Hard repo evidence

1. **Единый runtime state domain у jobs уже есть и используется как canonical API/UI filter-domain**.
   - `UI_JOB_STATES` содержит `WAIT_APPROVAL`, `APPROVED`, `PUBLISHED`, `CANCELLED`, `CLEANED` и failure states. Это текущая state-machine поверхность runtime publish-hand-off.  
   Source: `services/common/db.py`.

2. **Private upload → manual publish handoff уже является production-path**.
   - Uploader всегда пишет `youtube_uploads.privacy='private'` и переводит job в `WAIT_APPROVAL`.
   - Manual finalization делается оператором через `mark_published` (API или Telegram callback).  
   Sources: `services/workers/uploader.py`, `services/factory_api/app.py`, `services/bot/handlers.py`, `docs/LAUNCH_GUIDE.md`.

3. **Approval/publish boundary уже enforced state guards**.
   - `approve/reject` разрешены только из `WAIT_APPROVAL`.
   - `mark_published` разрешён из `APPROVED|WAIT_APPROVAL` и выполняет atomic update + committed playlist history write в транзакции.
   - `cancel` запрещён для terminal subset (`PUBLISHED|REJECTED|APPROVED|CANCELLED`) и иначе переводит в `CANCELLED`.  
   Source: `services/factory_api/app.py`.

4. **Recovery action semantics и audit trail уже стандартизованы**.
   - Recovery API: retry/reclaim/cleanup/cancel/restart.
   - Все write-actions требуют `{confirm: true, reason}`.
   - Audit хранится append-only JSONL (`<storage_root>/logs/recovery_audit.jsonl`) с `timestamp/actor/job_id/action/reason/result/details`.
   - Есть actionability flags и фильтры (`retryable/cancellable/reclaimable/cleanupable/restartable`).  
   Sources: `services/factory_api/app.py`, `docs/ops/runbook/recovery_console.md`, `tests/integration/test_recovery_console_api.py`.

5. **Idempotency/concurrency patterns в проекте уже есть (и повторяемы)**.
   - Claim pattern: `BEGIN IMMEDIATE` + update lock + conditional selection (`claim_job`).
   - Retry child dedupe: `jobs.retry_of_job_id UNIQUE` + fallback load existing row после `IntegrityError`.
   - Create-or-select pattern: transaction + recover-after-integrity-conflict (`PRJ_CONCURRENCY_CONFLICT`) в planner services.
   - Session freshness pattern: OPEN/EXECUTED/EXPIRED/INVALIDATED + TTL для mass-action preview/execute.
   - Many write paths используют condition guards `WHERE ... AND state != 'CANCELLED'`.  
   Sources: `services/common/db.py`, `services/ui_jobs/retry_service.py`, `services/planner/release_job_creation_service.py`, `services/planner/mass_actions_execute_service.py`.

6. **Error namespace convention уже префиксная и интерфейсная**.
   - UI jobs: `UIJ_*`, planner mass actions: `PMA_*`, planner release jobs: `PRJ_*`, materialization: `PRM_*`, ops restore: `OPS_*`, disk guard: `DISK_*`.
   - Ошибки обычно возвращаются как `{error: {code, message, ...}}` + HTTP status mapping.  
   Sources: `services/factory_api/app.py`, `services/planner/*.py`, `services/ops/backup_restore.py`, tests по API errors.

7. **SQLite schema/migration conventions стабилизированы**.
   - Central migration в `services/common/db.py` с additive `ALTER TABLE ... ADD COLUMN` и `CREATE INDEX IF NOT EXISTS`.
   - Индексная стратегия для jobs уже включает state/retry/lineage (`idx_jobs_state_retry`, `idx_jobs_retry_of_job_id`, `idx_jobs_root_job_id_attempt_no`).  
   Source: `services/common/db.py`.

8. **Release-job linkage уже canonicalized и защищён инвариантами**.
   - `releases.current_open_job_id` + invariant validation (`single open job`, pointer consistency).
   - Planner materialization связывает `planned_releases.materialized_release_id` и проверяет consistency.  
   Sources: `services/planner/release_job_creation_foundation.py`, `services/planner/materialization_foundation.py`.

### 1.2 Weak analogies (можно использовать только как pattern hint)

- Planner mass-action preview/execute sessions (subset checks + TTL + stale-session errors) — сильный structural аналог для будущих bulk publish actions, но домен другой (`planned_releases`, не runtime publish).
- Metadata preview/apply drift checks — аналог для external-state drift semantics, но сейчас drift там про metadata snapshot, не про YouTube publication state.

### 1.3 Gaps with no hard evidence

- Нет текущей persisted модели publish policy (item/channel/project уровни) в runtime DB.
- Нет endpoint/telegram contract для publish-policy resolution/inspect/apply.
- Нет явного external reconciliation job для YouTube publication drift (есть upload record и manual mark, но нет periodic reconcile contract).
- Нет полного epic-level error namespace именно для publish policy engine.
- Нет физического DB дизайна под policy scopes/overrides/audit по policy decisions.

---

## 2) Classification (items 1–10)

| item | status | conclusion | confidence | unresolved? |
|---|---|---|---|---|
| 1) audit/compliance status storage granularity | PARTIAL_ONLY | Можно закрыть granularity для runtime actions через existing recovery-style audit + job-level state timestamps; compliance taxonomy beyond this требует product decision. | High | yes |
| 2) exact publish policy data model + merge semantics | CLOSEABLE_FROM_REQ_PLUS_REPO | Приоритет `item > channel > project` уже задан REQ; можно заморозить deterministic merge contract на SQLite-таблицах без runtime owner change. | Medium | no |
| 3) exact API/Telegram wire contracts | CLOSEABLE_FROM_REQ_PLUS_REPO | Можно заморозить shape на основе existing API error/payload conventions и recovery confirm pattern + Telegram callback parity. | Medium | no |
| 4) action semantics (cancel/reset failure/unblock/bulk pause-hold) | PARTIAL_ONLY | `cancel` закрывается из repo; `reset failure`/`unblock`/`bulk pause-hold` частично закрываются как contract proposal, но часть правил переходов требует product decision per-state. | Medium | yes |
| 5) exact concurrency/idempotency mechanism | CLOSEABLE_FROM_REPO | Можно закрыть на canonical patterns: `BEGIN IMMEDIATE`, unique-key dedupe, conditional updates, session TTL/status guards. | High | no |
| 6) external-state reconciliation contract for drift detection | PARTIAL_ONLY | Можно закрыть интерфейс reconciliation-run/recording и mismatch taxonomy; schedule/authoritative source/auto-remediation policy требует product decision. | Medium | yes |
| 7) boundary approval flow vs Epic 3 operator actions | CLOSEABLE_FROM_REQ_PLUS_REPO | Граница закрывается: Epic 3 действует post-private-upload и не ломает существующие approve/reject/mark_published guards. | High | no |
| 8) reschedule rules for non-terminal states | PRODUCT_DECISION_REQUIRED | В repo есть retry/backoff для worker failures, но нет продуктовой матрицы reschedule для publish policy holds/unblocks across all non-terminal states. | Medium | yes |
| 9) full interface-level error namespace | PARTIAL_ONLY | Naming convention можно закрыть; полный исчерпывающий каталог кодов требует product acceptance of all operator scenarios. | Medium | yes |
| 10) physical DB/index design expectations | CLOSEABLE_FROM_REQ_PLUS_REPO | Можно заморозить SQLite-first additive schema/index expectations и migration conventions на epic уровне. | High | no |

---

## 3) Frozen contracts proposal

## A. Audit/compliance status scope proposal

**Invariant**
- Любое операторское publish-policy действие, меняющее runtime behavior, обязано оставлять append-only audit событие.

**Owner**
- Runtime owner: `job`.
- Audit writer: API layer (по аналогии recovery actions).

**Allowed values**
- `action`: `publish_policy_set`, `publish_policy_clear`, `publish_hold_set`, `publish_hold_clear`, `publish_cancel`, `publish_reset_failure`, `publish_unblock`, `publish_mark_published`, `publish_reconcile_run`.
- `result`: `ok|failed|rejected|noop`.

**Persistence rule**
- Phase 1 (freeze-compatible): JSONL в `<storage_root>/logs/publish_policy_audit.jsonl` по recovery pattern.
- Minimal record keys: `timestamp`, `actor`, `job_id`, `release_id`, `action`, `reason`, `result`, `details`.

**Guardrails**
- Нельзя silently mutate publish controls без audit row.
- `reason` mandatory для operator actions (reuse recovery confirm contract).

**Operator visibility requirement**
- Read endpoint с `limit<=200` (same clamp convention), latest-first.

**Testability requirement**
- Интеграционный test: action -> audit row appended with actor/result/details.

## B. Publish policy model + merge semantics

**Invariant**
- Resolution priority фиксирован: **item > channel > project** (REQ).

**Owner**
- Planning metadata owner: `release` scope (policy metadata).
- Runtime publish owner: `job` (effective resolved decision materialized on job snapshot at handoff).

**Allowed values (proposal)**
- `publish_mode`: `AUTO_PUBLISH` | `MANUAL_HANDOFF` | `HOLD`.
- `hold_reason_code`: nullable, controlled enum (см. Error namespace section).

**Merge rule**
- Compute effective policy deterministically:
  1) if item override exists -> use it
  2) else if channel override exists -> use it
  3) else project default
- Explicit `null` в override трактуется как “unset at this scope”, не как value.

**Persistence rule (SQLite)**
- `publish_policy_project_defaults(project_key, publish_mode, updated_at, updated_by)`
- `publish_policy_channel_overrides(channel_slug, publish_mode, updated_at, updated_by)`
- `publish_policy_item_overrides(release_id, publish_mode, reason, updated_at, updated_by)`
- Effective snapshot на runtime-job creation/handoff: `jobs.publish_mode_effective` (или отдельный immutable snapshot table keyed by job_id).

**Concurrency rule**
- Update policy rows via `BEGIN IMMEDIATE`.
- Idempotent upsert by natural keys (`project_key`, `channel_slug`, `release_id`).

**Error behavior**
- Invalid scope key -> `E3_POLICY_SCOPE_INVALID`.
- Unknown publish_mode -> `E3_POLICY_MODE_INVALID`.

**Guardrails**
- Не вводить independent publish lifecycle entity.
- Не менять existing uploader private behavior.

**Operator visibility requirement**
- API возврат должен включать `resolved_from_scope` (`ITEM|CHANNEL|PROJECT`) + `effective_publish_mode`.

**Testability requirement**
- Table-driven merge tests: all 3 levels + null-unset behavior + deterministic precedence.

## C. API surface proposal

Ниже contract freeze shape (proposal), runtime implementation deferred.

1. `GET /v1/publish-policy/resolve?job_id=<id>`
   - 200: `{ job_id, release_id, effective_publish_mode, resolved_from_scope, holds: {...}, version }`
   - 404: `E3_JOB_NOT_FOUND`

2. `PUT /v1/publish-policy/project-default`
   - body: `{ project_key, publish_mode, reason }`
   - 200: `{ ok: true, scope: "PROJECT", ... }`

3. `PUT /v1/publish-policy/channel/{channel_slug}`
   - body: `{ publish_mode, reason }`

4. `PUT /v1/publish-policy/item/{release_id}`
   - body: `{ publish_mode, reason }`

5. Runtime actions (post-private-upload surface):
   - `POST /v1/publish/jobs/{job_id}/cancel`
   - `POST /v1/publish/jobs/{job_id}/reset-failure`
   - `POST /v1/publish/jobs/{job_id}/unblock`
   - `POST /v1/publish/jobs/bulk-hold`
   - payload baseline for all mutating endpoints: `{ confirm: true, reason: "...", request_id: "..." }`

**Request/response contract invariant**
- Error envelope reused: `{ "error": { "code": "...", "message": "...", ... } }`.
- Success envelope reused: `{ "ok": true, ... }`.

## D. Telegram surface parity proposal

**Invariant**
- Telegram = полноценный remote control surface, но с same guards as API.

**Contract**
- Каждому mutating API action соответствует Telegram callback command c identical semantic result:
  - `publish_cancel:{job_id}` -> `/v1/publish/jobs/{job_id}/cancel`
  - `publish_reset_failure:{job_id}` -> `/v1/publish/jobs/{job_id}/reset-failure`
  - `publish_unblock:{job_id}` -> `/v1/publish/jobs/{job_id}/unblock`
  - `publish_hold_bulk:{scope_or_filter}` -> `/v1/publish/jobs/bulk-hold`

**Guardrails**
- Telegram handler не пишет state напрямую, а делегирует API/service layer (чтобы сохранить parity и единый audit/error behavior).
- Confirm/reason обязательны (как recovery pattern).

**Operator visibility requirement**
- Telegram reply must include: action, target, result, error_code(if any), correlation/request_id.

## E. Action semantics proposal

### E1) `cancel`

**Repo-anchored closure**: closeable.

- Allowed when job state not in terminal forbidden subset (`PUBLISHED|REJECTED|APPROVED|CANCELLED`), preserving existing behavior.
- Effect: state->`CANCELLED`, stage->`CANCELLED`, clear lock/retry, set reason.
- Idempotency: repeated call after cancel returns `noop` or 409-consistent terminal response (implementation choice, product-neutral).

### E2) `reset failure state`

**Partial closure**.

- Canonical intent: from failure states (`FAILED|RENDER_FAILED|QA_FAILED|UPLOAD_FAILED`) вернуть в actionable state respecting existing pipeline stage ownership.
- Contract-safe default from repo patterns: perform as controlled retry-child create/select (preferred) instead of mutating failed row in place.
- Unresolved: exact target state mapping per failure type и whether same job row vs child job across non-UI job types.

### E3) `unblock`

**Partial closure**.

- Canonical intent: снять operator hold/manual block, не bypassing runtime guards.
- Contract-safe behavior: allowed only if current blocker type is policy hold (not system failures like missing mp4).
- Unresolved: полный blocker taxonomy и precedence с disk/system guards.

### E4) `bulk pause/hold`

**Partial closure**.

- Use planner mass-action session pattern:
  - preview session (selection validation + TTL)
  - execute session (subset validation + stale session checks)
  - aggregate/item-level outcomes + reason codes
- Unresolved: canonical filter scopes (job_ids only vs channel/state filters) as product choice.

## F. Concurrency + idempotency proposal

**Canonical candidate selected**: existing DB transaction + uniqueness + conditional update pattern.

**Why canonical**
- Уже применяется в jobs claim/retry/planner flows.
- SQLite-compatible, deterministic, low-risk для текущего repo.

**Rules**
1. Mutating publish actions: `BEGIN IMMEDIATE`.
2. Idempotency key (`request_id`) persisted in action log table with unique constraint (`action_type`, `request_id`).
3. Duplicate request returns first result (`ok/noop/failed`) without re-execution.
4. State transition updates use guarded `WHERE` clauses against illegal transitions.
5. Bulk execute uses preview-session status (`OPEN` only) + expiry guard.

**Rejected alternatives**
- Distributed locks / external idempotency store: no repo evidence, unnecessary for SQLite single-DB pattern.
- Optimistic row-version across whole jobs table: no current row_version pattern in runtime core.

## G. Drift detection / reconciliation proposal

**Invariant**
- External publication drift must be detectable as read-only reconciliation pass before any auto-remediation.

**Contract**
- `POST /v1/publish/reconcile/run` (manual trigger)
- `GET /v1/publish/reconcile/runs/{id}`

**Run result schema**
- summary counters: `checked`, `matched`, `drifted`, `not_found`, `errors`
- per item: `{job_id, youtube_video_id, expected_state, observed_state, drift_code, details}`

**Persistence rule**
- Reconciliation run + items stored in SQLite tables (append-only history).

**Guardrails**
- No automatic state mutation in v1 reconciliation contract.
- Any remedial action remains operator-confirmed action with audit.

**Unresolved**
- authoritative source fields and polling cadence (product decision).

## H. Approval-flow boundary proposal

**Invariant**
- Existing approval flow remains source-of-truth for manual publication handoff.

**Boundary**
- Epic 3 operator actions are permitted only after private upload stage (post `UPLOADING` success path).
- `approve/reject/mark_published` existing endpoints and guards remain unchanged as baseline behavior.
- Publish policy engine may gate/annotate these actions but not bypass state guards.

## I. Reschedule semantics proposal

**Partial closure**

- Reuse existing retry/backoff primitives (`attempt`, `retry_at`, `schedule_retry`, max attempts).
- Reschedule decision must preserve per-stage ownership (render/qa/upload workers).

**Still product-blocked**
- Полная матрица non-terminal states -> allowed reschedule target/action (особенно для HOLD/UNBLOCK interplay).

## J. Error namespace proposal

**Invariant**
- Prefix-based namespace, machine-readable stable codes.

**Prefix frozen for Epic 3**
- `E3_*` for publish policy engine interface-level errors.

**Initial catalog (freeze)**
- `E3_POLICY_SCOPE_INVALID`
- `E3_POLICY_MODE_INVALID`
- `E3_POLICY_RESOLUTION_FAILED`
- `E3_ACTION_NOT_ALLOWED`
- `E3_ACTION_CONFIRMATION_REQUIRED`
- `E3_ACTION_IDEMPOTENCY_CONFLICT`
- `E3_ACTION_CONCURRENCY_CONFLICT`
- `E3_BULK_SESSION_NOT_FOUND`
- `E3_BULK_SESSION_EXPIRED`
- `E3_BULK_SCOPE_MISMATCH`
- `E3_RECONCILE_SOURCE_UNAVAILABLE`
- `E3_RECONCILE_DRIFT_DETECTED`

**HTTP mapping baseline**
- 400 invalid payload
- 404 target not found
- 409 transition/guard conflict
- 422 semantic validation/scope mismatch
- 500 internal
- 503 temporary external/source unavailable

## K. DB/schema/index expectations proposal (epic level)

**SQLite-only invariant**
- Любые новые таблицы/индексы совместимы с SQLite; migration style additive + `IF NOT EXISTS`.

**Proposed tables (contract level, no runtime implementation yet)**
1. `publish_policy_project_defaults`
2. `publish_policy_channel_overrides`
3. `publish_policy_item_overrides`
4. `publish_policy_action_log` (idempotency + audit mirror)
5. `publish_reconcile_runs`
6. `publish_reconcile_items`

**Index expectations**
- Fast resolve path: channel/release scoped unique indexes.
- Fast queue views: `(job_id, created_at)` for action log; `(run_id, drift_code)` for reconcile items.
- Idempotency: unique `(action_type, request_id)`.

**Migration expectations**
- No destructive migration in Epic 3 freeze phase.
- Backfill optional; must keep current pipeline behavior intact when tables empty.

---

## 4) Explicit unresolved product blockers

1. **Failure reset target semantics across all non-terminal states**
   - Missing decision: in-place state rewind vs mandatory child-job retry per job_type.
   - Options:
     - A) Always spawn retry child (repo-aligned strongest).
     - B) Per-state in-place rewind matrix.
   - Recommended default: A (directly follows existing retry/idempotent patterns).

2. **Unblock semantics taxonomy**
   - Missing decision: какие blocker classes считаются operator-unblockable.
   - Options:
     - A) Только policy holds.
     - B) Policy holds + selected system guards.
   - Recommended default: A (минимальный безопасный scope, не bypass runtime guards).

3. **Bulk hold scope definition**
   - Missing decision: targeting only explicit job_ids vs dynamic filters (`channel/state/...`).
   - Options: explicit IDs only (deterministic) / filter-based (powerful but higher drift risk).
   - Recommended default: explicit IDs only для первой итерации.

4. **Reconciliation authority and cadence**
   - Missing decision: authoritative external fields, poll cadence, SLA for drift handling.
   - Options: manual-run only / scheduled periodic + manual.
   - Recommended default: manual-run only v1 (repo has no current periodic external reconcile contract).

5. **Compliance grade beyond operator audit trail**
   - Missing decision: нужен ли отдельный compliance status lifecycle (e.g., attest/waiver/escalated) и retention policy.
   - Options: operator audit only / extended compliance lifecycle.
   - Recommended default: operator audit only in E3-MF1.

---

## 5) Appendix matrices

### 5.1 State/action permission matrix (proposal)

| state | cancel | reset_failure | unblock | bulk_hold_apply | mark_published |
|---|---:|---:|---:|---:|---:|
| FAILED/RENDER_FAILED/QA_FAILED/UPLOAD_FAILED | yes | yes | conditional | yes | no |
| WAIT_APPROVAL | yes | no | conditional | yes | yes (existing) |
| APPROVED | no (existing guard) | no | conditional | yes | yes (existing) |
| PUBLISHED | no | no | no | no | noop |
| CANCELLED | no | no | no | no | no |
| in-progress non-terminal | yes (existing guard set) | no | conditional | yes | no |

`conditional` = requires blocker type = policy-hold and confirm+reason.

### 5.2 Action semantics matrix (proposal)

| action | confirm required | reason required | idempotent | audit required | side effects |
|---|---:|---:|---:|---:|---|
| cancel | yes | yes | yes | yes | state->CANCELLED, clear lock/retry |
| reset_failure | yes | yes | yes | yes | create/select retry child (preferred) |
| unblock | yes | yes | yes | yes | remove policy hold flags only |
| bulk_hold | yes | yes | yes(session+request_id) | yes | set hold policy on selected scope |

### 5.3 Idempotency/concurrency matrix

| surface | mechanism | conflict signal |
|---|---|---|
| single action API | `BEGIN IMMEDIATE` + unique `(action_type, request_id)` | `E3_ACTION_IDEMPOTENCY_CONFLICT` or replay first result |
| retry-like create/select | unique lineage key (analogy: `retry_of_job_id UNIQUE`) + `IntegrityError` recovery | `E3_ACTION_CONCURRENCY_CONFLICT` |
| bulk execute | session status guard (`OPEN`) + TTL + subset validation | `E3_BULK_SESSION_EXPIRED` / `E3_BULK_SCOPE_MISMATCH` |

### 5.4 API/Telegram parity matrix

| capability | API | Telegram | parity rule |
|---|---|---|---|
| cancel | `/v1/publish/jobs/{id}/cancel` | callback `publish_cancel:{id}` | same guard + same error code |
| reset failure | `/v1/publish/jobs/{id}/reset-failure` | callback `publish_reset_failure:{id}` | same transition semantics |
| unblock | `/v1/publish/jobs/{id}/unblock` | callback `publish_unblock:{id}` | same blocker checks |
| bulk hold | `/v1/publish/jobs/bulk-hold` | command/callback bulk action | same preview/execute session semantics |

### 5.5 Unresolved product decisions table

| blocker | missing decision | options |
|---|---|---|
| failure reset mapping | in-place vs child-job | A child-job / B in-place matrix |
| unblock scope | which blockers are unblockable | A policy-only / B +system subset |
| bulk hold targeting | explicit ids vs dynamic filters | A ids-only / B filters |
| reconciliation cadence | manual vs scheduled | A manual / B mixed |
| compliance lifecycle | audit-only vs lifecycle entity | A audit-only / B extended compliance states |

---

## 6) Suggested follow-up split into microfeatures (E3-MF1..E3-MF6)

- **E3-MF1 — Policy Data Model + Resolution Read API**
  - Deliverables: policy tables, resolve endpoint, merge precedence tests.

- **E3-MF2 — Action Contracts (single-item) + Audit log**
  - Deliverables: cancel/reset_failure/unblock endpoints (contract-complete), audit append/read endpoints, error codes.

- **E3-MF3 — Bulk Hold Preview/Execute Session**
  - Deliverables: preview+execute session tables/API, subset/TTL/concurrency guards, aggregate/item result model.

- **E3-MF4 — Telegram Parity Surface**
  - Deliverables: Telegram handlers delegating to API/service layer, parity tests for success/error mapping.

- **E3-MF5 — Reconciliation v1 (manual run, read-only drift detection)**
  - Deliverables: reconcile run/items persistence, run/status APIs, drift code catalog, no auto-remediation.

- **E3-MF6 — Approval boundary + reschedule matrix hardening**
  - Deliverables: explicit transition matrix finalization, unresolved product decisions closure, integration tests across non-terminal states.

