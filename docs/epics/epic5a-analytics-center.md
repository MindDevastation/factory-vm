# Epic 5A — Analytics Center (Implemented)

## 1. Purpose
Epic 5A provides an analytics control plane composed of:
- canonical snapshot persistence,
- external ingestion (YouTube-focused in current code),
- operational KPI derivation,
- MF4 comparisons/predictions with explicit explainability payloads,
- MF5 recommendation synthesis + lifecycle,
- MF6 page/reporting APIs.

## 2. Scope
Implemented scope includes:
- Snapshot write/read/link services with current/supersession model.
- External sync run tracking, status, coverage, and manual refresh entrypoint.
- KPI computation runtime + events.
- MF4 baseline/comparison/prediction derivation + persistence/events.
- Recommendation synthesis/persistence/runtime lifecycle updates.
- Analytics page contracts, navigation spine, filter normalization, report generation/download records.

## 3. Non-goals (frozen boundaries)
- No black-box prediction engine; derivation outputs include basis/signals/explainability fields.
- No auto-execution or auto-apply into adjacent domains.
- No adjacent-domain mutation hidden in analytics pages/actions.
- No Epic-6 redesign here; MF6 is contract/data API surface.
- Advisory analytics behavior first; operational actions remain lightweight/delegated.

## 4. Architecture / components
Primary modules:
- Foundation: `write_service.py`, `read_service.py`, `helpers.py`.
- External ingestion/status: `external_sync.py`.
- MF3 operational KPIs: `operational_kpi.py`, `operational_kpi_runtime.py`.
- MF4: `mf4_derivation_core.py`, `mf4_runtime.py`.
- MF5: `recommendation_core.py`, `recommendation_runtime.py`.
- MF6: `ui_contracts.py`, `page_aggregations.py`, `reporting.py`.
- API surface in `services/factory_api/app.py` under `/v1/analytics/...`.

## 5. Data model / persistence
Canonical tables include:
- Foundation:
  - `analytics_snapshots` (with `is_current`, `snapshot_status`, `freshness_status`, `lineage_json`, `explainability_json`, `comparison_baseline_snapshot_id`)
  - `analytics_rollup_links`
  - `analytics_events`
  - `analytics_scope_links`, `analytics_external_identities`
- External sync:
  - `analytics_external_sync_runs`
  - `analytics_external_scope_status`
  - `analytics_external_audit_events`
  - `analytics_youtube_video_links`
- MF3:
  - `analytics_operational_kpi_runs`
  - `analytics_operational_kpi_snapshots`
  - `analytics_operational_kpi_events`
- MF4:
  - `analytics_prediction_runs`
  - `analytics_baseline_snapshots`
  - `analytics_comparison_snapshots`
  - `analytics_prediction_snapshots`
  - `analytics_prediction_events`
- MF5:
  - `analytics_recommendation_runs`
  - `analytics_recommendation_snapshots`
  - `analytics_recommendation_events`
- MF6/reporting:
  - `analytics_report_records`
  - `analytics_ui_events`

Current/supersession behavior:
- `is_current=1` uniqueness is enforced by normalized scope key for analytics snapshots.
- Writing a new current snapshot supersedes prior current record(s) transactionally and emits supersession events.

## 6. API / UI surface
Implemented MF6/page APIs:
- `GET /v1/analytics/filter-contract`
- `GET /v1/analytics/channels`, `/releases`, `/batches` (indexes)
- `GET /v1/analytics/overview`
- `GET /v1/analytics/channels/{channel_slug}`
- `GET /v1/analytics/releases/{release_id}`
- `GET /v1/analytics/batches/{batch_month}`
- `GET /v1/analytics/anomalies`
- `GET /v1/analytics/recommendations`
- `GET /v1/analytics/reports`

Reporting and export:
- `POST /v1/analytics/reports/request`
- `GET /v1/analytics/reports/records`
- `GET /v1/analytics/reports/{report_record_id}/download`

External ingestion/status:
- `POST /v1/analytics/external/manual-refresh`
- `GET /v1/analytics/external/status`
- `GET /v1/analytics/external/coverage`
- `GET /v1/analytics/external/runs`
- `GET /v1/analytics/external/runs/{run_id}`

Lightweight actions (non-mutating in adjacent domains):
- `POST /v1/analytics/actions/refresh`
- `POST /v1/analytics/actions/recompute`
- `GET /v1/analytics/actions/related-domain-jump`
- `POST /v1/analytics/actions/recommendations/{recommendation_id}/acknowledge`
- `POST /v1/analytics/actions/anomaly/inspect`

Unsupported filter rejection:
- `source_family` is explicitly validated by scope and rejected with `E5A_INVALID_ANALYTICS_FILTER_COMBINATION` where unsupported (e.g., release/report contexts in current config).

## 7. Runtime behavior / workflow
Typical pipeline:
1. Snapshots are written from internal/external sources with scope identity normalization.
2. Current snapshots supersede prior currents where applicable.
3. External sync runs update run history + scope status/coverage.
4. MF3 derives KPI snapshots/events.
5. MF4 derives baselines/comparisons/predictions with explicit explainability and source refs.
6. MF5 synthesizes recommendations and manages lifecycle states.
7. MF6 aggregates page contracts and supports report export records/artifacts.

MF2 survivability behavior:
- External failures produce degraded/partial/failed sync states, but analytics foundation remains queryable (internal snapshots and KPI/prediction/recommendation layers are persisted separately).

## 8. Safety rules / invariants
- Deterministic enum and payload validation on writes.
- Explicit degraded states (`PARTIAL`, `FAILED`, `STALE`, etc.) are persisted and surfaced.
- Explainability/source refs required in multiple MF4/MF5 paths (non-black-box contracts).
- Unsupported filters fail explicitly (no silent filter no-op in guarded paths).
- Actions endpoints return delegated/non-mutation semantics for cross-domain operations.

## 9. Testing / verification
Key evidence suites:
- Foundation write/read/validation:
  - `tests/unit/test_analytics_write_validation.py`
  - `tests/integration/test_analytics_write_service.py`
  - `tests/integration/test_analytics_read_service.py`
- External sync / MF2:
  - `tests/unit/test_analytics_external_sync.py`
  - `tests/unit/test_analytics_external_interfaces_hardening.py`
  - `tests/integration/test_analytics_external_sync_service.py`
- MF3:
  - `tests/unit/test_operational_kpi_runtime.py`
  - `tests/unit/test_operational_kpi_hardening.py`
  - `tests/integration/test_operational_kpi_derivation_service.py`
- MF4:
  - `tests/unit/test_mf4_derivation_core.py`
  - `tests/unit/test_mf4_runtime.py`
  - `tests/unit/test_mf4_hardening.py`
- MF5:
  - `tests/unit/test_mf5_recommendation_core.py`
  - `tests/unit/test_mf5_recommendation_runtime.py`
  - `tests/unit/test_mf5_recommendation_hardening.py`
- MF6/page/reporting contracts:
  - `tests/unit/test_mf6_page_contracts.py`
  - `tests/unit/test_mf6_reporting_helpers.py`
  - `tests/unit/test_mf6_hardening.py`

Suggested targeted verification:
```bash
python -m unittest -v tests.unit.test_analytics_write_validation tests.integration.test_analytics_write_service tests.integration.test_analytics_external_sync_service tests.unit.test_mf4_derivation_core tests.unit.test_mf5_recommendation_core tests.unit.test_mf6_reporting_helpers
```

## 10. Known limitations
- External provider scope is currently constrained to configured literals (YouTube provider; CHANNEL/RELEASE_VIDEO sync target scope types).
- `source_family` filter support is deliberately restricted by scope and can be rejected.
- Actions endpoints are lightweight/delegated and do not orchestrate cross-domain mutation workflows.
- Report generation requires required current source datasets; missing datasets return validation errors instead of partial synthetic reports.
