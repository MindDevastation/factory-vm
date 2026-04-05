# Epic 5 — Visual Automation Pack (Implemented)

## 1. Purpose
Epic 5 provides release-level visual decisioning for **background + cover** with explicit lifecycle controls (preview → approve → apply), runtime binding into job inputs, reuse warnings, and batch entry points.

## 2. Scope
Implemented scope includes:
- Background candidate listing and preview/approve/apply flow.
- Cover candidate ingestion/listing/selection and approve/apply flow.
- Release-level persisted applied package and history trail.
- Scoped preview approvals (`background`, `cover`, `package`).
- Batch preview/execute sessions with scope fingerprinting and expiry.
- Channel visual style template API surface (create/list/update/archive/activate/default + release override/effective resolution).

## 3. Non-goals (frozen boundaries)
Current v1 behavior explicitly excludes:
- Separate thumbnail entity; thumbnail is always derived from cover.
- Background generation engine (selection from adapters/assets only).
- Semantic/perceptual no-repeat engine (only exact/same-asset reuse warning heuristics).
- Design-system or UI redesign scope.
- Adjacent-domain mutations outside visual package lifecycle.

## 4. Architecture / components
Core components:
- API routes under `/v1/visual/...` and template API routes under `/v1/metadata/channel-visual-style-templates...`.
- Services:
  - `background_assignment_service` (candidate/preview/approve/apply).
  - `cover_assignment_service` (input payloads, candidate refs, selection, preview/approve/apply).
  - `visual_batch_service` (sessionized preview/execute with invalidation rules).
  - `runtime_visual_resolver` (apply release package + resolve job bindings).
  - `visual_history_service` (history events + reuse warning lookup).
  - `visual_domain` (tokenized stale/conflict safety and lifecycle validation).

## 5. Data model / persistence
Canonical operational persistence:
- `release_visual_preview_snapshots`
- `release_visual_approved_previews_scoped`
- `release_visual_applied_packages`

Supporting persistence:
- `release_visual_approved_previews` (compat path)
- `release_visual_background_decisions`
- `release_visual_cover_selection_inputs`
- `release_visual_cover_candidates`
- `release_visual_cover_selected_candidates`
- `release_visual_history_events`
- `release_visual_batch_preview_sessions`

Important identity/lifecycle points:
- Preview identity is snapshot `id` (e.g., `vbg-*`, `vcvp-*`).
- Approvals are scope-bound (`background|cover|package`) and upserted per release+scope.
- Apply writes/updates single release-level package row with `source_preview_id`.
- Legacy `release_visual_configs` is retained for compatibility, but not canonical for Epic-5 flow.

## 6. API / UI surface
### API-first endpoints (implemented)
Background flow:
- `GET /v1/visual/releases/{release_id}/background/candidates`
- `POST /v1/visual/releases/{release_id}/background/preview`
- `POST /v1/visual/releases/{release_id}/background/approve`
- `POST /v1/visual/releases/{release_id}/background/apply`

Cover flow:
- `POST /v1/visual/releases/{release_id}/cover/input-payload`
- `POST /v1/visual/releases/{release_id}/cover/candidates`
- `GET /v1/visual/releases/{release_id}/cover/candidates`
- `GET /v1/visual/releases/{release_id}/cover/candidates/{candidate_id}/preview`
- `POST /v1/visual/releases/{release_id}/cover/select`
- `POST /v1/visual/releases/{release_id}/cover/approve`
- `POST /v1/visual/releases/{release_id}/cover/apply`

History + batch:
- `GET /v1/visual/releases/{release_id}/history`
- `POST /v1/visual/batch/preview`
- `POST /v1/visual/batch/execute`

Template management API:
- `/v1/metadata/channel-visual-style-templates` (+ detail/patch/archive/activate/set-default)
- release override/effective endpoints.

### UI surface
- Job edit UI includes Epic-5 workflow controls and calls the API endpoints above.
- Template management is documented in UI as API-first (not a dedicated rich UI management surface in this page).

## 7. Runtime behavior / workflow
Resolution model:
1. Operator previews/approves scoped visual decisions.
2. Apply writes release-level `background_asset_id + cover_asset_id` package.
3. Runtime resolver checks `releases.current_open_job_id`.
   - If open job exists and belongs to release: replaces `BACKGROUND`/`COVER` job_inputs and syncs `ui_job_drafts` names.
   - If no open job or mismatch: marks as deferred (release decision persisted, runtime binding postponed).

Compatibility rules:
- BACKGROUND and COVER are both required for apply path correctness.
- Thumbnail is derived from cover (`thumbnail_source.source_kind = cover_asset`).

Batch behavior:
- Preview session stores scope fingerprint + per-item preview and warnings.
- Execute requires exact scope match and non-expired OPEN session.
- Session expiry marks invalidation reason and blocks execution.

## 8. Safety rules / invariants
- No apply without approved preview.
- No apply without `stale_token` and `conflict_token`.
- Stale/conflict mismatches are rejected (no silent overwrite).
- Re-applying same approved preview is blocked (`already applied`).
- Exact/same-asset reuse signals require explicit `reuse_override_confirmed` when flagged.
- Batch execute requires explicit overwrite confirmation for already-applied releases.
- Behavior is additive migration-wise; legacy table remains for compatibility.

## 9. Testing / verification
Key evidence families:
- Schema and canonical table contracts:
  - `tests/unit/test_visual_foundation_schema.py`
  - `tests/unit/test_epic5_schema_provisioning.py`
  - `tests/unit/test_release_visual_configs_contract.py`
- Lifecycle safety/tokens/scoped approvals:
  - `tests/unit/test_visual_lifecycle_safety.py`
  - `tests/unit/test_visual_scoped_preview_approval.py`
  - `tests/unit/test_visual_domain.py`
- Reuse warnings/history:
  - `tests/unit/test_visual_history_service.py`
- Cover/background contracts:
  - `tests/unit/test_cover_candidate_contract.py`
  - `tests/unit/test_background_source_adapter_registry.py`
- Batch preview/execute + conflict semantics:
  - `tests/integration/test_visual_batch_api.py`
  - `tests/integration/test_visual_batch_scope_hardening.py`
- UI/API smoke:
  - `tests/e2e/test_epic5_visual_smoke.py`
  - `tests/e2e/test_ui_pages_slice4.py`

Suggested targeted verification:
```bash
python -m unittest -v tests.unit.test_visual_foundation_schema tests.unit.test_visual_lifecycle_safety tests.unit.test_visual_history_service tests.unit.test_cover_candidate_contract tests.integration.test_visual_batch_scope_hardening
```

## 10. Known limitations
- Thumbnail is not separately assignable/stored.
- Reuse warning is exact/same-asset based; no semantic similarity engine.
- Background selection relies on existing adapters/assets; no generative background creation.
- Template management is API-first; no separate full template admin UI in this flow.
