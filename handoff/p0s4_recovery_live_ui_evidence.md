# P0-S4 reviewer evidence addendum (PR #344 follow-up)

This addendum captures live Recovery UI evidence from the same working runtime context used for seeded Slice 4 validation.

## Live runtime context used

- URL: `http://127.0.0.1:8010/ui/recovery`
- Auth: HTTP basic auth (`admin` / `testpass`)
- Seed profile: Slice 4 recovery-style seeded jobs (`FAILED`, `RENDERING` stale lock, cleanup pending) with seeded recovery audit entries.

## Captured UI evidence

1. Recovery details modal (job `1`) open in live Recovery UI context.
   - Shows **Worker role / runtime context** block.
   - Shows **Recent recovery audit entries** section with non-empty seeded content.
   - Artifact: `browser:/tmp/codex_browser_invocations/53d4efd3c77e1c8b/artifacts/artifacts/p0s4_recovery_details_modal.png`

2. Action preview modal for `reenqueue_allowed_stage` in live Recovery UI context.
   - Shows **Allowed stage token** control/state.
   - Artifact: `browser:/tmp/codex_browser_invocations/364abfb56e341cec/artifacts/artifacts/p0s4_recovery_stage_token_modal.png`

3. Recovery details modal (job `1`) with explicit, non-empty recent audit payload visible.
   - Shows **Recent recovery audit entries** section in-frame with populated JSON (`action_name: "retry_failed"`, `result_status: "success"`).
   - Seed note (deterministic and minimal): one seeded recovery audit row was inserted for job `1` to guarantee non-empty reviewer-visible content.
   - URL path used: `http://127.0.0.1:8010/ui/recovery` (details modal opened via first row “Open”).
   - Artifact: `browser:/tmp/codex_browser_invocations/a7ec410859c5b608/artifacts/artifacts/p0s4_recovery_details_modal_non_empty_audit.png`

## Notes

- This is evidence-only scope: no feature/API/action semantics changes.
- Both captures come from a working `/ui/recovery` runtime context (HTTP 200), not a 404 route mismatch.
