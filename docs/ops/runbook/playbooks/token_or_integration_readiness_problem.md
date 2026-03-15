# Playbook: Token or Integration Readiness Problem

## Symptoms
- Smoke reports integration readiness blockers (`youtube_ready`, `gdrive_ready`, `telegram_ready`, or `pipeline_readiness.integration_blockers`).
- Upload/origin integration operations fail while core API stays up.
- OAuth token presence/readiness appears missing for required channels.

## Likely causes
- Missing/invalid token files or client secret paths in environment.
- Expired/revoked per-channel OAuth grants.
- Profile/config mismatch (`--profile prod` with incomplete prod integration env).

## Checks to perform
1. Run smoke and inspect readiness details:
   ```bash
   python scripts/doctor.py production-smoke --profile prod --json
   ```
2. Check OAuth status endpoint:
   ```bash
   curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/oauth/status
   ```
3. Confirm configured env files/paths exist per deployment env settings.

## Actions to take
- **Preferred production path:** Regenerate/re-authorize integration tokens using dashboard OAuth Tokens actions (Generate/Regenerate) for affected channels.
- For controlled API flow, use implemented OAuth start endpoints (`/v1/oauth/youtube/{channel_slug}/start`, `/v1/oauth/gdrive/{channel_slug}/start`) and complete callback flow.
- Re-run smoke after token regeneration.
- **Alternative / debug-only path:** `scripts/youtube_auth.py`/console OAuth can be used only for targeted debugging when dashboard/API flow is blocked.

## Verification after fix
- OAuth status reflects expected token presence for affected channels.
- Smoke no longer reports integration blockers.
- New jobs pass stages that previously failed on token/integration checks.

## Escalation / fallback
- If OAuth flow fails repeatedly, escalate with endpoint response details and environment path checks.
