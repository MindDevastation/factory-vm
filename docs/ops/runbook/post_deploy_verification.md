# Post-Deploy Verification

Preferred production path: restart deployed services, run full smoke, then confirm API/worker endpoints.

## Checklist

1. Restart services using deployment-configured command/path defined in service manager artifacts (`deploy/systemd/*.service` for systemd deployments).
2. Run smoke via operational scenario wrapper:

```bash
python scripts/ops_smoke.py --scenario post-deploy --profile prod
```

3. Check API health and workers (`/health` unauthenticated, `/v1/workers` requires Basic Auth from deployment env `FACTORY_BASIC_AUTH_USER` / `FACTORY_BASIC_AUTH_PASS`):

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
```

4. If smoke or endpoint checks fail, use:
   - `playbooks/api_not_responding.md`
   - `playbooks/worker_heartbeat_missing.md`

## Pass criteria

- Smoke returns `exit_code=0` and overall `OK` (**OPERATIONAL PASS**).
- `exit_code=1` (`WARNING`) is **OPERATIONAL WARNING**: do not proceed silently; perform explicit review/escalation with `sop/when_smoke_fails.md` before continuing.
- `exit_code>=2` is **OPERATIONAL FAIL**: stop deploy verification flow and follow `sop/when_smoke_fails.md`.
- `/health` responds successfully.
- `/v1/workers` returns expected active roles for the enabled production flows.

## Source anchors

- Smoke verification command: `scripts/ops_smoke.py`, `scripts/doctor.py`, `docs/ops/production_smoke.md`
- API/worker endpoints: `README.md`, `services/factory_api/app.py`
- Service-manager artifacts: `deploy/systemd/*.service`
