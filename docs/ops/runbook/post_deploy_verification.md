# Post-Deploy Verification

Preferred production path: restart deployed services, run full smoke, then confirm API/worker endpoints.

## Checklist

1. Restart services using deployment-configured command/path defined in service manager artifacts (`deploy/systemd/*.service` for systemd deployments).
2. Run smoke:

```bash
python scripts/doctor.py production-smoke --profile prod
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

- Smoke returns `exit_code=0` and overall `OK`.
- `/health` responds successfully.
- `/v1/workers` returns expected active roles for the enabled production flows.

## Source anchors

- Smoke verification command: `scripts/doctor.py`, `docs/ops/production_smoke.md`
- API/worker endpoints: `README.md`, `services/factory_api/app.py`
- Service-manager artifacts: `deploy/systemd/*.service`
