# Post-Deploy Verification

Preferred production path: restart deployed services, run full smoke, then confirm API/worker endpoints.

## Checklist

1. Restart services using deployment-configured command/path defined in service manager artifacts (`deploy/systemd/*.service` for systemd deployments).
2. Run smoke:

```bash
python scripts/doctor.py production-smoke --profile prod
```

3. Check API health and workers:

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/v1/workers
```

4. If smoke or endpoint checks fail, use:
   - `playbooks/api_unhealthy.md`
   - `playbooks/worker_stalled.md`

## Pass criteria

- Smoke returns `exit_code=0` and overall `OK`.
- `/health` responds successfully.
- `/v1/workers` returns expected active roles for the enabled production flows.
