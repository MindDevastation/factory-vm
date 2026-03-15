# Post-Reboot Verification

Preferred production path: verify service auto-start, run smoke, then verify API + worker heartbeats.

## Checklist

1. Confirm core services are active (systemd example):

```bash
systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
```

2. Run smoke gate:

```bash
python scripts/ops_smoke.py --scenario post-reboot --profile prod
```

3. Confirm endpoints (`/health` unauthenticated, `/v1/workers` requires Basic Auth from deployment env `FACTORY_BASIC_AUTH_USER` / `FACTORY_BASIC_AUTH_PASS`):

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
```

4. If disk pressure is reported as warning/critical in smoke or logs, run retention procedures from `playbooks/low_disk_space.md`.

## Pass criteria

- Smoke returns `exit_code=0` and overall `OK` (**OPERATIONAL PASS**).
- `exit_code=1` (`WARNING`) is **OPERATIONAL WARNING**: run explicit review/escalation via `sop/when_smoke_fails.md` before declaring reboot verification complete.
- `exit_code>=2` is **OPERATIONAL FAIL**: stop and recover using `sop/when_smoke_fails.md`.
- `/health` responds successfully.
- `/v1/workers` returns expected active roles for enabled production flows.

## Source anchors

- Smoke verification command: `scripts/ops_smoke.py`, `scripts/doctor.py`, `docs/ops/production_smoke.md`
- API/worker endpoints: `README.md`, `services/factory_api/app.py`
- Service auto-start units: `deploy/systemd/*.service`
