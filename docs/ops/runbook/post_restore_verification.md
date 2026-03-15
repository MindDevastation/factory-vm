# Post-Restore Verification

Preferred production path: restore with `scripts/ops_backup_restore.py`, then perform smoke + endpoint + service checks.

## 1) Confirm restore success output

Expected restore success includes:

- `restore_ok backup_id=<backup_id> restore_id=<restore_id> quarantine_dir=/quarantine/<restore_id>/ restored=<count>`

Restore command reference:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py restore --backup-id <backup_id>
```

## 2) Start services (manual)

Automatic service restart after restore is not implemented; start services manually via deployment-configured service manager command/path.

## 3) Required validation sequence

> `/health` is intentionally unauthenticated. `/v1/workers` requires API Basic Auth credentials (`FACTORY_BASIC_AUTH_USER` / `FACTORY_BASIC_AUTH_PASS`) from deployment environment config.
```bash
python scripts/ops_smoke.py --scenario post-restore --profile prod
curl -fsS http://127.0.0.1:8080/health
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
```

## 4) Restore-specific checks

1. Verify expected env/config/export files exist per restore scope configuration.
2. Review quarantine directory under `<FACTORY_BACKUP_DIR>/quarantine/<restore_id>/` (for example, `/quarantine/<restore_id>/`) for replaced pre-restore targets.
3. If smoke fails with DB/access symptoms, re-run backup verification and restore from a different known-good backup:

```bash
PYTHONPATH=. python scripts/ops_backup_restore.py backup list
PYTHONPATH=. python scripts/ops_backup_restore.py backup verify --backup-id <backup_id>
```

## Pass criteria

- Smoke returns `exit_code=0` and overall `OK` (**OPERATIONAL PASS**).
- `exit_code=1` (`WARNING`) is **OPERATIONAL WARNING**: complete explicit review/escalation via `sop/when_smoke_fails.md` before resuming normal production flow.
- `exit_code>=2` is **OPERATIONAL FAIL**: stop and recover via `sop/when_smoke_fails.md`.
- `/health` responds successfully.
- `/v1/workers` returns expected active roles for enabled production flows.

## Source anchors

- Restore/list/verify commands: `scripts/ops_backup_restore.py`, `docs/ops/backup_restore.md`
- Smoke verification command: `scripts/ops_smoke.py`, `scripts/doctor.py`, `docs/ops/production_smoke.md`
- API/worker endpoints and service startup context: `README.md`, `deploy/systemd/*.service`
