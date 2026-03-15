# SOP: Preflight Environment Validation

## Required checks before launch/deploy

1. Python virtualenv active and dependencies installed from `requirements.txt`.
2. DB initialized and configs seeded:

```bash
python scripts/init_db.py
python scripts/seed_configs.py
```

3. Production env file created from `deploy/env.example` or `deploy/env.prod.example` and referenced by deployment service manager.
4. Smoke command runs from repo root:

```bash
python scripts/doctor.py production-smoke --profile prod
```

## Notes

- Use deployment-configured env path/command defined in `deploy/systemd/*.service` for production systemd deployments.
- Do not replace smoke with ad-hoc endpoint-only checks.
