# Playbook: API Unhealthy

## Trigger

- `curl -fsS http://127.0.0.1:8080/health` fails, or
- smoke reports API/access failures.

## Preferred production path

1. Check service status (systemd deployments):

```bash
systemctl status factory-api.service
```

2. Review recent API logs:

```bash
journalctl -u factory-api.service -n 200 --no-pager
```

3. Re-run smoke:

```bash
python scripts/doctor.py production-smoke --profile prod
```

4. Validate workers endpoint when API recovers:

```bash
curl -fsS -u "${FACTORY_BASIC_AUTH_USER}:${FACTORY_BASIC_AUTH_PASS}" http://127.0.0.1:8080/v1/workers
```

## Escalation

If API repeatedly fails after restart/log review, stop deployment rollout and treat as incident requiring code/config investigation.
