# Post-Reboot Verification

Preferred production path: verify service auto-start, run smoke, then verify API + worker heartbeats.

## Checklist

1. Confirm core services are active (systemd example):

```bash
systemctl status factory-api.service factory-orchestrator.service factory-qa.service factory-uploader.service factory-cleanup.service
```

2. Run smoke gate:

```bash
python scripts/doctor.py production-smoke --profile prod
```

3. Confirm endpoints:

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/v1/workers
```

4. If disk pressure is reported as warning/critical in smoke or logs, run retention procedures from `playbooks/low_disk_space.md`.
