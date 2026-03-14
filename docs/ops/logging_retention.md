# Logging retention: project-owned files + journald caps

This document defines bounded retention controls for `SPEC_OPS_LOGGING_RETENTION_v1.0` P0-S2.

## 1) Project-owned file logs

The runtime keeps class-separated rotating files under `storage/logs/`:

- `app.log`
- `workers.log`
- `bot.log`
- `pipeline.log`
- `recovery.log`
- `ops.log`

Rotation policy defaults:

- application: 20 MiB, keep 10
- workers: 20 MiB, keep 10
- bot: 10 MiB, keep 7
- uploader/render/pipeline: 25 MiB, keep 8
- recovery/audit: 10 MiB, keep 12
- smoke/ops: 5 MiB, keep 12

Notes:

- Rotation is size-based and deterministic from service name -> log class.
- Active file remains `*.log`; rotated files are bounded by backup count.
- Existing stdout logging remains enabled (for systemd/journald).

## 2) Journald retention hardening (deploy-level)

Apply deploy snippet:

- source: `deploy/systemd/journald-retention.conf`
- target: `/etc/systemd/journald.conf.d/factory-retention.conf`

Configured caps:

- `SystemMaxUse=512M`
- `RuntimeMaxUse=128M`
- `MaxRetentionSec=30day`

Example apply commands:

```bash
sudo mkdir -p /etc/systemd/journald.conf.d
sudo cp deploy/systemd/journald-retention.conf /etc/systemd/journald.conf.d/factory-retention.conf
sudo systemctl restart systemd-journald
```

Validation:

```bash
systemd-analyze cat-config systemd/journald.conf
journalctl --disk-usage
```
