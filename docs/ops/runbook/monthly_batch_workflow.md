# Monthly Batch Workflow

Preferred production path: pre-batch smoke gate, normal runtime processing, and post-batch hygiene checks.

## 1) Pre-batch gate (required)

```bash
python scripts/doctor.py production-smoke --profile prod
```

Proceed only when smoke overall status is `OK`.

## 2) Runtime processing baseline

For production service-managed deployments, keep API and required workers active via deployment-configured service manager commands.

If manual runtime fallback is needed (internal/debug-only), repository-supported stack runner is:

```bash
python scripts/run_stack.py --profile prod --with-bot 1
```

API-only mode without importer (alternative):

```bash
python scripts/run_stack.py --profile prod --with-bot 1 --no-importer
```

## 3) Mid-batch spot checks

```bash
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/v1/workers
```

## 4) End-of-batch checks

```bash
python scripts/doctor.py production-smoke --profile prod
```

If disk warnings/critical conditions appear, run retention workflow from `playbooks/low_disk_space.md`.
