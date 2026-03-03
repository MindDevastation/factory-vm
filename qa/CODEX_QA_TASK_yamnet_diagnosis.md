# CODEX_QA_TASK — Yamnet install visibility diagnostics

## Step 1: Identify containers

Command:
```bash
docker compose ps
```

Output:
```text
bash: command not found: docker
```

## Step 2: Check yamnet import in each container

Command:
```bash
docker compose exec -T dashboard python -c "import sys; print('dashboard_py', sys.executable); import yamnet; print('dashboard_yamnet_OK', yamnet.__file__)" || echo "dashboard_yamnet_IMPORT_FAIL=$?"
```
Output:
```text
bash: command not found: docker
dashboard_yamnet_IMPORT_FAIL=127
```

Command:
```bash
docker compose exec -T factory-api python -c "import sys; print('api_py', sys.executable); import yamnet; print('api_yamnet_OK', yamnet.__file__)" || echo "api_yamnet_IMPORT_FAIL=$?"
```
Output:
```text
bash: command not found: docker
api_yamnet_IMPORT_FAIL=127
```

Command:
```bash
docker compose exec -T worker-track_jobs python -c "import sys; print('worker_py', sys.executable); import yamnet; print('worker_yamnet_OK', yamnet.__file__)" || echo "worker_yamnet_IMPORT_FAIL=$?"
```
Output:
```text
bash: command not found: docker
worker_yamnet_IMPORT_FAIL=127
```

## Step 3: Check pip visibility in worker

Command:
```bash
docker compose exec -T worker-track_jobs python -m pip show yamnet || echo "worker_pip_show_FAIL=$?"
```
Output:
```text
bash: command not found: docker
worker_pip_show_FAIL=127
```

Command:
```bash
docker compose exec -T worker-track_jobs python -c "import site; print('sitepackages', site.getsitepackages()); print('usersite', site.getusersitepackages())"
```
Output:
```text
bash: command not found: docker
```

## Step 4: Capture logs around UI install click

Commands:
```bash
docker compose logs --no-color --tail=300 dashboard > .qa_dashboard_tail.log || true
docker compose logs --no-color --tail=300 worker-track_jobs > .qa_worker_tail.log || true
```

Execution output:
```text
bash: command not found: docker
bash: command not found: docker
```

Log file sizes:
```text
0 .qa_dashboard_tail.log
0 .qa_worker_tail.log
0 total
```

## Evidence from repository code (why worker may not see installs done in API/UI process)

- The UI endpoint `/v1/admin/yamnet/install` runs `scripts/install_yamnet.py` via `sys.executable` from the API process, so it installs into the API container/python environment where the endpoint is executed.
- The worker (`services/workers/track_jobs.py`) is a separate process/container in typical compose setups; if it has a different image/venv, those packages will not automatically appear there.
- `requirements-yamnet.txt` installs `tensorflow-cpu` and `tensorflow-hub`; there is no package named `yamnet` in this requirements file.

## PASS/FAIL for this QA run

- **FAIL (environment limitation):** Unable to determine live container state because `docker` CLI is not available in this execution environment (`command not found`).

## Minimal fix proposal

1. Ensure yamnet dependencies are installed in the same runtime image used by `worker-track_jobs` (not only API/dashboard):
   - Add `RUN python -m pip install -r requirements-yamnet.txt` to the worker image Dockerfile, or
   - Build a shared base image used by both API and worker with `requirements-yamnet.txt` installed.
2. Recreate containers (`docker compose up -d --build`) and rerun this QA checklist.
3. Optional hardening: expose an admin endpoint/check that runs in worker context (or a startup self-check in worker) to confirm `tensorflow` + `tensorflow_hub` importability.
