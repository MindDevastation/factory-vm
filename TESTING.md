# Testing

This project uses **unittest** (standard library) for Unit/Integration/E2E tests.

> Tip (Windows / PowerShell): most commands below assume you run them from the `factory-vm/` folder.
> If you see `ModuleNotFoundError: No module named 'services'`, set `PYTHONPATH=.` first.

---

## 0) One-time: ensure dependencies

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## Codex/CI environment setup

For Codex/CI environments, run:

```bash
python -m pip install -U pip
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 1) Run all tests

### Linux/macOS
```bash
PYTHONPATH=. python -m unittest discover -s tests -v
```

### Windows PowerShell
```powershell
$env:PYTHONPATH="."
python -m unittest discover -s tests -v
```

---

## 2) Run by test tier

### Unit
```bash
PYTHONPATH=. python -m unittest discover -s tests/unit -v
```

### Integration
```bash
PYTHONPATH=. python -m unittest discover -s tests/integration -v
```

### E2E (API + DB, mock upload)
```bash
PYTHONPATH=. python -m unittest discover -s tests/e2e -v
```

### UIJ-S4 manual smoke (UI jobs status filters)

> This repository currently does not include a Playwright browser E2E framework.
> Until one is added, use these manual smoke checks for `SPEC_UI_JOBS_FILTERS_v1.0 §16`:

1. Open the jobs UI page and verify default state shows all rows (no `statuses` query parameter in URL).
2. Select only `FAILED` in the status filter and verify:
   - the URL contains `statuses=FAILED`, and
   - the jobs table displays only `FAILED` rows.
3. Add `PLANNED` and verify:
   - the URL contains `statuses=PLANNED,FAILED` (backend status ordering), and
   - the table displays both `PLANNED` and `FAILED` rows.
4. Clear the status filter and verify:
   - the `statuses` query parameter is removed from the URL, and
   - all jobs are visible again.

---

## 3) Coverage

### Console report
```bash
PYTHONPATH=. coverage run -m unittest discover -s tests
coverage report -m
```

### HTML report
```bash
PYTHONPATH=. coverage run -m unittest discover -s tests
coverage html
```

Open `htmlcov/index.html` in a browser.

---

## 4) Notes on external integrations

- Tests mock Google Drive / YouTube SDKs. They do **not** require Google credentials.
- E2E tests run the FastAPI app using `TestClient` and an isolated temp SQLite DB.
## Render debug knobs (FFmpeg)

These env vars help diagnose stuck renders and make logs more informative:

- `FFMPEG_LOGLEVEL` — ffmpeg log level (default: `error`). Try `warning` or `info` while debugging.
- `FFMPEG_ECHO_STDERR` — echo ffmpeg stderr into `job_N.log` (default: **on**). Set `0` to disable.
- `PYTHONUNBUFFERED=1` — disables Python stdout buffering (recommended for Windows).
- `FORCE_CPU_ENCODER=1` — force CPU encoder (bypass NVENC).
- `RENDER_WATCHDOG_GRACE_SEC` — initial grace period before watchdog checks (default: `30`).
- `RENDER_WATCHDOG_IDLE_SEC` — seconds without output growth before marking render as stuck (default: `120`).
- `RENDER_WATCHDOG_MIN_DELTA_BYTES` — minimum byte growth to count as progress (default: `1024`).
- `RENDER_WATCHDOG_KILL_AFTER_SEC` — after watchdog triggers, kill ffmpeg if it did not exit (default: `15`).

When watchdog triggers, attempt 1 (NVENC) fails and the pipeline automatically retries with CPU encoder on attempt 2.

## 5) QA artifact hygiene

- Never commit `.qa_*` logs or reports that can capture environment values (for example auth/user/pass env vars).
- If you need to share a QA report, redact values and use placeholders like `FACTORY_BASIC_AUTH_USER=<set via env>` and `FACTORY_BASIC_AUTH_PASS=<set via env>`.
