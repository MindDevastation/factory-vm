# Testing

This project uses **unittest** (standard library) for Unit/Integration/E2E tests.

> Tip (Windows / PowerShell): most commands below assume you run them from the `factory-vm/` folder.
> If you see `ModuleNotFoundError: No module named 'services'`, set `PYTHONPATH=.` first.

---

## 0) One-time: ensure dependencies

```bash
pip install -r requirements.txt
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

---

## 3) Coverage

> `coverage` is included in `requirements.txt`.

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

