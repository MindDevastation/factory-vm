# Local setup (Windows, PowerShell) — Factory VM (MVP)

Ниже — пошаговое развёртывание **с нуля** на Windows так, как мы делали в чате: venv → зависимости → env → DB → seed → dev release → запуск стека → открытие UI → базовые проверки → остановка → типовые проблемы.

> Эти команды рассчитаны на запуск из **PowerShell** в корне проекта `factory-vm`.
> Если используешь **CMD**, см. отдельные примеры в соответствующих блоках.

---

## 0) Предусловия

### 0.1 Python
- Python **3.12+** установлен (в PyCharm выбран интерпретатор venv проекта).

### 0.2 FFmpeg (обязательно)
Проект использует `ffmpeg` и `ffprobe` для рендера/QA.

Проверь в терминале:

```powershell
ffmpeg -version
ffprobe -version
```

Если команды не найдены — установи FFmpeg и добавь в `PATH` (иначе рендер/QA будут падать).

---

## 1) Создать проект и окружение (venv)

1) Распакуй архив проекта в папку, например:  
`...\YouTube_render_project\factory-vm\`

2) В PyCharm создай/подключи интерпретатор:
- **Settings → Project → Python Interpreter → Add Interpreter → Virtualenv**
- Версия Python: 3.12

3) Установи зависимости:

```powershell
pip install -U pip
pip install -r requirements.txt
```

---

## 2) Подготовить env-файл (deploy/env)

Проект читает переменные окружения из файла `deploy/env`.

### 2.1 Создать `deploy/env` из примера

В PowerShell:

```powershell
Copy-Item deploy\env.local.example deploy\env
```

Если `env.local.example` отсутствует, попробуй:

```powershell
Copy-Item deploy\env.example deploy\env
```

### 2.2 Минимальные значения для локального режима

Открой `deploy/env` и проверь/выставь **минимум**:

```env
# локальный режим (без Google Drive/YouTube/Telegram)
ORIGIN_BACKEND=local
UPLOAD_BACKEND=mock
TELEGRAM_ENABLED=0

# Basic Auth для UI/API
FACTORY_BASIC_AUTH_USER=admin
FACTORY_BASIC_AUTH_PASS=change_me_123

# чтобы не падало на парсинге int (даже если TG выключен)
TG_ADMIN_CHAT_ID=0
```

#### Важное про путь к БД
Рекомендуется хранить БД рядом с проектом:

```env
FACTORY_DB_PATH=./data/factory.sqlite3
```

Если оставить linux-путь вида `/opt/...`, на Windows он станет `C:\opt\...` — это работает, но БД окажется “вне проекта”.

---

## 3) PowerShell: правильно выставить PYTHONPATH

Если запускать скрипты без `PYTHONPATH`, будет ошибка вида:
`ModuleNotFoundError: No module named 'services'`.

В PowerShell нужно так:

```powershell
$env:PYTHONPATH="."
```

Проверка:

```powershell
python -c "import services; print('OK')"
```

Должно вывести `OK`.

> ❗️ В PowerShell команда `set PYTHONPATH=.` **не работает** (это синтаксис CMD).

### CMD-эквивалент (если вдруг нужен)
В CMD:

```bat
set PYTHONPATH=.
python -c "import services; print('OK')"
```

---

## 4) Инициализация базы данных и сидинг конфигов

Создай папку `data` (если её нет):

```powershell
New-Item -ItemType Directory -Force data | Out-Null
```

Запусти:

```powershell
python scripts\init_db.py
python scripts\seed_configs.py
```

Ожидаемые сообщения:
- `DB initialized at: ...`
- `Seeded channels + render_profiles.`

### Где лежит БД?
Проверь фактический путь:

```powershell
python -c "from services.common.profile import load_profile_env; load_profile_env(); import os; print(os.environ.get('FACTORY_DB_PATH'))"
```

И проверь, что файл существует:

```powershell
Test-Path .\data\factory.sqlite3
Get-Item .\data\factory.sqlite3 | Select FullName,Length,LastWriteTime
```

---

## 5) Создать тестовый релиз (dev smoke)

Создаём входные данные для конвейера:

```powershell
python scripts\gen_dev_release.py --channel darkwood-reverie
```

Если получишь ошибку “unknown channel” — проверь какие каналы засеялены (в `configs/` или через UI), и используй корректный `--channel`.

---

## 6) Запустить стек (API + воркеры)

Запуск:

```powershell
python scripts\run_stack.py --profile local --with-bot 0
```

Оставь терминал открытым — процесс должен работать постоянно.

### Открыть UI
Открой в браузере:

- `http://127.0.0.1:8080/`

Введи Basic Auth из `deploy/env`:
- `FACTORY_BASIC_AUTH_USER`
- `FACTORY_BASIC_AUTH_PASS`

### Проверить, что воркеры живые
Открой:

- `http://127.0.0.1:8080/v1/workers`

Должен вернуться JSON со списком воркеров (importer/orchestrator/qa/uploader/cleanup) и их `state/last_seen`.

---

## 7) Как понять, что рендер реально идёт

### 7.1 На странице job
В UI кликни по `Job ID` → смотри блок **Logs tail**.
Там обычно появляется строка вида:

`CMD: ... python.exe render_worker\main.py --root ...`

### 7.2 “Хвост” job-лога из PowerShell
(самый надёжный способ)

```powershell
Get-Content .\storage\logs\job_1.log -Tail 200 -Wait
```

### 7.3 Почему в UI может быть 0% (и это не баг рендера)
UI показывает прогресс, только если `render_worker` пишет/парсит события прогресса.
Иногда рендер идёт, а UI “висит” на 0% из-за буферизации stdout.

#### Решение: включить unbuffered stdout
В `deploy/env` добавь:

```env
PYTHONUNBUFFERED=1
```

Перезапусти стек.

---

## 8) Где лежат логи

По умолчанию логи в:

- `storage/logs/`
  - `factory_api.log`
  - `job_1.log`, `job_2.log`, ...
  - `worker-cleanup.log`, `worker-orchestrator.log`, ...

Важно:
- `worker-cleanup.log` часто “живой”, потому что cleanup пишет запись каждый цикл.
- Остальные воркеры могут писать мало (особенно в idle), а основные подробности идут в `job_*.log`.

---

## 9) Как остановить всё

Самый правильный способ:
- перейти в терминал, где запущен `run_stack.py`
- нажать **Ctrl + C** (иногда дважды)

Если нужно “жёстко” — закрыть вкладку терминала в PyCharm или остановить процесс Run/Terminal.

---

## 10) Частые проблемы и решения

### 10.1 `ModuleNotFoundError: No module named 'services'`
Причина: `PYTHONPATH` не выставлен.

PowerShell:

```powershell
$env:PYTHONPATH="."
python -c "import services; print('OK')"
```

### 10.2 БД создалась “не там” (например, показывается `/opt/...`)
Причина: `FACTORY_DB_PATH` в `deploy/env` указан как linux-путь.

Решение:
- поставь в `deploy/env`:

```env
FACTORY_DB_PATH=./data/factory.sqlite3
```

- затем снова:

```powershell
python scripts\init_db.py
python scripts\seed_configs.py
```

### 10.3 Нет логов рендера / UI стоит на 0%
Возможна буферизация stdout.

Решение:
- добавить `PYTHONUNBUFFERED=1` в `deploy/env`
- перезапустить стек
- смотреть `storage/logs/job_N.log`

### 10.4 NVENC/кодирование (если рендер использует NVENC)
Если профиль рендера выставлен на NVENC, убедись, что ffmpeg поддерживает NVENC:

```powershell
ffmpeg -hide_banner -encoders | findstr /i nvenc
```

Если NVENC отсутствует/падает — переключись на CPU-энкодер (например, libx264) в render profile.

---

## 11) Быстрый smoke-check после запуска

1) UI открывается: `http://127.0.0.1:8080/`
2) В `/v1/workers` видны воркеры со state `running`
3) Создан dev release (`gen_dev_release.py`)
4) В списке jobs появляется job
5) На странице job в **Logs tail** виден `CMD: ... render_worker...`
6) В `storage/logs/job_N.log` появляются записи рендера

---

## 12) Полезные команды

### Проверить ключевые env
```powershell
python -c "from services.common.profile import load_profile_env; load_profile_env(); import os; print('ORIGIN_BACKEND=',os.environ.get('ORIGIN_BACKEND')); print('UPLOAD_BACKEND=',os.environ.get('UPLOAD_BACKEND')); print('TELEGRAM_ENABLED=',os.environ.get('TELEGRAM_ENABLED')); print('FACTORY_DB_PATH=',os.environ.get('FACTORY_DB_PATH'))"
```

### Запуск тестов
```powershell
python -m unittest discover -s tests -v
```

---

**Готово.** Этот файл можно хранить в репозитории как `LOCAL_SETUP_WINDOWS.md`.
