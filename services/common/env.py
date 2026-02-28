import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Env:
    db_path: str
    storage_root: str
    bind: str
    port: int
    basic_user: str
    basic_pass: str

    # runtime switching
    origin_backend: str       # gdrive | local
    origin_local_root: str    # used when origin_backend=local

    upload_backend: str       # youtube | mock
    telegram_enabled: int     # 1/0

    # gdrive origin
    gdrive_root_id: str
    gdrive_library_root_id: str
    gdrive_sa_json: str
    gdrive_oauth_client_json: str
    gdrive_oauth_token_json: str

    # oauth
    oauth_redirect_base_url: str
    oauth_state_secret: str

    gdrive_client_secret_json: str
    gdrive_tokens_dir: str

    # youtube oauth
    yt_client_secret_json: str
    yt_tokens_dir: str

    # telegram
    tg_bot_token: str
    tg_admin_chat_id: int

    # reliability/perf knobs
    qa_volumedetect_seconds: int
    job_lock_ttl_sec: int
    retry_backoff_sec: int
    max_render_attempts: int
    max_upload_attempts: int

    worker_sleep_sec: int
    db_viewer_policy_path: str = ""
    db_viewer_privileged_users: str = ""

    @staticmethod
    def load() -> "Env":
        return Env(
            db_path=os.environ.get("FACTORY_DB_PATH", "data/factory.sqlite3"),
            db_viewer_policy_path=os.environ.get("DB_VIEWER_POLICY_PATH", ""),
            db_viewer_privileged_users=os.environ.get("DB_VIEWER_PRIVILEGED_USERS", ""),
            storage_root=os.environ.get("FACTORY_STORAGE_ROOT", "storage"),
            bind=os.environ.get("FACTORY_BIND", "0.0.0.0"),
            port=int(os.environ.get("FACTORY_PORT", "8080")),
            basic_user=os.environ.get("FACTORY_BASIC_AUTH_USER", "admin"),
            basic_pass=os.environ.get("FACTORY_BASIC_AUTH_PASS", "change_me"),

            origin_backend=os.environ.get("ORIGIN_BACKEND", "gdrive"),
            origin_local_root=os.environ.get("ORIGIN_LOCAL_ROOT", "local_origin"),

            upload_backend=os.environ.get("UPLOAD_BACKEND", "youtube"),
            telegram_enabled=int(os.environ.get("TELEGRAM_ENABLED", "1")),

            gdrive_root_id=os.environ.get("GDRIVE_ROOT_ID", ""),
            gdrive_library_root_id=os.environ.get("GDRIVE_LIBRARY_ROOT_ID", ""),
            gdrive_sa_json=os.environ.get("GDRIVE_SERVICE_ACCOUNT_JSON", ""),
            gdrive_oauth_client_json=os.environ.get("GDRIVE_OAUTH_CLIENT_JSON", ""),
            gdrive_oauth_token_json=os.environ.get("GDRIVE_OAUTH_TOKEN_JSON", ""),

            oauth_redirect_base_url=os.environ.get("OAUTH_REDIRECT_BASE_URL", ""),
            oauth_state_secret=os.environ.get("OAUTH_STATE_SECRET", ""),

            gdrive_client_secret_json=os.environ.get("GDRIVE_CLIENT_SECRET_JSON", ""),
            gdrive_tokens_dir=os.environ.get("GDRIVE_TOKENS_DIR", ""),

            yt_client_secret_json=os.environ.get("YT_CLIENT_SECRET_JSON", ""),
            yt_tokens_dir=os.environ.get("YT_TOKENS_DIR", ""),

            tg_bot_token=os.environ.get("TG_BOT_TOKEN", ""),
            tg_admin_chat_id=int(os.environ.get("TG_ADMIN_CHAT_ID", "0")),

            # Keep QA fast for multi-hour videos; can be overridden via env.
            qa_volumedetect_seconds=int(os.environ.get("QA_VOLUMEDETECT_SECONDS", "60")),
            job_lock_ttl_sec=int(os.environ.get("JOB_LOCK_TTL_SEC", str(12 * 3600))),
            retry_backoff_sec=int(os.environ.get("RETRY_BACKOFF_SEC", "300")),
            max_render_attempts=int(os.environ.get("MAX_RENDER_ATTEMPTS", "3")),
            max_upload_attempts=int(os.environ.get("MAX_UPLOAD_ATTEMPTS", "3")),

            worker_sleep_sec=int(os.environ.get("WORKER_SLEEP_SEC", "5")),
        )
