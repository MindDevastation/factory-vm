from __future__ import annotations

import base64
import logging
import secrets
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from services.common import db as dbm
from services.common.env import Env
from services.db_viewer.filtering import (
    detect_text_columns,
    filter_allowed_tables,
    filter_visible_columns,
    make_human_table_name,
)
from services.db_viewer.meta import is_safe_identifier, list_existing_tables, list_table_columns
from services.db_viewer.policy import DbViewerPolicyError, is_privileged, load_policy
from services.db_viewer.rate_limit import GROUP_READ, InMemoryRateLimiter

logger = logging.getLogger(__name__)

_ALLOWED_PAGE_SIZES = {10, 50, 100}
_limiter = InMemoryRateLimiter()


def _request_id(request: Request) -> str:
    existing = getattr(request.state, "dbv_request_id", "")
    if isinstance(existing, str) and existing:
        return existing
    rid = request.headers.get("X-Request-Id", "").strip() or secrets.token_hex(8)
    request.state.dbv_request_id = rid
    return rid


def _error(
    request: Request,
    *,
    code: str,
    message: str,
    status_code: int,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    rid = _request_id(request)
    logger.warning("db_viewer_error code=%s request_id=%s details=%s", code, rid, details or {})
    payload: dict[str, Any] = {
        "error": {"code": code, "message": message, "request_id": rid},
    }
    if details:
        payload["error"]["details"] = details
    return JSONResponse(status_code=status_code, content=payload)


def _require_db_viewer_auth(env: Env):
    async def _dep(request: Request) -> str:
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            return ""
        try:
            raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
            user, pwd = raw.split(":", 1)
        except Exception:
            return ""
        if secrets.compare_digest(user, env.basic_user) and secrets.compare_digest(pwd, env.basic_pass):
            return user
        return ""

    return _dep


def _quote_ident(ident: str) -> str:
    return f'"{ident}"'


def _validated_page(request: Request, page: int, page_size: int, search: str, sort_dir: str) -> JSONResponse | None:
    if page < 1:
        return _error(request, code="DBV_INVALID_PARAMS", message="page must be >= 1", status_code=422)
    if page_size not in _ALLOWED_PAGE_SIZES:
        return _error(request, code="DBV_INVALID_PARAMS", message="page_size must be one of 10, 50, 100", status_code=422)
    if len(search) > 50:
        return _error(request, code="DBV_INVALID_PARAMS", message="search length must be <= 50", status_code=422)
    if sort_dir not in ("asc", "desc"):
        return _error(request, code="DBV_INVALID_PARAMS", message="sort_dir must be asc or desc", status_code=422)
    return None


def _table_access(table_name: str, username: str, env: Env) -> tuple[str, dict[str, Any], list[str]]:
    conn = dbm.connect(env)
    try:
        existing_tables = list_existing_tables(conn)
    finally:
        conn.close()

    policy = load_policy(env)
    allowed = filter_allowed_tables(existing_tables, policy["denylist_tables"])

    if not is_safe_identifier(table_name) or table_name not in existing_tables:
        return "not_found", policy, allowed
    if table_name not in allowed:
        if is_privileged(username, env):
            return "forbidden", policy, allowed
        return "not_found", policy, allowed
    return "ok", policy, allowed


def create_db_viewer_router(env: Env) -> APIRouter:
    router = APIRouter(prefix="/v1/db-viewer", tags=["db-viewer"])

    @router.get("/tables")
    def db_viewer_tables(request: Request, username: str = Depends(_require_db_viewer_auth(env))):
        if not username:
            return _error(request, code="DBV_UNAUTHORIZED", message="Unauthorized", status_code=401)
        if _limiter.is_limited(username, GROUP_READ):
            return _error(request, code="DBV_RATE_LIMITED", message="rate limit exceeded", status_code=429)

        try:
            conn = dbm.connect(env)
            try:
                existing_tables = list_existing_tables(conn)
            finally:
                conn.close()
            policy = load_policy(env)
        except DbViewerPolicyError as exc:
            logger.exception("db_viewer_tables policy_error request_id=%s", _request_id(request))
            return _error(request, code="DBV_POLICY_ERROR", message="db viewer policy error", status_code=500)
        except Exception:
            logger.exception("db_viewer_tables failed request_id=%s", _request_id(request))
            return _error(request, code="DBV_INTERNAL", message="db viewer internal error", status_code=500)

        allowed = filter_allowed_tables(existing_tables, policy["denylist_tables"])
        return {
            "tables": [
                {"name": table_name, "human_name": make_human_table_name(table_name, policy["human_name_overrides"])}
                for table_name in allowed
            ]
        }

    @router.get("/tables/{table_name}/rows")
    def db_viewer_rows(
        request: Request,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
        sort_by: str = "",
        sort_dir: str = "asc",
        search: str = "",
        username: str = Depends(_require_db_viewer_auth(env)),
    ):
        if not username:
            return _error(request, code="DBV_UNAUTHORIZED", message="Unauthorized", status_code=401)
        if _limiter.is_limited(username, GROUP_READ):
            return _error(request, code="DBV_RATE_LIMITED", message="rate limit exceeded", status_code=429)

        invalid = _validated_page(request, page, page_size, search, sort_dir)
        if invalid is not None:
            return invalid

        try:
            access, _policy, _allowed = _table_access(table_name, username, env)
        except DbViewerPolicyError:
            logger.exception("db_viewer_rows policy_error table=%s request_id=%s", table_name, _request_id(request))
            return _error(request, code="DBV_POLICY_ERROR", message="db viewer policy error", status_code=500)
        except Exception:
            logger.exception("db_viewer_rows table_access_failed table=%s request_id=%s", table_name, _request_id(request))
            return _error(request, code="DBV_INTERNAL", message="db viewer internal error", status_code=500)

        if access == "not_found":
            return _error(request, code="DBV_TABLE_NOT_FOUND", message="table not found", status_code=404)
        if access == "forbidden":
            return _error(request, code="DBV_TABLE_FORBIDDEN", message="table is forbidden", status_code=403)

        try:
            conn = dbm.connect(env)
            try:
                column_meta = list_table_columns(conn, table_name)
                visible_columns = filter_visible_columns([str(c["name"]) for c in column_meta])
                text_columns = detect_text_columns(column_meta).intersection(set(visible_columns))

                if not visible_columns:
                    total = int(conn.execute(f"SELECT COUNT(*) AS total FROM {_quote_ident(table_name)}").fetchone()["total"])
                    return {"columns": [], "rows": [], "total": total, "page": page, "page_size": page_size}

                if sort_by:
                    if not is_safe_identifier(sort_by) or sort_by not in visible_columns:
                        return _error(request, code="DBV_INVALID_PARAMS", message="sort_by must be a visible column", status_code=422)
                else:
                    sort_by = visible_columns[0]

                where_parts: list[str] = []
                params: list[Any] = []
                if search and text_columns:
                    like_value = f"%{search}%"
                    where_parts = [f"LOWER({_quote_ident(col)}) LIKE LOWER(?)" for col in sorted(text_columns)]
                    params = [like_value for _ in where_parts]

                where_sql = f" WHERE ({' OR '.join(where_parts)})" if where_parts else ""

                count_sql = f"SELECT COUNT(*) AS total FROM {_quote_ident(table_name)}{where_sql}"
                total = int(conn.execute(count_sql, params).fetchone()["total"])

                select_cols = ", ".join(_quote_ident(col) for col in visible_columns)
                offset = (page - 1) * page_size
                row_sql = (
                    f"SELECT {select_cols} FROM {_quote_ident(table_name)}{where_sql} "
                    f"ORDER BY {_quote_ident(sort_by)} {sort_dir.upper()} LIMIT ? OFFSET ?"
                )
                rows = conn.execute(row_sql, params + [page_size, offset]).fetchall()
            finally:
                conn.close()
        except Exception:
            logger.exception("db_viewer_rows query_failed table=%s request_id=%s", table_name, _request_id(request))
            return _error(request, code="DBV_INTERNAL", message="db viewer internal error", status_code=500)

        logger.info("db_viewer_rows table=%s user=%s request_id=%s", table_name, username, _request_id(request))
        return {"columns": visible_columns, "rows": rows, "total": total, "page": page, "page_size": page_size}

    return router
