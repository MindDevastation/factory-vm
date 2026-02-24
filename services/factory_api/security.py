from __future__ import annotations

import base64
import secrets
from fastapi import Depends, HTTPException, Request, status

from services.common.env import Env


def require_basic_auth(env: Env):
    async def _dep(request: Request):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Basic "):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, headers={"WWW-Authenticate": "Basic"})
        try:
            raw = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
            user, pwd = raw.split(":", 1)
        except Exception:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, headers={"WWW-Authenticate": "Basic"})
        if not (secrets.compare_digest(user, env.basic_user) and secrets.compare_digest(pwd, env.basic_pass)):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, headers={"WWW-Authenticate": "Basic"})
        return True

    return _dep
