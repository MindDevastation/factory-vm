from __future__ import annotations

import base64
import unittest

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from services.common.env import Env
from services.factory_api.security import require_basic_auth


class TestSecurityBasicAuth(unittest.TestCase):
    def _client(self) -> tuple[TestClient, Env]:
        env = Env.load()
        app = FastAPI()

        @app.get("/x")
        def x(_: bool = Depends(require_basic_auth(env))):
            return {"ok": True}

        return TestClient(app), env

    def test_no_auth_header_401(self):
        c, _env = self._client()
        r = c.get("/x")
        self.assertEqual(r.status_code, 401)

    def test_invalid_base64_401(self):
        c, _env = self._client()
        r = c.get("/x", headers={"Authorization": "Basic !!!"})
        self.assertEqual(r.status_code, 401)

    def test_wrong_credentials_401(self):
        c, env = self._client()
        token = base64.b64encode(f"{env.basic_user}:wrong".encode("utf-8")).decode("ascii")
        r = c.get("/x", headers={"Authorization": f"Basic {token}"})
        self.assertEqual(r.status_code, 401)
