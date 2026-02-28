from __future__ import annotations

import importlib
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestDbViewerPolicyEndpoints(unittest.TestCase):
    def _seed_db(self, env: Env) -> None:
        conn = sqlite3.connect(env.db_path)
        try:
            conn.execute("CREATE TABLE channels (id INTEGER, slug TEXT)")
            conn.execute("INSERT INTO channels(id, slug) VALUES (1, 'alpha')")
            conn.execute("CREATE TABLE visible_table (id INTEGER, name TEXT)")
            conn.execute("INSERT INTO visible_table(id, name) VALUES (1, 'ok')")
            conn.commit()
        finally:
            conn.close()

    def test_put_get_policy_and_denylist_effect_on_tables_and_rows(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            self._seed_db(env)
            with tempfile.TemporaryDirectory() as td:
                os.environ["DB_VIEWER_POLICY_PATH"] = str(Path(td) / "dbv_policy.json")
                os.environ["DB_VIEWER_PRIVILEGED_USERS"] = env.basic_user

                mod = importlib.import_module("services.factory_api.app")
                importlib.reload(mod)
                client = TestClient(mod.app)
                auth = basic_auth_header(env.basic_user, env.basic_pass)

                put_resp = client.put(
                    "/v1/db-viewer/policy",
                    headers=auth,
                    json={"denylist_tables": ["channels"], "human_name_overrides": {"visible_table": "Visible"}},
                )
                self.assertEqual(put_resp.status_code, 200)
                self.assertEqual(put_resp.json()["denylist_tables"], ["channels"])

                get_resp = client.get("/v1/db-viewer/policy", headers=auth)
                self.assertEqual(get_resp.status_code, 200)
                self.assertEqual(get_resp.json(), put_resp.json())

                tables = client.get("/v1/db-viewer/tables", headers=auth)
                self.assertEqual(tables.status_code, 200)
                names = [row["table_name"] for row in tables.json()["tables"]]
                self.assertIn("visible_table", names)
                self.assertNotIn("channels", names)

                non_priv_env_user = "reader"
                os.environ["FACTORY_BASIC_AUTH_USER"] = non_priv_env_user
                os.environ["FACTORY_BASIC_AUTH_PASS"] = "reader-pass"
                importlib.reload(mod)
                client_non_priv = TestClient(mod.app)
                non_priv_auth = basic_auth_header(non_priv_env_user, "reader-pass")

                non_priv_rows = client_non_priv.get("/v1/db-viewer/tables/channels/rows?page=1&page_size=10", headers=non_priv_auth)
                self.assertEqual(non_priv_rows.status_code, 404)
                self.assertEqual(non_priv_rows.json()["error"]["code"], "DBV_TABLE_NOT_FOUND")

                os.environ["FACTORY_BASIC_AUTH_USER"] = env.basic_user
                os.environ["FACTORY_BASIC_AUTH_PASS"] = env.basic_pass
                importlib.reload(mod)
                client_priv = TestClient(mod.app)

                priv_rows = client_priv.get("/v1/db-viewer/tables/channels/rows?page=1&page_size=10", headers=auth)
                self.assertEqual(priv_rows.status_code, 403)
                self.assertEqual(priv_rows.json()["error"]["code"], "DBV_TABLE_FORBIDDEN")

    def test_policy_endpoints_require_privilege_and_config(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            self._seed_db(env)
            os.environ["DB_VIEWER_POLICY_PATH"] = ""
            os.environ["DB_VIEWER_PRIVILEGED_USERS"] = env.basic_user

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            not_configured = client.get("/v1/db-viewer/policy", headers=auth)
            self.assertEqual(not_configured.status_code, 500)
            self.assertEqual(not_configured.json()["error"]["code"], "DBV_POLICY_ERROR")
            self.assertEqual(not_configured.json()["error"]["message"], "Policy storage is not configured")

            with tempfile.TemporaryDirectory() as td:
                os.environ["DB_VIEWER_POLICY_PATH"] = str(Path(td) / "policy.json")
                os.environ["DB_VIEWER_PRIVILEGED_USERS"] = "someone-else"
                importlib.reload(mod)
                client = TestClient(mod.app)

                forbidden = client.put(
                    "/v1/db-viewer/policy",
                    headers=auth,
                    json={"denylist_tables": ["channels"], "human_name_overrides": {}},
                )
                self.assertEqual(forbidden.status_code, 403)
                self.assertEqual(forbidden.json()["error"]["code"], "DBV_POLICY_FORBIDDEN")

    def test_put_policy_validation_errors(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            self._seed_db(env)
            with tempfile.TemporaryDirectory() as td:
                os.environ["DB_VIEWER_POLICY_PATH"] = str(Path(td) / "policy.json")
                os.environ["DB_VIEWER_PRIVILEGED_USERS"] = env.basic_user

                mod = importlib.import_module("services.factory_api.app")
                importlib.reload(mod)
                client = TestClient(mod.app)
                auth = basic_auth_header(env.basic_user, env.basic_pass)

                invalid = client.put(
                    "/v1/db-viewer/policy",
                    headers=auth,
                    json={"denylist_tables": ["channels", "channels"], "human_name_overrides": {}},
                )
                self.assertEqual(invalid.status_code, 400)
                self.assertEqual(invalid.json()["error"]["code"], "DBV_POLICY_INVALID")


if __name__ == "__main__":
    unittest.main()
