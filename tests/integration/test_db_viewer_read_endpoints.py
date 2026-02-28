from __future__ import annotations

import importlib
import os
import sqlite3
import unittest

from fastapi.testclient import TestClient

from services.common.env import Env
from tests._helpers import basic_auth_header, temp_env


class TestDbViewerReadEndpoints(unittest.TestCase):
    def _seed_db(self, env: Env) -> None:
        conn = sqlite3.connect(env.db_path)
        try:
            conn.execute("CREATE TABLE visible_table (id INTEGER, name TEXT, oauth_token TEXT)")
            conn.execute("INSERT INTO visible_table(id, name, oauth_token) VALUES (1, 'Alpha', 'tok1')")
            conn.execute("INSERT INTO visible_table(id, name, oauth_token) VALUES (2, 'Beta', 'tok2')")
            conn.execute("CREATE TABLE token_store (id INTEGER, value TEXT)")
            conn.execute("INSERT INTO token_store(id, value) VALUES (1, 'x')")
            conn.execute("CREATE TABLE vault_data (oauth_token TEXT, api_key TEXT)")
            conn.execute("INSERT INTO vault_data(oauth_token, api_key) VALUES ('a', 'b')")
            conn.commit()
        finally:
            conn.close()

    def test_tables_and_rows_and_request_id_and_filters(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            self._seed_db(env)
            os.environ["DB_VIEWER_POLICY_PATH"] = ""

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            tables = client.get("/v1/db-viewer/tables", headers=auth)
            self.assertEqual(tables.status_code, 200)
            names = [t["name"] for t in tables.json()["tables"]]
            self.assertIn("visible_table", names)
            self.assertNotIn("token_store", names)

            rows = client.get("/v1/db-viewer/tables/visible_table/rows?page=1&page_size=10&search=alp", headers=auth)
            self.assertEqual(rows.status_code, 200)
            body = rows.json()
            self.assertEqual(body["columns"], ["id", "name"])
            self.assertEqual(body["total"], 1)
            self.assertEqual(len(body["rows"]), 1)
            self.assertEqual(body["rows"][0]["name"], "Alpha")

            only_secret = client.get("/v1/db-viewer/tables/vault_data/rows?page=1&page_size=10&search=q", headers=auth)
            self.assertEqual(only_secret.status_code, 200)
            only_secret_body = only_secret.json()
            self.assertEqual(only_secret_body["columns"], [])
            self.assertEqual(only_secret_body["rows"], [])
            self.assertEqual(only_secret_body["total"], 1)

            invalid = client.get(
                "/v1/db-viewer/tables/visible_table/rows?page=0&page_size=11",
                headers={**auth, "X-Request-Id": "req-123"},
            )
            self.assertEqual(invalid.status_code, 422)
            err = invalid.json()["error"]
            self.assertEqual(err["request_id"], "req-123")
            self.assertEqual(err["code"], "DBV_INVALID_PARAMS")

    def test_forbidden_vs_not_found_privileged_and_non_privileged(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            self._seed_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            non_priv = client.get("/v1/db-viewer/tables/token_store/rows?page=1&page_size=10", headers=auth)
            self.assertEqual(non_priv.status_code, 404)
            self.assertEqual(non_priv.json()["error"]["code"], "DBV_TABLE_NOT_FOUND")

            os.environ["DB_VIEWER_PRIVILEGED_USERS"] = env.basic_user
            importlib.reload(mod)
            client = TestClient(mod.app)

            priv_forbidden = client.get("/v1/db-viewer/tables/token_store/rows?page=1&page_size=10", headers=auth)
            self.assertEqual(priv_forbidden.status_code, 403)
            self.assertEqual(priv_forbidden.json()["error"]["code"], "DBV_TABLE_FORBIDDEN")

            missing = client.get("/v1/db-viewer/tables/missing_table/rows?page=1&page_size=10", headers=auth)
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["error"]["code"], "DBV_TABLE_NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
