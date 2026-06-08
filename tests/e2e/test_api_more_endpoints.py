from __future__ import annotations

import importlib
import json
import unittest
from unittest import mock
from pathlib import Path

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.common.paths import logs_path, qa_path

from tests._helpers import temp_env, seed_minimal_db, insert_release_and_job, basic_auth_header


class TestApiMoreEndpoints(unittest.TestCase):
    def _mock_oauth_auth_url(self, captured: list[dict]):
        def _side_effect(**kwargs):
            captured.append(dict(kwargs))
            return f"https://accounts.google.com/auth?state={kwargs['state']}"

        return _side_effect

    def _write_oauth_verifier(self, mod, env: Env, *, state: str, kind: str, verifier: str, require_channel_slug: bool = True) -> Path:
        payload = mod.verify_state(
            secret="state-secret",
            expected_kind=kind,
            state=state,
            require_channel_slug=require_channel_slug,
        )
        nonce = str(payload["nonce"])
        verifier_path = Path(env.storage_root) / "tmp" / "oauth" / f"{nonce}.code_verifier"
        verifier_path.parent.mkdir(parents=True, exist_ok=True)
        verifier_path.write_text(verifier, encoding="utf-8")
        return verifier_path

    def test_health_workers_logs_qa(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            # seed a worker heartbeat
            conn = dbm.connect(env)
            try:
                dbm.touch_worker(conn, worker_id="orchestrator:1", role="orchestrator", pid=1, hostname="h", details={"x": 1})
            finally:
                conn.close()

            job_id = insert_release_and_job(env, state="WAIT_APPROVAL", stage="APPROVAL")

            # Create job log + qa file
            logs_path(env, job_id).parent.mkdir(parents=True, exist_ok=True)
            logs_path(env, job_id).write_text("line1\nline2\n", encoding="utf-8")

            qa_path(env, job_id).parent.mkdir(parents=True, exist_ok=True)
            qa_path(env, job_id).write_text(json.dumps({"hard_ok": True}), encoding="utf-8")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/health")
            self.assertEqual(r.status_code, 200)
            self.assertTrue(r.json().get("ok"))

            rw = client.get("/v1/workers", headers=h)
            self.assertEqual(rw.status_code, 200)
            self.assertTrue(rw.json()["workers"])

            rl = client.get(f"/v1/jobs/{job_id}/logs?tail=1", headers=h)
            self.assertEqual(rl.status_code, 200)
            self.assertIn("line2", rl.text)

            rq = client.get(f"/v1/jobs/{job_id}/qa", headers=h)
            self.assertEqual(rq.status_code, 200)
            self.assertEqual(rq.json()["qa"]["hard_ok"], True)

            rj = client.get(f"/v1/jobs/{job_id}", headers=h)
            self.assertEqual(rj.status_code, 200)
            self.assertEqual(int(rj.json()["job"]["id"]), job_id)

    def test_channels_requires_auth_and_returns_schema(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            insert_release_and_job(env, channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/v1/channels")
            self.assertIn(unauthorized.status_code, (401, 403))

            authorized = client.get("/v1/channels", headers=h)
            self.assertEqual(authorized.status_code, 200)
            channels = authorized.json()
            self.assertIsInstance(channels, list)
            self.assertGreater(len(channels), 0)
            for item in channels:
                self.assertIsInstance(item, dict)
                self.assertIn("id", item)
                self.assertIn("slug", item)
                self.assertIn("display_name", item)

            display_names = [str(item["display_name"]) for item in channels]
            self.assertEqual(display_names, sorted(display_names))


    def test_channels_export_yaml_requires_auth_and_contains_slug_display_name(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.get("/v1/channels/export/yaml")
            self.assertIn(unauthorized.status_code, (401, 403))

            authorized = client.get("/v1/channels/export/yaml", headers=h)
            self.assertEqual(authorized.status_code, 200)
            self.assertIn("text/plain", authorized.headers.get("content-type", ""))
            body = authorized.text

            self.assertIn("channels:", body)
            self.assertIn('slug: darkwood-reverie', body)
            self.assertIn('display_name: Darkwood Reverie', body)
            self.assertNotIn('yt_token_json_path', body)
            self.assertNotIn('yt_client_secret_json_path', body)


    def test_create_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.post(
                "/v1/channels",
                json={"slug": "new-channel", "display_name": "New Channel"},
            )
            self.assertIn(unauthorized.status_code, (401, 403))

            invalid_slug = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "bad_slug", "display_name": "Valid Name"},
            )
            self.assertEqual(invalid_slug.status_code, 422)

            invalid_display_name = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "good-slug", "display_name": "   "},
            )
            self.assertEqual(invalid_display_name.status_code, 422)

            created = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "new-channel", "display_name": "New Channel"},
            )
            self.assertEqual(created.status_code, 200)
            body = created.json()
            self.assertIsInstance(body.get("id"), int)
            self.assertEqual(body.get("slug"), "new-channel")
            self.assertEqual(body.get("display_name"), "New Channel")

            duplicate = client.post(
                "/v1/channels",
                headers=h,
                json={"slug": "new-channel", "display_name": "Another Name"},
            )
            self.assertEqual(duplicate.status_code, 409)

    def test_update_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.patch(
                "/v1/channels/darkwood-reverie",
                json={"display_name": "Darkwood Updated"},
            )
            self.assertIn(unauthorized.status_code, (401, 403))

            invalid_display_name = client.patch(
                "/v1/channels/darkwood-reverie",
                headers=h,
                json={"display_name": "   "},
            )
            self.assertEqual(invalid_display_name.status_code, 422)

            not_found = client.patch(
                "/v1/channels/missing-channel",
                headers=h,
                json={"display_name": "Darkwood Updated"},
            )
            self.assertEqual(not_found.status_code, 404)

            updated = client.patch(
                "/v1/channels/darkwood-reverie",
                headers=h,
                json={"display_name": "Darkwood Updated"},
            )
            self.assertEqual(updated.status_code, 200)
            body = updated.json()
            self.assertEqual(body.get("slug"), "darkwood-reverie")
            self.assertEqual(body.get("display_name"), "Darkwood Updated")


    def test_delete_channel_endpoint(self) -> None:
        with temp_env() as (_, _env0):
            env = Env.load()
            seed_minimal_db(env)
            insert_release_and_job(env, channel_slug="darkwood-reverie")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.delete("/v1/channels/channel-c")
            self.assertIn(unauthorized.status_code, (401, 403))

            not_found = client.delete("/v1/channels/missing-channel", headers=h)
            self.assertEqual(not_found.status_code, 404)

            with_job = client.delete("/v1/channels/darkwood-reverie", headers=h)
            self.assertEqual(with_job.status_code, 409)
            self.assertIn("jobs exist", with_job.json().get("detail", ""))

            no_jobs = client.delete("/v1/channels/channel-c", headers=h)
            self.assertEqual(no_jobs.status_code, 200)
            body = no_jobs.json()
            self.assertEqual(body.get("ok"), True)
            self.assertEqual(body.get("slug"), "channel-c")

            conn = dbm.connect(env)
            try:
                self.assertIsNone(dbm.get_channel_by_slug(conn, "channel-c"))
            finally:
                conn.close()

    def test_oauth_endpoints_require_auth_and_validate_channel(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            unauthorized = client.post("/v1/oauth/gdrive/darkwood-reverie/start")
            self.assertIn(unauthorized.status_code, (401, 403))

            captured: list[dict] = []
            with mock.patch("services.factory_api.app.build_authorization_url", side_effect=self._mock_oauth_auth_url(captured)):
                authorized = client.post("/v1/oauth/gdrive/darkwood-reverie/start", headers=h)
            self.assertEqual(authorized.status_code, 200)
            self.assertIn("auth_url", authorized.json())
            self.assertNotIn("code_verifier", authorized.json())
            gdrive_start = captured[-1]
            gdrive_payload = mod.verify_state(secret="state-secret", expected_kind="gdrive", state=gdrive_start["state"])
            gdrive_verifier = Path(env.storage_root) / "tmp" / "oauth" / f"{gdrive_payload['nonce']}.code_verifier"
            self.assertTrue(gdrive_verifier.is_file())
            self.assertEqual(gdrive_verifier.read_text(encoding="utf-8"), gdrive_start["code_verifier"])
            self.assertNotIn(gdrive_start["code_verifier"], authorized.text)

            with mock.patch("services.factory_api.app.build_authorization_url", side_effect=self._mock_oauth_auth_url(captured)):
                authorized_yt = client.post("/v1/oauth/youtube/darkwood-reverie/start", headers=h)
            self.assertEqual(authorized_yt.status_code, 200)
            self.assertNotIn("code_verifier", authorized_yt.json())
            yt_start = captured[-1]
            yt_payload = mod.verify_state(secret="state-secret", expected_kind="youtube", state=yt_start["state"])
            yt_verifier = Path(env.storage_root) / "tmp" / "oauth" / f"{yt_payload['nonce']}.code_verifier"
            self.assertTrue(yt_verifier.is_file())
            self.assertEqual(yt_verifier.read_text(encoding="utf-8"), yt_start["code_verifier"])
            self.assertNotIn(yt_start["code_verifier"], authorized_yt.text)

            with mock.patch("services.factory_api.app.build_authorization_url", side_effect=self._mock_oauth_auth_url(captured)):
                authorized_global = client.post("/v1/oauth/gdrive_global/start", headers=h)
            self.assertEqual(authorized_global.status_code, 200)
            self.assertIn("auth_url", authorized_global.json())
            self.assertNotIn("code_verifier", authorized_global.json())
            global_start = captured[-1]
            global_payload = mod.verify_state(secret="state-secret", expected_kind="gdrive_global", state=global_start["state"], require_channel_slug=False)
            global_verifier = Path(env.storage_root) / "tmp" / "oauth" / f"{global_payload['nonce']}.code_verifier"
            self.assertTrue(global_verifier.is_file())
            self.assertEqual(global_verifier.read_text(encoding="utf-8"), global_start["code_verifier"])
            self.assertNotIn(global_start["code_verifier"], authorized_global.text)

            missing = client.post("/v1/oauth/gdrive/missing-channel/start", headers=h)
            self.assertEqual(missing.status_code, 404)

            callback_unauthorized = client.get("/v1/oauth/gdrive/callback?code=fake&state=fake")
            self.assertIn(callback_unauthorized.status_code, (401, 403))

    def test_oauth_callback_writes_channel_tokens_with_mocked_exchange(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            state_gdrive = mod.sign_state(secret="state-secret", kind="gdrive", channel_slug="darkwood-reverie")
            gdrive_verifier = self._write_oauth_verifier(mod, env, state=state_gdrive, kind="gdrive", verifier="gdrive-verifier")
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"gdrive-token"}') as exchange:
                rg = client.get(f"/v1/oauth/gdrive/callback?code=fake-code&state={state_gdrive}", headers=h)
            self.assertEqual(rg.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "gdrive-verifier")
            self.assertFalse(gdrive_verifier.exists())
            self.assertNotIn("gdrive-verifier", rg.text)
            gdrive_token = Path(td.name) / "gdrive_tokens" / "darkwood-reverie" / "token.json"
            self.assertTrue(gdrive_token.is_file())
            self.assertIn("gdrive-token", gdrive_token.read_text(encoding="utf-8"))

            state_yt = mod.sign_state(secret="state-secret", kind="youtube", channel_slug="darkwood-reverie")
            yt_verifier = self._write_oauth_verifier(mod, env, state=state_yt, kind="youtube", verifier="yt-verifier")
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"yt-token"}') as exchange:
                ry = client.get(f"/v1/oauth/youtube/callback?code=fake-code&state={state_yt}", headers=h)
            self.assertEqual(ry.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "yt-verifier")
            self.assertFalse(yt_verifier.exists())
            self.assertNotIn("yt-verifier", ry.text)
            yt_token = Path(td.name) / "yt_tokens" / "darkwood-reverie" / "token.json"
            self.assertTrue(yt_token.is_file())
            self.assertIn("yt-token", yt_token.read_text(encoding="utf-8"))

            missing_state = mod.sign_state(secret="state-secret", kind="youtube", channel_slug="darkwood-reverie")
            missing = client.get(f"/v1/oauth/youtube/callback?code=fake-code&state={missing_state}", headers=h)
            self.assertEqual(missing.status_code, 400)
            self.assertEqual(missing.json().get("detail"), "oauth session expired; start authorization again")

    def test_oauth_global_gdrive_callback_writes_global_token_path(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")
            os.environ["GDRIVE_OAUTH_TOKEN_JSON"] = str(Path(td.name) / "secure" / "gdrive_token.json")
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            state = mod.sign_state(secret="state-secret", kind="gdrive_global")
            verifier_path = self._write_oauth_verifier(
                mod,
                env,
                state=state,
                kind="gdrive_global",
                verifier="global-gdrive-verifier",
                require_channel_slug=False,
            )
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"global-gdrive-token"}') as exchange:
                response = client.get(f"/v1/oauth/gdrive_global/callback?code=fake-code&state={state}", headers=h)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "global-gdrive-verifier")
            self.assertFalse(verifier_path.exists())
            self.assertNotIn("global-gdrive-verifier", response.text)

            token_path = Path(td.name) / "secure" / "gdrive_token.json"
            self.assertTrue(token_path.is_file())
            self.assertIn("global-gdrive-token", token_path.read_text(encoding="utf-8"))

    def test_oauth_status_reports_presence_without_reading_contents(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            gdrive_token = Path(td.name) / "gdrive_tokens" / "darkwood-reverie" / "token.json"
            gdrive_token.parent.mkdir(parents=True, exist_ok=True)
            gdrive_token.write_text('{"access_token":"x"}', encoding="utf-8")

            res = client.get("/v1/oauth/status", headers=h)
            self.assertEqual(res.status_code, 200)
            channels = res.json().get("channels", [])
            item = next((row for row in channels if row.get("slug") == "darkwood-reverie"), None)
            self.assertIsNotNone(item)
            assert item is not None
            self.assertEqual(item["drive_token_present"], True)
            self.assertIsNotNone(item["drive_token_mtime"])
            self.assertEqual(item["yt_token_present"], False)
            self.assertIsNone(item["yt_token_mtime"])


    def test_youtube_add_channel_oauth_multi_select_and_confirm_deletes_temp_token(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            captured: list[dict] = []
            with mock.patch("services.factory_api.app.build_authorization_url", side_effect=self._mock_oauth_auth_url(captured)):
                start = client.post("/v1/oauth/youtube/add_channel/start", headers=h)
            self.assertEqual(start.status_code, 200)
            self.assertIn("auth_url", start.json())
            self.assertNotIn("code_verifier", start.json())
            start_kwargs = captured[-1]
            start_payload = mod.verify_state(secret="state-secret", expected_kind="youtube_add_channel", state=start_kwargs["state"], require_channel_slug=False)
            start_verifier = Path(env.storage_root) / "tmp" / "oauth" / f"{start_payload['nonce']}.code_verifier"
            self.assertTrue(start_verifier.is_file())
            self.assertNotIn(start_kwargs["code_verifier"], start.text)

            state = mod.sign_state(secret="state-secret", kind="youtube_add_channel")
            verifier_path = self._write_oauth_verifier(
                mod,
                env,
                state=state,
                kind="youtube_add_channel",
                verifier="add-channel-verifier",
                require_channel_slug=False,
            )
            channels = [
                {"id": "UC111", "title": "Brand Channel One"},
                {"id": "UC222", "title": "Brand Channel Two"},
            ]
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"yt-token"}') as exchange:
                with mock.patch("services.factory_api.app._youtube_channels_from_token_json", return_value=channels):
                    cb = client.get(f"/v1/oauth/youtube/add_channel/callback?code=fake-code&state={state}", headers=h)
            self.assertEqual(cb.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "add-channel-verifier")
            self.assertFalse(verifier_path.exists())
            self.assertIn("Select YouTube Channel", cb.text)
            self.assertNotIn("add-channel-verifier", cb.text)

            m = __import__("re").search(r"name='state' value='([^']+)'", cb.text)
            self.assertIsNotNone(m)
            confirm_state = m.group(1)

            payload = mod.verify_state(secret="state-secret", expected_kind="youtube_add_channel_confirm", state=confirm_state, require_channel_slug=False)
            nonce = str(payload["nonce"])
            tmp_token = Path(env.storage_root) / "tmp" / "oauth" / f"{nonce}.json"
            self.assertTrue(tmp_token.is_file())

            with mock.patch("services.factory_api.app._youtube_channels_from_token_json", return_value=channels):
                confirm = client.post(
                    "/v1/oauth/youtube/add_channel/confirm",
                    params={"state": confirm_state, "youtube_channel_id": "UC222"},
                    headers=h,
                )
            self.assertEqual(confirm.status_code, 200)
            self.assertIn("Channel connected", confirm.text)
            self.assertFalse(tmp_token.exists())

            conn = dbm.connect(env)
            try:
                row = dbm.get_channel_by_youtube_channel_id(conn, "UC222")
                self.assertIsNotNone(row)
                slug = str(row["slug"])
            finally:
                conn.close()

            token_path = Path(td.name) / "yt_tokens" / slug / "token.json"
            self.assertTrue(token_path.is_file())

    def test_youtube_add_channel_dedup_and_slug_suffix(self) -> None:
        with temp_env() as (td, _env0):
            env = Env.load()
            seed_minimal_db(env)

            import os
            os.environ["OAUTH_REDIRECT_BASE_URL"] = "http://localhost:8080"
            os.environ["OAUTH_STATE_SECRET"] = "state-secret"
            os.environ["YT_CLIENT_SECRET_JSON"] = str(Path(td.name) / "yt_client.json")
            os.environ["YT_TOKENS_DIR"] = str(Path(td.name) / "yt_tokens")
            os.environ["GDRIVE_CLIENT_SECRET_JSON"] = str(Path(td.name) / "gdrive_client.json")
            os.environ["GDRIVE_TOKENS_DIR"] = str(Path(td.name) / "gdrive_tokens")

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            conn = dbm.connect(env)
            try:
                dbm.create_channel(conn, slug="brand-channel", display_name="Brand Channel")
            finally:
                conn.close()

            state = mod.sign_state(secret="state-secret", kind="youtube_add_channel")
            first_verifier = self._write_oauth_verifier(
                mod, env, state=state, kind="youtube_add_channel", verifier="first-add-verifier", require_channel_slug=False
            )
            channels = [{"id": "UCX", "title": "Brand Channel"}]
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"yt-token"}') as exchange:
                with mock.patch("services.factory_api.app._youtube_channels_from_token_json", return_value=channels):
                    first = client.get(f"/v1/oauth/youtube/add_channel/callback?code=fake-code&state={state}", headers=h)
            self.assertEqual(first.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "first-add-verifier")
            self.assertFalse(first_verifier.exists())
            self.assertIn("brand-channel-2", first.text)

            state2 = mod.sign_state(secret="state-secret", kind="youtube_add_channel")
            second_verifier = self._write_oauth_verifier(
                mod, env, state=state2, kind="youtube_add_channel", verifier="second-add-verifier", require_channel_slug=False
            )
            with mock.patch("services.factory_api.app.exchange_code_for_token_json", return_value='{"access_token":"yt-token-2"}') as exchange:
                with mock.patch("services.factory_api.app._youtube_channels_from_token_json", return_value=channels):
                    second = client.get(f"/v1/oauth/youtube/add_channel/callback?code=fake-code&state={state2}", headers=h)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(exchange.call_args.kwargs["code_verifier"], "second-add-verifier")
            self.assertFalse(second_verifier.exists())
            self.assertIn("already connected", second.text.lower())


if __name__ == "__main__":
    unittest.main()
