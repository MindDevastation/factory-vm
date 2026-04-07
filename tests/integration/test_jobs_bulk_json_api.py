from __future__ import annotations

import importlib
import os
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.factory_api.oauth_tokens import oauth_token_path

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class _PreflightOk:
    def __init__(self):
        self.ok = True
        self.resolved = {
            "tracks": [{"file_id": "track1", "filename": "track1.wav"}],
            "background_file_id": "bg1",
            "background_filename": "bg1.png",
            "cover_file_id": "cover1",
            "cover_filename": "cover1.png",
        }


class _PreflightBackgroundFail:
    ok = False
    field_errors = {"background": ["background 'bg.jpg' matches=0"]}
    resolved = {}


class TestJobsBulkJsonApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod, TestClient(mod.app)

    def _create_ui_draft(self, env: Env, title: str) -> int:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title=title,
                description="",
                tags_csv="a,b",
                cover_name="cover",
                cover_ext="png",
                background_name="bg",
                background_ext="jpg",
                audio_ids_text="1",
                job_type="UI",
            )
        finally:
            conn.close()

    def _create_item(self, channel_id: int, title: str = "Bulk") -> dict[str, object]:
        return {
            "channel_id": channel_id,
            "title": title,
            "description": "desc",
            "tags_csv": "one,two",
            "playlist_ids": ["PL_ONE"],
            "audience_is_for_kids": False,
            "video_language": "English",
            "cover_name": "cover",
            "cover_ext": "png",
            "background_name": "bg",
            "background_ext": "jpg",
            "audio_ids_text": "1",
        }

    def _seed_playlist_tracks(self, env: Env) -> None:
        conn = dbm.connect(env)
        try:
            ts = dbm.now_ts()
            for pk, tid, duration, month in [
                (801, "t801", 240.0, "2024-01"),
                (802, "t802", 260.0, "2024-01"),
                (803, "t803", 280.0, "2024-02"),
            ]:
                conn.execute(
                    "INSERT INTO tracks(id, channel_slug, track_id, gdrive_file_id, title, duration_sec, month_batch, discovered_at, analyzed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (pk, "darkwood-reverie", tid, f"g{pk}", f"Track {pk}", duration, month, ts, ts),
                )
                conn.execute(
                    "INSERT INTO track_analysis_flat(track_pk, channel_slug, track_id, analysis_computed_at, analysis_status, duration_sec, yamnet_top_tags_text, voice_flag, speech_flag, dominant_texture, dsp_score, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))",
                    (pk, "darkwood-reverie", tid, ts, "ok", duration, "ambient,calm", 0, 0, "smooth", 0.6),
                )
            conn.commit()
        finally:
            conn.close()

    def test_preview_all_modes_and_examples(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            job_id = self._create_ui_draft(env, "Existing")

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            _, client = self._new_client()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            a_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(a_preview.status_code, 200)
            self.assertEqual(
                a_preview.json(),
                {
                    "mode": "create_draft_jobs",
                    "summary": {"requested": 1, "valid": 1, "failed": 0},
                    "results": [
                        {
                            "index": 0,
                            "valid": True,
                            "metadata": {"playlist_ids": ["PL_ONE"], "playlist_create_title": "", "audience_is_for_kids": False, "video_language": "English"},
                        }
                    ],
                },
            )

            b_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(b_preview.status_code, 200)
            self.assertEqual(b_preview.json()["mode"], "create_and_enqueue")

            c_preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": job_id}]},
            )
            self.assertEqual(c_preview.status_code, 200)
            self.assertEqual(c_preview.json(), {"mode": "enqueue_existing_jobs", "summary": {"requested": 1, "valid": 1, "failed": 0}, "results": [{"job_id": str(job_id), "enqueued": True}]})

    def test_execute_modes_atomicity_and_non_atomic_behavior(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            existing_ok = self._create_ui_draft(env, "enqueue ok")
            existing_bad = self._create_ui_draft(env, "enqueue bad")
            conn = dbm.connect(env)
            try:
                dbm.update_job_state(conn, existing_bad, state="READY_FOR_RENDER", stage="FETCH")
            finally:
                conn.close()

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            conn = dbm.connect(env)
            try:
                before_a = dbm.list_jobs(conn, limit=500)
            finally:
                conn.close()
            a_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id), {"channel_id": channel_id}]},
            )
            self.assertEqual(a_execute.status_code, 200)
            self.assertEqual(a_execute.json()["summary"]["created"], 0)
            conn = dbm.connect(env)
            try:
                after_a = dbm.list_jobs(conn, limit=500)
            finally:
                conn.close()
            self.assertEqual(len(before_a), len(after_a))

            b_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id, title="B1")]},
            )
            self.assertEqual(b_execute.status_code, 200)
            b_payload = b_execute.json()
            self.assertEqual(b_payload["mode"], "create_and_enqueue")
            self.assertEqual(b_payload["summary"], {"requested": 1, "created": 1, "enqueued": 1, "noop": 0, "failed": 0})
            created_job_id = int(b_payload["results"][0]["job_id"])
            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, created_job_id)
                self.assertEqual(job["state"], "READY_FOR_RENDER")
            finally:
                conn.close()

            b_fail = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id), {"channel_id": channel_id}]},
            )
            self.assertEqual(b_fail.status_code, 200)
            self.assertEqual(b_fail.json()["summary"]["created"], 0)

            c_execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": existing_ok}, {"job_id": existing_bad}]},
            )
            self.assertEqual(c_execute.status_code, 200)
            self.assertEqual(c_execute.json(), {
                "mode": "enqueue_existing_jobs",
                "summary": {"requested": 2, "enqueued": 1, "noop": 0, "failed": 1},
                "results": [
                    {"job_id": str(existing_ok), "enqueued": True},
                    {"job_id": str(existing_bad), "error": {"code": "UIJ_RENDER_NOT_ALLOWED", "message": "Status not allowed"}},
                ],
            })

    def test_bulk_playlist_builder_preview_and_execute_for_create_modes(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            self._seed_playlist_tracks(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()

            h = basic_auth_header(env.basic_user, env.basic_pass)

            preview_ok = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={
                    "mode": "create_draft_jobs",
                    "items": [
                        {
                            **self._create_item(channel_id, title="PB Preview"),
                            "playlist_builder": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15},
                        }
                    ],
                },
            )
            self.assertEqual(preview_ok.status_code, 200)
            payload_preview_ok = preview_ok.json()
            self.assertEqual(payload_preview_ok["summary"], {"requested": 1, "valid": 1, "failed": 0})
            self.assertTrue(payload_preview_ok["results"][0]["playlist_builder"]["ok"])
            self.assertIsInstance(payload_preview_ok["results"][0]["playlist_builder"].get("summary"), dict)

    def test_bulk_execute_rolls_back_when_background_preflight_fails(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightBackgroundFail()

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["summary"]["created"], 0)
            self.assertEqual(payload["summary"]["failed"], 1)
            self.assertIn("background", payload["results"][0]["error"]["field_errors"])

            conn2 = dbm.connect(env)
            try:
                total = int(conn2.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"])
                self.assertEqual(total, 0)
            finally:
                conn2.close()

    def test_bulk_preview_execute_persists_metadata_fields_and_keeps_compat_defaults(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)

            preview = client.post(
                "/v1/ui/jobs/bulk-json/preview",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["summary"]["valid"], 1)
            self.assertEqual(preview.json()["results"][0]["metadata"]["playlist_ids"], ["PL_ONE"])
            self.assertEqual(preview.json()["results"][0]["metadata"]["playlist_create_title"], "")
            self.assertEqual(preview.json()["results"][0]["metadata"]["audience_is_for_kids"], False)
            self.assertEqual(preview.json()["results"][0]["metadata"]["video_language"], "English")

            execute = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [self._create_item(channel_id)]},
            )
            self.assertEqual(execute.status_code, 200)
            self.assertEqual(execute.json()["results"][0]["metadata"]["playlist_ids"], ["PL_ONE"])
            self.assertEqual(execute.json()["results"][0]["metadata"]["playlist_create_title"], "")
            created_job_id = int(execute.json()["results"][0]["job_id"])

            conn2 = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn2, created_job_id)
                self.assertEqual(draft["playlists_json"], '["PL_ONE"]')
                self.assertEqual(int(draft["audience_is_for_kids"]), 0)
                self.assertEqual(draft["video_language"], "English")
            finally:
                conn2.close()

            compat_item = self._create_item(channel_id)
            compat_item.pop("playlist_ids")
            compat_item.pop("audience_is_for_kids")
            compat_item.pop("video_language")
            execute_compat = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_draft_jobs", "items": [compat_item]},
            )
            self.assertEqual(execute_compat.status_code, 200)
            self.assertEqual(execute_compat.json()["results"][0]["metadata"]["playlist_ids"], [])
            self.assertEqual(execute_compat.json()["results"][0]["metadata"]["playlist_create_title"], "")
            compat_job_id = int(execute_compat.json()["results"][0]["job_id"])
            conn3 = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn3, compat_job_id)
                self.assertEqual(draft["playlists_json"], "[]")
                self.assertEqual(int(draft["audience_is_for_kids"]), 0)
                self.assertEqual(draft["video_language"], "English")
            finally:
                conn3.close()

    def test_bulk_create_accepts_playlist_create_title_and_union_metadata(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            h = basic_auth_header(env.basic_user, env.basic_pass)
            item = self._create_item(channel_id)
            item["playlist_create_title"] = "  Fresh Mix  "
            preview = client.post("/v1/ui/jobs/bulk-json/preview", headers=h, json={"mode": "create_draft_jobs", "items": [item]})
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["results"][0]["metadata"]["playlist_ids"], ["PL_ONE"])
            self.assertEqual(preview.json()["results"][0]["metadata"]["playlist_create_title"], "Fresh Mix")

            execute = client.post("/v1/ui/jobs/bulk-json/execute", headers=h, json={"mode": "create_draft_jobs", "items": [item]})
            self.assertEqual(execute.status_code, 200)
            created_job_id = int(execute.json()["results"][0]["job_id"])
            conn2 = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn2, created_job_id)
                self.assertEqual(str(draft["playlist_create_title"]), "Fresh Mix")
            finally:
                conn2.close()
    def test_execute_mode_c_runtime_exception_converted_to_item_error(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            ok_job = self._create_ui_draft(env, "mode c ok")
            boom_job = self._create_ui_draft(env, "mode c boom")

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()

            original_render_selected = mod._render_selected_item

            def _raise_for_specific_job(job_id_text: str):
                if int(job_id_text) == boom_job:
                    raise RuntimeError("unexpected runtime crash")
                return original_render_selected(job_id_text)

            mod._render_selected_item = _raise_for_specific_job

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "enqueue_existing_jobs", "items": [{"job_id": ok_job}, {"job_id": boom_job}]},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {
                "mode": "enqueue_existing_jobs",
                "summary": {"requested": 2, "enqueued": 1, "noop": 0, "failed": 1},
                "results": [
                    {"job_id": str(ok_job), "enqueued": True},
                    {"job_id": str(boom_job), "error": {"code": "UIJ_INTERNAL", "message": "Internal error"}},
                ],
            })

    def test_execute_mode_b_runtime_exception_converted_to_enqueue_item_error(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn.close()

            token_path = oauth_token_path(base_dir=Env.load().gdrive_tokens_dir, channel_slug="darkwood-reverie")
            token_path.parent.mkdir(parents=True, exist_ok=True)
            token_path.write_text("{}", encoding="utf-8")

            mod, client = self._new_client()
            mod._create_drive_client = lambda _env: object()
            mod.run_preflight_for_job = lambda conn, _env, _job_id, drive: _PreflightOk()
            mod._render_selected_item = lambda _job_id_text: (_ for _ in ()).throw(RuntimeError("enqueue exploded"))

            h = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(
                "/v1/ui/jobs/bulk-json/execute",
                headers=h,
                json={"mode": "create_and_enqueue", "items": [self._create_item(channel_id, title="B crash")]},
            )
            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertEqual(payload["mode"], "create_and_enqueue")
            self.assertEqual(payload["summary"], {"requested": 1, "created": 1, "enqueued": 0, "noop": 0, "failed": 1})
            self.assertEqual(payload["results"][0]["enqueue"], {"job_id": payload["results"][0]["job_id"], "error": {"code": "UIJ_INTERNAL", "message": "Internal error"}})

            created_job_id = int(payload["results"][0]["job_id"])
            conn = dbm.connect(env)
            try:
                job = dbm.get_job(conn, created_job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job["state"], "DRAFT")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
