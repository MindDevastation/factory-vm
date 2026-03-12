from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlaylistBuilderApi(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _create_ui_draft(self, *, channel_slug: str, title: str) -> int:
        conn = dbm.connect(self.env)
        try:
            ch = dbm.get_channel_by_slug(conn, channel_slug)
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title=title,
                description="",
                tags_csv="",
                cover_name=None,
                cover_ext=None,
                background_name="bg",
                background_ext="jpg",
                audio_ids_text="1",
                job_type="UI",
            )
        finally:
            conn.close()

    def test_channel_settings_put_and_get(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            missing = client.get("/v1/playlist-builder/channels/darkwood-reverie/settings", headers=headers)
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["error"]["code"], "PLB_CHANNEL_SETTINGS_NOT_FOUND")

            put_resp = client.put(
                "/v1/playlist-builder/channels/darkwood-reverie/settings",
                headers=headers,
                json={
                    "generation_mode": "safe",
                    "min_duration_min": 45,
                    "max_duration_min": 90,
                    "tolerance_min": 10,
                    "vocal_policy": "exclude_speech",
                },
            )
            self.assertEqual(put_resp.status_code, 200)
            payload = put_resp.json()
            self.assertEqual(payload["settings"]["generation_mode"], "safe")
            self.assertEqual(payload["settings"]["min_duration_min"], 45)
            self.assertEqual(payload["settings"]["max_duration_min"], 90)
            self.assertEqual(payload["settings"]["tolerance_min"], 10)
            self.assertEqual(payload["settings"]["vocal_policy"], "exclude_speech")

            get_resp = client.get("/v1/playlist-builder/channels/darkwood-reverie/settings", headers=headers)
            self.assertEqual(get_resp.status_code, 200)
            self.assertEqual(get_resp.json(), payload)

    def test_channel_settings_put_rejects_unknown_field(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            resp = client.put(
                "/v1/playlist-builder/channels/darkwood-reverie/settings",
                headers=headers,
                json={"generation_mode": "safe", "unexpected_field": True},
            )
            self.assertEqual(resp.status_code, 422)

    def test_job_override_patch_rejects_unknown_field(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            resp = client.patch(
                f"/v1/ui/jobs/{job_id}/playlist-builder/override",
                headers=headers,
                json={"generation_mode": "curated", "unexpected_field": 123},
            )
            self.assertEqual(resp.status_code, 422)

    def test_job_brief_rejects_stored_override_with_unknown_field(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            conn = dbm.connect(self.env)
            try:
                dbm.update_ui_job_playlist_builder_override_json(
                    conn,
                    job_id=job_id,
                    playlist_builder_override_json='{"generation_mode":"safe","unknown_key":"x"}',
                )
                conn.commit()
            finally:
                conn.close()

            resp = client.get(f"/v1/playlist-builder/jobs/{job_id}/brief", headers=headers)
            self.assertEqual(resp.status_code, 422)
            self.assertEqual(resp.json()["error"]["code"], "PLB_INVALID_BRIEF")

    def test_job_override_patch_merges_with_existing_override(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            first = client.patch(
                f"/v1/ui/jobs/{job_id}/playlist-builder/override",
                headers=headers,
                json={"generation_mode": "curated"},
            )
            self.assertEqual(first.status_code, 200)
            self.assertEqual(first.json()["override"]["generation_mode"], "curated")

            second = client.patch(
                f"/v1/ui/jobs/{job_id}/playlist-builder/override",
                headers=headers,
                json={"max_duration_min": 75},
            )
            self.assertEqual(second.status_code, 200)
            self.assertEqual(second.json()["override"]["generation_mode"], "curated")
            self.assertEqual(second.json()["override"]["max_duration_min"], 75)

            brief_resp = client.get(f"/v1/playlist-builder/jobs/{job_id}/brief", headers=headers)
            self.assertEqual(brief_resp.status_code, 200)
            brief = brief_resp.json()["brief"]
            self.assertEqual(brief["generation_mode"], "curated")
            self.assertEqual(brief["max_duration_min"], 75)

    def test_job_brief_resolution_and_invalid_override(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            conn = dbm.connect(self.env)
            try:
                dbm.upsert_playlist_builder_channel_settings(
                    conn,
                    channel_slug="darkwood-reverie",
                    default_generation_mode="safe",
                    min_duration_min=30,
                    max_duration_min=70,
                    tolerance_min=5,
                    preferred_month_batch="2024-01",
                    preferred_batch_ratio=61,
                    allow_cross_channel=False,
                    novelty_target_min=0.55,
                    novelty_target_max=0.75,
                    position_memory_window=9,
                    strictness_mode="flexible",
                    vocal_policy="allow_any",
                )
                conn.commit()
            finally:
                conn.close()

            patch_resp = client.patch(
                f"/v1/ui/jobs/{job_id}/playlist-builder/override",
                headers=headers,
                json={"generation_mode": "curated", "max_duration_min": 80},
            )
            self.assertEqual(patch_resp.status_code, 200)

            brief_resp = client.get(f"/v1/playlist-builder/jobs/{job_id}/brief", headers=headers)
            self.assertEqual(brief_resp.status_code, 200)
            brief = brief_resp.json()["brief"]
            self.assertEqual(brief["generation_mode"], "curated")
            self.assertEqual(brief["min_duration_min"], 30)
            self.assertEqual(brief["max_duration_min"], 80)
            self.assertEqual(brief["preferred_batch_ratio"], 61)
            self.assertEqual(brief["target_duration_min"], 55)

            conn = dbm.connect(self.env)
            try:
                dbm.update_ui_job_playlist_builder_override_json(
                    conn,
                    job_id=job_id,
                    playlist_builder_override_json="{bad-json",
                )
                conn.commit()
            finally:
                conn.close()

            invalid_resp = client.get(f"/v1/playlist-builder/jobs/{job_id}/brief", headers=headers)
            self.assertEqual(invalid_resp.status_code, 422)
            self.assertEqual(invalid_resp.json()["error"]["code"], "PLB_INVALID_BRIEF")


if __name__ == "__main__":
    unittest.main()
