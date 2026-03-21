from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMetadataChannelDefaultsApi(unittest.TestCase):
    def _new_client(self) -> TestClient:
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _create_source_ids(self, client: TestClient, headers: dict[str, str], channel_slug: str = "darkwood-reverie") -> tuple[int, int, int]:
        title = client.post("/v1/metadata/title-templates", headers=headers, json={"channel_slug": channel_slug, "template_name": "T", "template_body": "{{channel_slug}}"})
        desc = client.post("/v1/metadata/description-templates", headers=headers, json={"channel_slug": channel_slug, "template_name": "D", "template_body": "{{channel_slug}}"})
        preset = client.post("/v1/metadata/video-tag-presets", headers=headers, json={"channel_slug": channel_slug, "preset_name": "P", "preset_body": ["{{channel_slug}}"]})
        self.assertEqual(title.status_code, 200)
        self.assertEqual(desc.status_code, 200)
        self.assertEqual(preset.status_code, 200)
        return title.json()["id"], desc.json()["id"], preset.json()["id"]

    def _event_record(self, records: list, event_name: str):
        for rec in records:
            if rec.msg == event_name:
                return rec
        return None

    def test_read_defaults_null_and_partial(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            empty = client.get("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers)
            self.assertEqual(empty.status_code, 200)
            self.assertEqual(empty.json()["defaults"], {"title_template": None, "description_template": None, "video_tag_preset": None})

            title_id, _, _ = self._create_source_ids(client, headers)
            put_resp = client.put(
                "/v1/metadata/channels/darkwood-reverie/defaults",
                headers=headers,
                json={"default_title_template_id": title_id, "default_description_template_id": None, "default_video_tag_preset_id": None},
            )
            self.assertEqual(put_resp.status_code, 200)

            partial = client.get("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers).json()["defaults"]
            self.assertIsNotNone(partial["title_template"])
            self.assertIsNone(partial["description_template"])
            self.assertIsNone(partial["video_tag_preset"])

    def test_update_success_and_no_release_mutation(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            title_id, desc_id, preset_id = self._create_source_ids(client, headers)

            conn = dbm.connect(env)
            try:
                channel = conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()
                release_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES (?, 'before-title', 'before-description', '[\"before\"]', NULL, NULL, 'meta_release_1', ?)",
                        (int(channel["id"]), dbm.now_ts()),
                    ).lastrowid
                )
                before = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()

            resp = client.put(
                "/v1/metadata/channels/darkwood-reverie/defaults",
                headers=headers,
                json={"default_title_template_id": title_id, "default_description_template_id": desc_id, "default_video_tag_preset_id": preset_id},
            )
            self.assertEqual(resp.status_code, 200)
            self.assertTrue(resp.json()["defaults_updated"])

            reread = client.get("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers).json()["defaults"]
            self.assertEqual(reread["title_template"]["id"], title_id)
            self.assertEqual(reread["description_template"]["id"], desc_id)
            self.assertEqual(reread["video_tag_preset"]["id"], preset_id)

            conn = dbm.connect(env)
            try:
                after = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
            finally:
                conn.close()
            self.assertEqual(before, after)

    def test_update_rejections(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            # wrong field type
            desc = client.post("/v1/metadata/description-templates", headers=headers, json={"channel_slug": "darkwood-reverie", "template_name": "D", "template_body": "{{channel_slug}}"})
            self.assertEqual(desc.status_code, 200)
            wrong_type = client.put("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers, json={"default_title_template_id": desc.json()["id"]})
            self.assertEqual(wrong_type.status_code, 422)
            self.assertEqual(wrong_type.json()["error"]["code"], "MDO_DEFAULT_FIELD_TYPE_MISMATCH")

            title_id, _, _ = self._create_source_ids(client, headers)
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE title_templates SET validation_status = 'INVALID' WHERE id = ?", (title_id,))
            finally:
                conn.close()
            invalid = client.put("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers, json={"default_title_template_id": title_id})
            self.assertEqual(invalid.status_code, 422)
            self.assertEqual(invalid.json()["error"]["code"], "MDO_DEFAULT_SOURCE_INVALID")

            archived = client.post(f"/v1/metadata/title-templates/{title_id}/archive", headers=headers)
            self.assertEqual(archived.status_code, 200)
            not_active = client.put("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers, json={"default_title_template_id": title_id})
            self.assertEqual(not_active.status_code, 422)
            self.assertEqual(not_active.json()["error"]["code"], "MDO_DEFAULT_SOURCE_NOT_ACTIVE")

            foreign_title_id, _, _ = self._create_source_ids(client, headers, channel_slug="channel-b")
            mismatch = client.put("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers, json={"default_title_template_id": foreign_title_id})
            self.assertEqual(mismatch.status_code, 422)
            self.assertEqual(mismatch.json()["error"]["code"], "MDO_DEFAULT_SOURCE_CHANNEL_MISMATCH")

    def test_logging_payload_successful_put_and_read(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            title_id, desc_id, preset_id = self._create_source_ids(client, headers)

            with self.assertLogs("services.factory_api.app", level="INFO") as logs:
                put_resp = client.put(
                    "/v1/metadata/channels/darkwood-reverie/defaults",
                    headers=headers,
                    json={"default_title_template_id": title_id, "default_description_template_id": desc_id, "default_video_tag_preset_id": preset_id},
                )
                self.assertEqual(put_resp.status_code, 200)
                read_resp = client.get("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers)
                self.assertEqual(read_resp.status_code, 200)

            updated_rec = self._event_record(logs.records, "metadata.defaults.updated")
            self.assertIsNotNone(updated_rec)
            self.assertEqual(updated_rec.channel_slug, "darkwood-reverie")
            self.assertEqual(updated_rec.result_status, "success")
            self.assertEqual(updated_rec.field_name, "multiple")
            self.assertEqual(updated_rec.source_type, "multiple")
            self.assertEqual(len(updated_rec.source_refs), 3)
            self.assertEqual({item["field_name"] for item in updated_rec.source_refs}, {"title", "description", "tags"})

            read_rec = self._event_record(logs.records, "metadata.defaults.read")
            self.assertIsNotNone(read_rec)
            self.assertEqual(read_rec.channel_slug, "darkwood-reverie")
            self.assertEqual(read_rec.result_status, "success")
            self.assertEqual(len(read_rec.source_refs), 3)

    def test_logging_payload_failed_put_includes_failing_source(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            client = self._new_client()
            headers = basic_auth_header(env.basic_user, env.basic_pass)
            desc = client.post("/v1/metadata/description-templates", headers=headers, json={"channel_slug": "darkwood-reverie", "template_name": "D", "template_body": "{{channel_slug}}"})
            self.assertEqual(desc.status_code, 200)

            with self.assertLogs("services.factory_api.app", level="INFO") as logs:
                resp = client.put("/v1/metadata/channels/darkwood-reverie/defaults", headers=headers, json={"default_title_template_id": desc.json()["id"]})
                self.assertEqual(resp.status_code, 422)
                self.assertEqual(resp.json()["error"]["code"], "MDO_DEFAULT_FIELD_TYPE_MISMATCH")

            updated_rec = self._event_record(logs.records, "metadata.defaults.updated")
            self.assertIsNotNone(updated_rec)
            self.assertEqual(updated_rec.result_status, "error")
            self.assertEqual(updated_rec.error_codes, ["MDO_DEFAULT_FIELD_TYPE_MISMATCH"])
            self.assertEqual(updated_rec.field_name, "title")
            self.assertEqual(updated_rec.source_type, "title_template")
            self.assertEqual(updated_rec.source_id, desc.json()["id"])
            self.assertEqual(len(updated_rec.source_refs), 3)


if __name__ == "__main__":
    unittest.main()
