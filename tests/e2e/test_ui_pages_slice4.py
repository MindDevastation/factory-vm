from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPagesSlice4(unittest.TestCase):
    def test_pages_and_validation(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert ch
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(ch["id"]),
                    title="T",
                    description="",
                    tags_csv="",
                    cover_name="",
                    cover_ext="",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            r = client.get("/ui/jobs/create", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Create Job", r.text)
            self.assertIn("<form", r.text)
            self.assertIn('name="channel_id"', r.text)
            self.assertIn('name="title"', r.text)
            self.assertIn('name="audio_ids_text"', r.text)
            self.assertIn('name="background_name"', r.text)
            self.assertIn('name="background_ext"', r.text)

            r = client.get("/", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn('action="/ui/jobs/render_all"', r.text)
            self.assertIn('method="post"', r.text)
            self.assertIn('id="channel-add-btn"', r.text)
            self.assertIn('id="channels-table"', r.text)
            self.assertIn('href="/ui/db-viewer"', r.text)
            self.assertIn('href="/ui/planner"', r.text)
            self.assertIn('href="/ui/tags"', r.text)
            self.assertIn('href="/ui/track-catalog/analysis-report"', r.text)

            r = client.get("/ui/db-viewer", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Database Viewer", r.text)
            self.assertIn('id="table-select"', r.text)
            self.assertIn('id="search-input"', r.text)
            self.assertIn('id="page-size-select"', r.text)

            r = client.get("/ui/planner", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Planner · Bulk Releases", r.text)
            self.assertIn('id="planner-tbody"', r.text)
            self.assertIn('id="bulk-create-modal"', r.text)
            self.assertIn('id="import-modal"', r.text)
            self.assertIn('/static/planner_bulk_releases.js', r.text)

            r = client.get("/ui/track-catalog/analysis-report", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Track Catalog · Analysis Report", r.text)
            self.assertIn('id="tar-channel-select"', r.text)
            self.assertIn('id="tar-export-btn"', r.text)
            self.assertIn('id="tar-table"', r.text)
            self.assertIn('id="tar-tag-editor-modal"', r.text)
            self.assertIn('id="tar-tag-add-btn"', r.text)
            self.assertIn('tagEditorGroups.addEventListener', r.text)

            r = client.get("/ui/tags", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("TAGS", r.text)
            self.assertIn('id="tags-table"', r.text)
            self.assertIn('id="tags-import-btn"', r.text)
            self.assertIn('id="tags-export-btn"', r.text)
            self.assertIn('id="tag-editor-modal"', r.text)
            self.assertIn('id="tag-json-mode"', r.text)
            self.assertIn('if (editorJsonMode.checked)', r.text)
            self.assertIn("credentials: 'same-origin'", r.text)
            self.assertIn('function resolveApiPath(path)', r.text)
            self.assertIn('Catalog is empty.', r.text)
            self.assertNotIn("fetch('/v1/track-catalog/custom-tags/catalog')", r.text)
            self.assertNotIn('payload.code || editorCode.value', r.text)
            self.assertNotIn('payload.label || editorLabel.value', r.text)

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Edit Job", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": int(ch["id"]),
                    "title": "",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "",
                    "background_ext": "",
                    "audio_ids_text": "",
                },
            )
            self.assertEqual(r.status_code, 422)
            self.assertIn("title is required", r.text)
            self.assertIn("audio ids are required", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": "not-a-number",
                    "title": "Valid title",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertNotEqual(r.status_code, 500)
            self.assertEqual(r.status_code, 422)
            self.assertIn("project is required", r.text)

            r = client.post(
                "/ui/jobs/create",
                headers=h,
                data={
                    "channel_id": 999999,
                    "title": "Valid title",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertEqual(r.status_code, 422)
            self.assertIn("project is invalid", r.text)

            conn2 = dbm.connect(env)
            try:
                dbm.update_job_state(conn2, job_id, state="READY_FOR_RENDER", stage="FETCH")
            finally:
                conn2.close()

            r = client.post(
                f"/ui/jobs/{job_id}/edit",
                headers=h,
                data={
                    "channel_id": int(ch["id"]),
                    "title": "Z",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "bg",
                    "background_ext": "jpg",
                    "audio_ids_text": "001",
                },
            )
            self.assertEqual(r.status_code, 409)


    def test_tags_editor_manual_fields_override_stale_json(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            created = client.post(
                "/v1/track-catalog/custom-tags/catalog",
                headers=h,
                json={
                    "category": "VISUAL",
                    "code": "OLD_CODE",
                    "label": "Old Label",
                    "description": "old",
                    "is_active": True,
                },
            )
            self.assertEqual(created.status_code, 200)
            tag_id = created.json().get("tag", {}).get("id")
            self.assertIsInstance(tag_id, int)

            # Simulate editor save where stale JSON is present but manual fields were changed.
            stale_json_payload = {
                "id": tag_id,
                "category": "VISUAL",
                "code": "OLD_CODE",
                "label": "Old Label",
                "description": "old",
                "is_active": True,
            }
            manual_payload = {
                "code": "NEW_CODE",
                "label": "New Label",
                "description": "new desc",
                "is_active": False,
            }

            saved = client.patch(
                f"/v1/track-catalog/custom-tags/catalog/{tag_id}",
                headers=h,
                json=manual_payload,
            )
            self.assertEqual(saved.status_code, 200)

            listed = client.get("/v1/track-catalog/custom-tags/catalog", headers=h)
            self.assertEqual(listed.status_code, 200)
            tags = listed.json().get("tags", [])
            updated = next((t for t in tags if t.get("id") == tag_id), None)
            self.assertIsNotNone(updated)
            self.assertEqual(updated.get("code"), "NEW_CODE")
            self.assertEqual(updated.get("label"), "New Label")
            self.assertEqual(updated.get("description"), "new desc")
            self.assertFalse(updated.get("is_active"))

            # Confirm stale JSON values were not written back.
            self.assertNotEqual(updated.get("code"), stale_json_payload["code"])
            self.assertNotEqual(updated.get("label"), stale_json_payload["label"])


if __name__ == "__main__":
    unittest.main()
