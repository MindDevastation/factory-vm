from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.metadata import preview_apply_service
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerMetadataBulkPreviewApi(unittest.TestCase):
    def _insert_planner_item(self, env, *, publish_at: str, link_release_id: int | None = None) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES('darkwood-reverie', 'LONG', 'P title', ?, 'P notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                (publish_at,),
            )
            pid = int(cur.lastrowid)
            if link_release_id is not None:
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (pid, link_release_id),
                )
            conn.commit()
            return pid
        finally:
            conn.close()

    def _force_title_prepared(self, env, *, session_id: str) -> None:
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT item_states_json FROM metadata_bulk_preview_sessions WHERE id = ?", (session_id,)).fetchone()
            items = dbm.json_loads(str(row["item_states_json"]))
            for item in items:
                if item.get("mapping_status") != "RESOLVED_TO_RELEASE":
                    continue
                release = preview_apply_service._load_release(conn, release_id=int(item["release_id"]))
                item["fields"]["title"] = {
                    "status": "OVERWRITE_READY",
                    "current_value": str(release.get("title") or ""),
                    "proposed_value": f"Prepared {item['release_id']}",
                    "changed": True,
                    "overwrite_required": True,
                    "source": None,
                    "warnings": [],
                    "errors": [],
                    "dependency_fingerprint": preview_apply_service._build_field_dependency_fingerprint(
                        field="title",
                        release_row=release,
                        source={},
                        generator_fingerprint="seed",
                    ),
                }
                item["fields"]["description"] = {
                    "status": "NO_CHANGE",
                    "current_value": str(release.get("description") or ""),
                    "proposed_value": str(release.get("description") or ""),
                    "changed": False,
                    "overwrite_required": False,
                    "source": None,
                    "warnings": [],
                    "errors": [],
                    "dependency_fingerprint": preview_apply_service._build_field_dependency_fingerprint(
                        field="description",
                        release_row=release,
                        source={},
                        generator_fingerprint="seed",
                    ),
                }
            conn.execute(
                "UPDATE metadata_bulk_preview_sessions SET item_states_json = ? WHERE id = ?",
                (dbm.json_dumps(items), session_id),
            )
            conn.commit()
        finally:
            conn.close()

    def test_context_preview_and_session_roundtrip(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-int', 0)
                    """,
                    (channel_id,),
                )
                release_id = int(cur.lastrowid)
                conn.commit()
            finally:
                conn.close()
            p1 = self._insert_planner_item(env, publish_at="2026-01-01T00:00:00Z", link_release_id=release_id)
            p2 = self._insert_planner_item(env, publish_at="2026-01-01T01:00:00Z", link_release_id=None)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            ctx = client.get(f"/v1/planner/metadata-bulk/context?planner_item_ids={p1},{p2}", headers=auth)
            self.assertEqual(ctx.status_code, 200)
            self.assertEqual(ctx.json()["selected_item_count"], 2)

            preview = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [p1, p2], "fields": ["title", "description", "tags"], "overrides": {}},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertEqual(body["summary"]["selected_item_count"], 2)
            unresolved = next(item for item in body["items"] if item["planner_item_id"] == p2)
            self.assertEqual(unresolved["mapping_status"], "UNRESOLVED_NO_TARGET")

            sid = body["session_id"]
            sess = client.get(f"/v1/planner/metadata-bulk/sessions/{sid}", headers=auth)
            self.assertEqual(sess.status_code, 200)
            self.assertEqual(sess.json()["session_id"], sid)

    def test_context_invalid_query_returns_deterministic_error(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get("/v1/planner/metadata-bulk/context?planner_item_ids=abc,2", headers=auth)
            self.assertEqual(resp.status_code, 400)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "PLR_INVALID_INPUT")

    def test_preview_non_object_and_shape_validation_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            non_object = client.post("/v1/planner/metadata-bulk/preview", headers=auth, json=["not-an-object"])
            self.assertEqual(non_object.status_code, 400)
            self.assertEqual(non_object.json()["error"]["code"], "PLR_INVALID_INPUT")

            invalid_ids = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [1, "x"], "fields": ["title"], "overrides": {}},
            )
            self.assertEqual(invalid_ids.status_code, 400)
            self.assertEqual(invalid_ids.json()["error"]["code"], "PLR_INVALID_INPUT")

            invalid_fields = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [1], "fields": [1, 2], "overrides": {}},
            )
            self.assertEqual(invalid_fields.status_code, 400)
            self.assertEqual(invalid_fields.json()["error"]["code"], "PLR_INVALID_INPUT")

            invalid_overrides = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [1], "fields": ["title"], "overrides": []},
            )
            self.assertEqual(invalid_overrides.status_code, 400)
            self.assertEqual(invalid_overrides.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_apply_selected_subset_and_partial_success(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                r1 = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?, 'r1', 'd1', '[\"a\"]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-a', 0)",
                        (channel_id,),
                    ).lastrowid
                )
                r2 = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?, 'r2', 'd2', '[\"b\"]', '2026-01-02T00:00:00Z', NULL, 'seed-meta-b', 0)",
                        (channel_id,),
                    ).lastrowid
                )
                conn.commit()
            finally:
                conn.close()

            p1 = self._insert_planner_item(env, publish_at="2026-01-01T00:00:00Z", link_release_id=r1)
            p2 = self._insert_planner_item(env, publish_at="2026-01-02T00:00:00Z", link_release_id=r2)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            preview = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [p1, p2], "fields": ["title", "description"], "overrides": {}},
            )
            self.assertEqual(preview.status_code, 200)
            sid = str(preview.json()["session_id"])
            self._force_title_prepared(env, session_id=sid)
            p1_title = f"Prepared {r1}"

            # Drift p2 so apply is stale while p1 remains fresh.
            conn = dbm.connect(env)
            try:
                conn.execute("UPDATE releases SET title = 'drifted p2' WHERE id = ?", (r2,))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(
                f"/v1/planner/metadata-bulk/sessions/{sid}/apply",
                headers=auth,
                json={
                    "selected_items": [p1, p2],
                    "selected_fields": ["title"],
                    "overwrite_confirmed": {str(p1): ["title"], str(p2): ["title"]},
                },
            )
            self.assertEqual(apply_resp.status_code, 200)
            result = apply_resp.json()
            self.assertEqual(result["result"], "partial_success")
            item1 = next(item for item in result["items"] if item["planner_item_id"] == p1)
            item2 = next(item for item in result["items"] if item["planner_item_id"] == p2)
            self.assertEqual(item1["result"], "success")
            self.assertEqual(item2["result"], "failure")

            conn = dbm.connect(env)
            try:
                self.assertEqual(str(conn.execute("SELECT title FROM releases WHERE id = ?", (r1,)).fetchone()["title"]), p1_title)
                self.assertEqual(str(conn.execute("SELECT title FROM releases WHERE id = ?", (r2,)).fetchone()["title"]), "drifted p2")
            finally:
                conn.close()

    def test_apply_no_change_and_unselected_fields_unchanged_and_defaults_unchanged(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                rel_id = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at) VALUES(?, 'same-title', 'd1', '[\"a\"]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-c', 0)",
                        (channel_id,),
                    ).lastrowid
                )
                defaults_before = dict(
                    conn.execute(
                        "SELECT default_title_template_id, default_description_template_id, default_video_tag_preset_id FROM channel_metadata_defaults WHERE channel_slug = 'darkwood-reverie'"
                    ).fetchone() or {}
                )
                conn.commit()
            finally:
                conn.close()
            p1 = self._insert_planner_item(env, publish_at="2026-01-01T00:00:00Z", link_release_id=rel_id)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            preview = client.post(
                "/v1/planner/metadata-bulk/preview",
                headers=auth,
                json={"planner_item_ids": [p1], "fields": ["title", "description"], "overrides": {}},
            )
            self.assertEqual(preview.status_code, 200)
            sid = str(preview.json()["session_id"])
            self._force_title_prepared(env, session_id=sid)
            apply_resp = client.post(
                f"/v1/planner/metadata-bulk/sessions/{sid}/apply",
                headers=auth,
                json={
                    "selected_items": [p1],
                    "selected_fields": ["description"],
                    "overwrite_confirmed": {},
                },
            )
            self.assertEqual(apply_resp.status_code, 200)
            payload = apply_resp.json()
            item = payload["items"][0]
            self.assertEqual(item["result"], "success")
            self.assertIn("description", item["unchanged_fields"])
            self.assertNotIn("title", item["applied_fields"])
            self.assertNotIn("title", item["unchanged_fields"])

            conn = dbm.connect(env)
            try:
                row = dict(conn.execute("SELECT title, description FROM releases WHERE id = ?", (rel_id,)).fetchone())
                defaults_after = dict(
                    conn.execute(
                        "SELECT default_title_template_id, default_description_template_id, default_video_tag_preset_id FROM channel_metadata_defaults WHERE channel_slug = 'darkwood-reverie'"
                    ).fetchone() or {}
                )
                self.assertEqual(row["title"], "same-title")
                self.assertEqual(defaults_after, defaults_before)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
