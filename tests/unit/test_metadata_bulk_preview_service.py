from __future__ import annotations

import unittest
import uuid

from services.common import db as dbm
from services.metadata import preview_apply_service
from services.planner import metadata_bulk_preview_service as svc
from tests._helpers import seed_minimal_db, temp_env


class TestMetadataBulkPreviewService(unittest.TestCase):
    def _insert_planner_item(self, conn, *, channel_slug: str = "darkwood-reverie", status: str = "PLANNED", publish_at: str = "2026-01-01T00:00:00Z") -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES(?, 'LONG', 'P title', ?, 'P notes', ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (channel_slug, publish_at, status),
        )
        return int(cur.lastrowid)

    def test_preview_persists_only_bulk_session_and_marks_unresolved(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                planner_id = self._insert_planner_item(conn)
                unresolved_id = self._insert_planner_item(conn, publish_at="2026-01-01T01:00:00Z")
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-1', 0)
                    """,
                    (channel_id,),
                )
                rel_id = int(cur.lastrowid)
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (planner_id, rel_id),
                )
                conn.commit()

                out = svc.create_bulk_preview_session(
                    conn,
                    planner_item_ids=[planner_id, unresolved_id],
                    fields=["title", "description", "tags"],
                    overrides={},
                    created_by="tester",
                    ttl_seconds=1800,
                )
                self.assertEqual(out["session_status"], "OPEN")
                self.assertEqual(len(out["items"]), 2)
                resolved = next(item for item in out["items"] if item["planner_item_id"] == planner_id)
                self.assertEqual((resolved["fields"]["title"].get("source") or {}).get("selection_mode"), "channel_default")
                unresolved = next(item for item in out["items"] if item["planner_item_id"] == unresolved_id)
                self.assertEqual(unresolved["mapping_status"], "UNRESOLVED_NO_TARGET")

                bulk_rows = conn.execute("SELECT COUNT(*) AS c FROM metadata_bulk_preview_sessions").fetchone()["c"]
                self.assertEqual(int(bulk_rows), 1)
                nested_rows = conn.execute("SELECT COUNT(*) AS c FROM metadata_preview_sessions").fetchone()["c"]
                self.assertEqual(int(nested_rows), 0)
            finally:
                conn.close()

    def test_duplicate_release_target_is_deduped(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                p1 = self._insert_planner_item(conn)
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                cur = conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-2', 0)
                    """,
                    (channel_id,),
                )
                rid = int(cur.lastrowid)
                conn.execute("INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')", (p1, rid))
                conn.commit()

                out = svc.create_bulk_preview_session(
                    conn,
                    planner_item_ids=[p1, p1],
                    fields=["title"],
                    overrides={},
                    created_by="tester",
                    ttl_seconds=1800,
                )
                self.assertEqual(out["summary"]["selected_item_count"], 2)
                self.assertEqual(out["summary"]["resolved_target_count"], 1)
                self.assertEqual(out["summary"]["deduped_target_count"], 1)
                dup = out["items"][1]
                self.assertEqual(dup["mapping_status"], "DUPLICATE_TARGET")
            finally:
                conn.close()

    def test_preview_rejects_more_than_100_selected_items(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                over_limit = list(range(1, 102))
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.create_bulk_preview_session(
                        conn,
                        planner_item_ids=over_limit,
                        fields=["title"],
                        overrides={},
                        created_by="tester",
                        ttl_seconds=1800,
                    )
                self.assertEqual(ctx.exception.code, "MBP_SELECTED_ITEMS_LIMIT_EXCEEDED")
            finally:
                conn.close()

    def test_required_override_mode_does_not_fallback_to_default_for_non_matching_channel(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled, youtube_channel_id)
                    VALUES('titanwave-sonic', 'Titanwave Sonic', 'music', 1, 'default', 0, 'yt-titanwave-sonic')
                    """
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO channel_metadata_defaults(channel_slug, default_title_template_id, default_description_template_id, default_video_tag_preset_id, created_at, updated_at)
                    VALUES('titanwave-sonic', NULL, NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                    """
                )
                planner_id = self._insert_planner_item(conn, channel_slug="titanwave-sonic")
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'titanwave-sonic'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'seed', 'seed', '[]', '2026-01-01T00:00:00Z', NULL, 'seed-meta-override-required', 0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute(
                    "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')",
                    (planner_id, release_id),
                )
                darkwood_title_template_id = int(
                    conn.execute(
                        """
                        INSERT INTO title_templates(channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at)
                        VALUES('darkwood-reverie', 'darkwood-only', 'Darkwood {{release_id}}', 'ACTIVE', 0, 'VALID', NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL)
                        """
                    ).lastrowid
                )
                conn.commit()

                out = svc.create_bulk_preview_session(
                    conn,
                    planner_item_ids=[planner_id],
                    fields=["title"],
                    overrides={
                        "title": {
                            "mode": "CHANNEL_GROUP_OVERRIDE_REQUIRED",
                            "overrides": [{"channel_slug": "darkwood-reverie", "source_id": darkwood_title_template_id}],
                        }
                    },
                    created_by="tester",
                    ttl_seconds=1800,
                )
                item = out["items"][0]
                field = item["fields"]["title"]
                self.assertEqual(field["status"], "INVALID_OVERRIDE")
                self.assertEqual((field.get("source") or {}).get("selection_mode"), "temporary_override")
            finally:
                conn.close()

    def _insert_release(self, conn, *, title: str = "seed", description: str = "seed", tags_json: str = '["ambient"]', planned_at: str = "2026-01-01T00:00:00Z") -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        meta_id = f"seed-meta-{uuid.uuid4().hex[:8]}"
        cur = conn.execute(
            """
            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, ?, ?, ?, ?, NULL, ?, 0)
            """,
            (channel_id, title, description, tags_json, planned_at, meta_id),
        )
        return int(cur.lastrowid)

    def _create_applyable_session(self, conn) -> tuple[dict, int, int]:
        p1 = self._insert_planner_item(conn, publish_at="2026-01-01T00:00:00Z")
        p2 = self._insert_planner_item(conn, publish_at="2026-01-02T00:00:00Z")
        r1 = self._insert_release(conn, title="T1", description="D1", tags_json='["a"]', planned_at="2026-01-01T00:00:00Z")
        r2 = self._insert_release(conn, title="T2", description="D2", tags_json='["b"]', planned_at="2026-01-02T00:00:00Z")
        conn.execute("INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')", (p1, r1))
        conn.execute("INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES(?, ?, '2026-01-01T00:00:00Z', 'seed')", (p2, r2))
        title_template_id = int(
            conn.execute(
                """
                INSERT INTO title_templates(channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at)
                VALUES('darkwood-reverie', 'u-title', 'Generated {{release_id}}', 'ACTIVE', 0, 'VALID', NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL)
                """
            ).lastrowid
        )
        desc_template_id = int(
            conn.execute(
                """
                INSERT INTO description_templates(channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, last_validated_at, created_at, updated_at, archived_at)
                VALUES('darkwood-reverie', 'u-desc', 'Desc {{release_id}}', 'ACTIVE', 0, 'VALID', NULL, NULL, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', NULL)
                """
            ).lastrowid
        )
        conn.commit()
        out = svc.create_bulk_preview_session(
            conn,
            planner_item_ids=[p1, p2],
            fields=["title", "description"],
            overrides={
                "title": {"mode": "CHANNEL_GROUP_OVERRIDE_IF_MATCHES", "overrides": [{"channel_slug": "darkwood-reverie", "source_id": title_template_id}]},
                "description": {"mode": "CHANNEL_GROUP_OVERRIDE_IF_MATCHES", "overrides": [{"channel_slug": "darkwood-reverie", "source_id": desc_template_id}]},
            },
            created_by="tester",
            ttl_seconds=1800,
        )
        session_items = dbm.json_loads(
            str(conn.execute("SELECT item_states_json FROM metadata_bulk_preview_sessions WHERE id = ?", (out["session_id"],)).fetchone()["item_states_json"])
        )
        for item in session_items:
            if item["mapping_status"] != "RESOLVED_TO_RELEASE":
                continue
            release = preview_apply_service._load_release(conn, release_id=int(item["release_id"]))
            source = {
                "source_type": "title_template",
                "source_id": title_template_id,
                "source_name": "u-title",
                "selection_mode": "temporary_override",
                "channel_slug": "darkwood-reverie",
                "updated_at": "2026-01-01T00:00:00Z",
            }
            proposed_title = f"Prepared Title {item['release_id']}"
            item["fields"]["title"] = {
                "status": "OVERWRITE_READY",
                "current_value": str(release.get("title") or ""),
                "proposed_value": proposed_title,
                "changed": True,
                "overwrite_required": True,
                "source": source,
                "warnings": [],
                "errors": [],
                "dependency_fingerprint": preview_apply_service._build_field_dependency_fingerprint(
                    field="title",
                    release_row=release,
                    source=source,
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
            (dbm.json_dumps(session_items), out["session_id"]),
        )
        conn.commit()
        return out, p1, p2

    def test_apply_selected_items_empty_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _p2 = self._create_applyable_session(conn)
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.apply_bulk_preview_session(
                        conn,
                        session_id=out["session_id"],
                        selected_items=[],
                        selected_fields=["title"],
                        overwrite_confirmed={str(p1): ["title"]},
                    )
                self.assertEqual(ctx.exception.code, "MBP_SELECTED_ITEMS_EMPTY")
            finally:
                conn.close()

    def test_apply_selected_fields_empty_errors(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _p2 = self._create_applyable_session(conn)
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.apply_bulk_preview_session(
                        conn,
                        session_id=out["session_id"],
                        selected_items=[p1],
                        selected_fields=[],
                        overwrite_confirmed={},
                    )
                self.assertEqual(ctx.exception.code, "MBP_SELECTED_FIELDS_EMPTY")
            finally:
                conn.close()

    def test_apply_selected_items_subset_validation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _p2 = self._create_applyable_session(conn)
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.apply_bulk_preview_session(
                        conn,
                        session_id=out["session_id"],
                        selected_items=[p1, 999999],
                        selected_fields=["title"],
                        overwrite_confirmed={},
                    )
                self.assertEqual(ctx.exception.code, "MBP_FIELD_NOT_PREPARED")
            finally:
                conn.close()

    def test_apply_requires_overwrite_confirmation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _p2 = self._create_applyable_session(conn)
                sess = svc.get_bulk_preview_session(conn, session_id=out["session_id"])
                target = next(item for item in sess["items"] if item["planner_item_id"] == p1)
                overwrite_field = "title"
                resp = svc.apply_bulk_preview_session(
                    conn,
                    session_id=out["session_id"],
                    selected_items=[p1],
                    selected_fields=[overwrite_field],
                    overwrite_confirmed={},
                )
                self.assertEqual(resp["items"][0]["result"], "failure")
                self.assertEqual(resp["items"][0]["errors"][0]["code"], "MBP_OVERWRITE_CONFIRMATION_REQUIRED")
            finally:
                conn.close()

    def test_apply_stale_current_snapshot_and_dependency_drift(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _ = self._create_applyable_session(conn)
                release_id = int(conn.execute("SELECT release_id FROM planner_release_links WHERE planned_release_id = ?", (p1,)).fetchone()["release_id"])
                conn.execute("UPDATE releases SET title = 'changed', planned_at = '2026-01-05T00:00:00Z' WHERE id = ?", (release_id,))
                conn.commit()
                resp = svc.apply_bulk_preview_session(
                    conn,
                    session_id=out["session_id"],
                    selected_items=[p1],
                    selected_fields=["title"],
                    overwrite_confirmed={str(p1): ["title"]},
                )
                self.assertEqual(resp["items"][0]["result"], "failure")
                self.assertEqual(resp["items"][0]["errors"][0]["code"], "MBP_PREVIEW_STALE")
            finally:
                conn.close()

    def test_apply_stale_source_invalidation(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _ = self._create_applyable_session(conn)
                source_id = int(
                    conn.execute(
                        "SELECT id FROM title_templates WHERE channel_slug = 'darkwood-reverie' AND status = 'ACTIVE' LIMIT 1"
                    ).fetchone()["id"]
                )
                conn.execute("UPDATE title_templates SET status = 'ARCHIVED' WHERE id = ?", (source_id,))
                conn.commit()
                resp = svc.apply_bulk_preview_session(
                    conn,
                    session_id=out["session_id"],
                    selected_items=[p1],
                    selected_fields=["title"],
                    overwrite_confirmed={str(p1): ["title"]},
                )
                self.assertEqual(resp["items"][0]["result"], "failure")
                self.assertEqual(resp["items"][0]["errors"][0]["code"], "MBP_PREVIEW_STALE")
            finally:
                conn.close()

    def test_apply_session_expired_and_invalidated(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _ = self._create_applyable_session(conn)
                conn.execute("UPDATE metadata_bulk_preview_sessions SET session_status = 'INVALIDATED' WHERE id = ?", (out["session_id"],))
                conn.commit()
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.apply_bulk_preview_session(
                        conn,
                        session_id=out["session_id"],
                        selected_items=[p1],
                        selected_fields=["title"],
                        overwrite_confirmed={str(p1): ["title"]},
                    )
                self.assertEqual(ctx.exception.code, "MBP_SESSION_INVALIDATED")
            finally:
                conn.close()
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, _ = self._create_applyable_session(conn)
                conn.execute("UPDATE metadata_bulk_preview_sessions SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (out["session_id"],))
                conn.commit()
                with self.assertRaises(svc.MetadataBulkPreviewError) as ctx:
                    svc.apply_bulk_preview_session(
                        conn,
                        session_id=out["session_id"],
                        selected_items=[p1],
                        selected_fields=["title"],
                        overwrite_confirmed={str(p1): ["title"]},
                    )
                self.assertEqual(ctx.exception.code, "MBP_SESSION_EXPIRED")
            finally:
                conn.close()

    def test_apply_item_atomicity_rolls_back_item_and_uses_persisted_values(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                out, p1, p2 = self._create_applyable_session(conn)
                sess = svc.get_bulk_preview_session(conn, session_id=out["session_id"])
                p1_state = next(item for item in sess["items"] if item["planner_item_id"] == p1)
                p2_state = next(item for item in sess["items"] if item["planner_item_id"] == p2)
                # Force p2 to fail by drift after preview.
                conn.execute("UPDATE releases SET title = 'drifted' WHERE id = ?", (int(p2_state["release_id"]),))
                # Change source template body after preview to prove no hidden regeneration.
                title_source_id = int(p1_state["fields"]["title"]["source"]["source_id"])
                conn.execute("UPDATE title_templates SET template_body = 'MUTATED {{release_id}}' WHERE id = ?", (title_source_id,))
                conn.commit()

                resp = svc.apply_bulk_preview_session(
                    conn,
                    session_id=out["session_id"],
                    selected_items=[p1, p2],
                    selected_fields=["title"],
                    overwrite_confirmed={str(p1): ["title"], str(p2): ["title"]},
                )
                self.assertEqual(resp["result"], "partial_success")
                i1 = next(item for item in resp["items"] if item["planner_item_id"] == p1)
                i2 = next(item for item in resp["items"] if item["planner_item_id"] == p2)
                self.assertEqual(i1["result"], "success")
                self.assertEqual(i2["result"], "failure")

                r1_title = str(conn.execute("SELECT title FROM releases WHERE id = ?", (int(p1_state["release_id"]),)).fetchone()["title"])
                expected_persisted_title = str(p1_state["fields"]["title"]["proposed_value"] or "")
                self.assertEqual(r1_title, expected_persisted_title)
                r2_title = str(conn.execute("SELECT title FROM releases WHERE id = ?", (int(p2_state["release_id"]),)).fetchone()["title"])
                self.assertEqual(r2_title, "drifted")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
