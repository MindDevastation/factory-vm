from __future__ import annotations

import hashlib
import importlib
import json
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from services.planner.planned_release_readiness_service import PlannedReleaseReadinessService
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerPlannedReleaseReadinessApi(unittest.TestCase):
    _SNAPSHOT_TABLES = [
        "planned_releases",
        "planner_release_links",
        "releases",
        "jobs",
        "ui_job_drafts",
        "playlist_history",
        "playlist_history_items",
        "channel_metadata_defaults",
        "title_templates",
        "description_templates",
        "video_tag_presets",
    ]

    def _insert_planned_release(self, env: Env, *, publish_at: str | None = "2025-06-01T10:00:00+02:00") -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES ('darkwood-reverie', 'LONG', 'x', ?, 'n', 'PLANNED', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """,
                (publish_at,),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def _seed_full_ready_state(self, env: Env, planned_release_id: int) -> None:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES (?, 't', 'd', '["tag"]', '2025-06-01T10:00:00+02:00', NULL, ?, 1.0)
                """,
                (int(ch["id"]), f"meta-{planned_release_id}"),
            )
            release_id = int(cur.lastrowid)
            conn.execute(
                "INSERT INTO planner_release_links(planned_release_id, release_id, created_at, created_by) VALUES (?, ?, '2025-01-01T00:00:00Z', 'u')",
                (planned_release_id, release_id),
            )
            conn.execute(
                """
                INSERT INTO title_templates(id, channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (201, 'darkwood-reverie', 't', 'x', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO description_templates(id, channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (202, 'darkwood-reverie', 'd', 'x', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO video_tag_presets(id, channel_slug, preset_name, preset_body_json, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (203, 'darkwood-reverie', 'v', '["a"]', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO channel_metadata_defaults(channel_slug, default_title_template_id, default_description_template_id, default_video_tag_preset_id, created_at, updated_at)
                VALUES ('darkwood-reverie', 201, 202, 203, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO playlist_builder_channel_settings(
                    channel_slug, default_generation_mode, min_duration_min, max_duration_min,
                    tolerance_min, preferred_month_batch, preferred_batch_ratio, allow_cross_channel,
                    novelty_target_min, novelty_target_max, position_memory_window,
                    strictness_mode, vocal_policy, reuse_policy, created_at, updated_at
                ) VALUES ('darkwood-reverie', 'AUTO', 10, 40, 1, NULL, 70, 0, 0.5, 0.8, 10, 'balanced', 'allow', 'avoid_recent', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                "INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, filename, discovered_at) VALUES ('darkwood-reverie', 'trk1', 'gid1', 'x.wav', 1.0)"
            )
            cur_job = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'DRAFT', 'FETCH', 1, 1.0, 1.0)",
                (release_id,),
            )
            job_id = int(cur_job.lastrowid)
            if job_id != 1:
                conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (job_id, job_id))
            conn.execute(
                "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, is_active, created_at) VALUES (1, 'darkwood-reverie', ?, 'DRAFT', 'AUTO', 'balanced', 100.0, 1, 's', 'o', 'n3', 'n5', 1, '2025-01-01T00:00:00Z')",
                (job_id,),
            )
            conn.execute(
                "INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES (1, 1, 0, 1, '2025-06', 100.0, 'darkwood-reverie')"
            )
            conn.execute(
                "INSERT INTO ui_job_drafts(job_id, channel_id, title, description, tags_csv, cover_name, cover_ext, background_name, background_ext, audio_ids_text, created_at, updated_at) VALUES (?, ?, 't', 'd', 'a', NULL, NULL, 'bg', 'png', '1', 1.0, 1.0)",
                (job_id, int(ch["id"])),
            )
        finally:
            conn.close()

    def _snapshot(self, env: Env) -> dict[str, str]:
        conn = dbm.connect(env)
        try:
            out: dict[str, str] = {}
            for table in self._SNAPSHOT_TABLES:
                rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                payload = json.dumps(rows, sort_keys=True, default=str)
                out[table] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
            return out
        finally:
            conn.close()

    def test_get_readiness_returns_expected_shape_and_ready_state(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_release_id = self._insert_planned_release(env)
            self._seed_full_ready_state(env, planned_release_id)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/planner/planned-releases/{planned_release_id}/readiness", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["aggregate_status"], "READY_FOR_MATERIALIZATION")
            self.assertIn(body["aggregate_status"], {"NOT_READY", "BLOCKED", "READY_FOR_MATERIALIZATION"})
            self.assertTrue(str(body.get("computed_at") or "").endswith("Z"))
            self.assertEqual(set(body["domains"].keys()), {"planning_identity", "scheduling", "metadata", "playlist", "visual_assets"})
            self.assertIsNone(body["primary_reason"])
            self.assertIsNone(body["primary_remediation_hint"])

    def test_get_readiness_blocked_precedence_and_no_side_effects(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_release_id = self._insert_planned_release(env, publish_at="bad-date")
            before = self._snapshot(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/planner/planned-releases/{planned_release_id}/readiness", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["aggregate_status"], "BLOCKED")
            self.assertEqual(body["primary_reason"]["severity"], "BLOCKED")

            after = self._snapshot(env)
            self.assertEqual(before, after)

    def test_opening_readiness_detail_endpoint_is_read_only_no_mutation(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_release_id = self._insert_planned_release(env)
            self._seed_full_ready_state(env, planned_release_id)
            before = self._snapshot(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/planner/planned-releases/{planned_release_id}/readiness", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["planned_release_id"], planned_release_id)
            self.assertIn(body["aggregate_status"], {"NOT_READY", "BLOCKED", "READY_FOR_MATERIALIZATION"})
            self.assertTrue(str(body.get("computed_at") or "").endswith("Z"))

            after = self._snapshot(env)
            self.assertEqual(before, after)

    def test_get_readiness_link_absent_fallback_and_not_found(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_release_id = self._insert_planned_release(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            headers = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(f"/v1/planner/planned-releases/{planned_release_id}/readiness", headers=headers)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["domains"]["visual_assets"]["status"], "NOT_READY")
            self.assertEqual(body["domains"]["metadata"]["status"], "NOT_READY")
            self.assertEqual(body["domains"]["playlist"]["status"], "NOT_READY")

            not_found = client.get("/v1/planner/planned-releases/999999/readiness", headers=headers)
            self.assertEqual(not_found.status_code, 404)
            self.assertEqual(not_found.json()["error"]["code"], "PRS_PLANNED_RELEASE_NOT_FOUND")

    def test_evaluate_many_batch_matches_single_and_no_side_effects(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_planned_release(env)
            second = self._insert_planned_release(env, publish_at="bad-date")
            self._seed_full_ready_state(env, first)

            conn = dbm.connect(env)
            try:
                svc = PlannedReleaseReadinessService(conn)
                before = self._snapshot(env)
                batched = svc.evaluate_many(planned_release_ids=[first, second])
                single_first = svc.evaluate(planned_release_id=first)
                single_second = svc.evaluate(planned_release_id=second)
                after = self._snapshot(env)
            finally:
                conn.close()

            self.assertEqual(before, after)
            self.assertEqual(batched[first]["aggregate_status"], single_first["aggregate_status"])
            self.assertEqual(batched[second]["aggregate_status"], single_second["aggregate_status"])
            self.assertEqual(
                set(batched[first]["domains"].keys()),
                {"planning_identity", "scheduling", "metadata", "playlist", "visual_assets"},
            )
            self.assertEqual(
                set(batched[second]["domains"].keys()),
                {"planning_identity", "scheduling", "metadata", "playlist", "visual_assets"},
            )


if __name__ == "__main__":
    unittest.main()
