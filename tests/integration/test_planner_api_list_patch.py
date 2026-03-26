from __future__ import annotations

import hashlib
import importlib
import json
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlannerApiListPatch(unittest.TestCase):
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

    def _insert_release(
        self,
        env: Env,
        *,
        channel_slug: str,
        content_type: str,
        title: str,
        publish_at: str | None,
        status: str = "PLANNED",
        created_at: str = "2025-01-01T00:00:00Z",
    ) -> int:
        conn = dbm.connect(env)
        try:
            cur = conn.execute(
                """
                INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (channel_slug, content_type, title, publish_at, "notes", status, created_at, created_at),
            )
            return int(cur.lastrowid)
        finally:
            conn.close()

    def _seed_ready_context(self, env: Env, planned_release_id: int) -> None:
        conn = dbm.connect(env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch
            cur = conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                VALUES (?, 'rt', 'rd', '["tag"]', '2025-06-01T10:00:00+02:00', NULL, ?, 1.0)
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
                VALUES (701, 'darkwood-reverie', 't', 'x', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO description_templates(id, channel_slug, template_name, template_body, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (702, 'darkwood-reverie', 'd', 'x', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO video_tag_presets(id, channel_slug, preset_name, preset_body_json, status, is_default, validation_status, validation_errors_json, created_at, updated_at)
                VALUES (703, 'darkwood-reverie', 'v', '["a"]', 'ACTIVE', 1, 'VALID', '[]', '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO channel_metadata_defaults(channel_slug, default_title_template_id, default_description_template_id, default_video_tag_preset_id, created_at, updated_at)
                VALUES ('darkwood-reverie', 701, 702, 703, '2025-01-01T00:00:00Z', '2025-01-01T00:00:00Z')
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
                "INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, filename, discovered_at) VALUES ('darkwood-reverie', 'trk-ready', 'gid-ready', 'x.wav', 1.0)"
            )
            cur_job = conn.execute(
                "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'DRAFT', 'FETCH', 1, 1.0, 1.0)",
                (release_id,),
            )
            job_id = int(cur_job.lastrowid)
            if job_id != 1:
                conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (job_id, job_id))
            conn.execute(
                "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, is_active, created_at) VALUES (701, 'darkwood-reverie', ?, 'DRAFT', 'AUTO', 'balanced', 100.0, 1, 's', 'o', 'n3', 'n5', 1, '2025-01-01T00:00:00Z')",
                (job_id,),
            )
            conn.execute(
                "INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES (701, 701, 0, 1, '2025-06', 100.0, 'darkwood-reverie')"
            )
            conn.execute(
                "INSERT INTO ui_job_drafts(job_id, channel_id, title, description, tags_csv, cover_name, cover_ext, background_name, background_ext, audio_ids_text, created_at, updated_at) VALUES (?, ?, 't', 'd', 'a', NULL, NULL, 'bg', 'png', '1', 1.0, 1.0)",
                (job_id, int(ch["id"])),
            )
        finally:
            conn.close()

    def _seed_readiness_mix(self, env: Env) -> tuple[int, int, int]:
        ready_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="Ready",
            publish_at="2025-06-01T10:00:00+02:00",
            created_at="2025-01-03T00:00:00Z",
        )
        not_ready_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="NotReady",
            publish_at=None,
            created_at="2025-01-02T00:00:00Z",
        )
        blocked_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="Blocked",
            publish_at="bad-date",
            created_at="2025-01-01T00:00:00Z",
        )
        self._seed_ready_context(env, ready_id)
        return ready_id, not_ready_id, blocked_id

    def _seed_materialized_state_mix(self, env: Env) -> tuple[int, int, int]:
        materialized_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="Materialized Item",
            publish_at="2025-06-10T10:00:00+02:00",
            created_at="2025-01-04T00:00:00Z",
        )
        not_materialized_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="Not Materialized Item",
            publish_at="2025-06-11T10:00:00+02:00",
            created_at="2025-01-03T00:00:00Z",
        )
        binding_inconsistent_id = self._insert_release(
            env,
            channel_slug="darkwood-reverie",
            content_type="LONG",
            title="Binding Inconsistent Item",
            publish_at="2025-06-12T10:00:00+02:00",
            created_at="2025-01-02T00:00:00Z",
        )
        conn = dbm.connect(env)
        try:
            channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
            release_id = int(
                conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'mat', '', '[]', NULL, NULL, 'mat-seed', 1.0)
                    """,
                    (channel_id,),
                ).lastrowid
            )
            inconsistent_release_id = int(
                conn.execute(
                    """
                    INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                    VALUES(?, 'inconsistent', '', '[]', NULL, NULL, 'mat-seed-inc', 1.0)
                    """,
                    (channel_id,),
                ).lastrowid
            )
            conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, materialized_id))
            conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (inconsistent_release_id, binding_inconsistent_id))
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("DELETE FROM releases WHERE id = ?", (inconsistent_release_id,))
            conn.execute("PRAGMA foreign_keys=ON")
        finally:
            conn.close()
        return materialized_id, not_materialized_id, binding_inconsistent_id

    def _get_release_channel_slug(self, env: Env, release_id: int) -> str:
        conn = dbm.connect(env)
        try:
            row = conn.execute("SELECT channel_slug FROM planned_releases WHERE id = ?", (release_id,)).fetchone()
            assert row is not None
            return str(row["channel_slug"])
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

    def test_list_filters_sort_search_and_pagination(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Dream Sequence",
                publish_at="2025-01-02T10:00:00+02:00",
            )
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="SHORT",
                title="Night Pulse",
                publish_at="2025-01-03T10:00:00+02:00",
            )
            self._insert_release(
                env,
                channel_slug="channel-b",
                content_type="LONG",
                title="Dawn Echo",
                publish_at="2025-01-04T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/planner/releases?channel_slug=darkwood-reverie&content_type=LONG&q=dream&sort_by=publish_at&sort_dir=asc&page=1&page_size=1",
                headers=auth,
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["pagination"], {"page": 1, "page_size": 1, "total": 1})
            self.assertEqual([item["title"] for item in body["items"]], ["Dream Sequence"])

    def test_patch_locked_returns_409(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Locked",
                publish_at="2025-01-02T10:00:00+02:00",
                status="LOCKED",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(f"/v1/planner/releases/{rid}", json={"title": "Updated"}, headers=auth)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_RELEASE_LOCKED")

    def test_patch_rejects_status_field(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Editable",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(f"/v1/planner/releases/{rid}", json={"status": "LOCKED"}, headers=auth)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_FIELD_NOT_EDITABLE")

    def test_patch_uniqueness_conflict_returns_409(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            first = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="First",
                publish_at="2025-01-02T10:00:00+02:00",
            )
            second = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Second",
                publish_at="2025-01-03T10:00:00+02:00",
            )
            self.assertNotEqual(first, second)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(
                f"/v1/planner/releases/{second}",
                json={"publish_at": "2025-01-02T10:00:00"},
                headers=auth,
            )
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLR_CONFLICT")

    def test_patch_malformed_json_returns_400_invalid_input(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Malformed",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.patch(
                f"/v1/planner/releases/{rid}",
                data='{"title": ',
                headers={**auth, "Content-Type": "application/json"},
            )
            self.assertEqual(resp.status_code, 400)
            self.assertIn("error", resp.json())
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_patch_empty_or_non_object_payload_returns_400_invalid_input(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Payload",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            empty_resp = client.patch(
                f"/v1/planner/releases/{rid}",
                data="",
                headers={**auth, "Content-Type": "application/json"},
            )
            self.assertEqual(empty_resp.status_code, 400)
            self.assertIn("error", empty_resp.json())
            self.assertEqual(empty_resp.json()["error"]["code"], "PLR_INVALID_INPUT")

            list_resp = client.patch(f"/v1/planner/releases/{rid}", json=["not", "object"], headers=auth)
            self.assertEqual(list_resp.status_code, 400)
            self.assertIn("error", list_resp.json())
            self.assertEqual(list_resp.json()["error"]["code"], "PLR_INVALID_INPUT")

    def test_patch_channel_slug_empty_string_rejected_and_not_saved(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Empty Slug",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            before = self._get_release_channel_slug(env, rid)
            resp = client.patch(f"/v1/planner/releases/{rid}", json={"channel_slug": ""}, headers=auth)
            after = self._get_release_channel_slug(env, rid)

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")
            self.assertEqual(before, "darkwood-reverie")
            self.assertEqual(after, "darkwood-reverie")

    def test_patch_channel_slug_whitespace_rejected_and_not_saved(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            rid = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Whitespace Slug",
                publish_at="2025-01-02T10:00:00+02:00",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            before = self._get_release_channel_slug(env, rid)
            resp = client.patch(f"/v1/planner/releases/{rid}", json={"channel_slug": "   "}, headers=auth)
            after = self._get_release_channel_slug(env, rid)

            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PLR_INVALID_INPUT")
            self.assertEqual(before, "darkwood-reverie")
            self.assertEqual(after, "darkwood-reverie")

    def test_list_include_readiness_true_returns_row_readiness_and_summary(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?include_readiness=true&page=1&page_size=2", headers=auth)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(len(resp.json()["items"]), 2)
            readiness = resp.json()["items"][0]["readiness"]
            self.assertEqual(
                set(readiness.keys()),
                {
                    "aggregate_status",
                    "blocked_domains_count",
                    "not_ready_domains_count",
                    "computed_at",
                    "primary_reason",
                    "primary_remediation_hint",
                },
            )
            summary = resp.json()["readiness_summary"]
            self.assertEqual(summary["scope_total"], 3)
            self.assertEqual(summary["ready_for_materialization"], 1)
            self.assertEqual(summary["not_ready"], 1)
            self.assertEqual(summary["blocked"], 1)
            self.assertEqual(summary["attention_count"], 2)
            self.assertEqual(summary["unavailable"], 0)
            self.assertTrue(summary["computed_at"])

    def test_include_readiness_omitted_preserves_existing_behavior(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?page=1&page_size=2", headers=auth)
            self.assertEqual(resp.status_code, 200)
            self.assertNotIn("readiness_summary", resp.json())
            self.assertNotIn("readiness", resp.json()["items"][0])

    def test_readiness_status_filters_not_ready_blocked_ready(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            ready_id, not_ready_id, blocked_id = self._seed_readiness_mix(env)
            self.assertTrue(all([ready_id, not_ready_id, blocked_id]))
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            blocked = client.get("/v1/planner/releases?readiness_status=BLOCKED&page=1&page_size=1", headers=auth)
            self.assertEqual(blocked.status_code, 200)
            self.assertEqual(blocked.json()["pagination"]["total"], 1)
            self.assertEqual(blocked.json()["items"][0]["title"], "Blocked")
            not_ready = client.get("/v1/planner/releases?readiness_status=NOT_READY&page=1&page_size=5", headers=auth)
            self.assertEqual(not_ready.status_code, 200)
            self.assertEqual(not_ready.json()["pagination"]["total"], 1)
            self.assertEqual(not_ready.json()["items"][0]["title"], "NotReady")
            ready = client.get("/v1/planner/releases?readiness_status=READY_FOR_MATERIALIZATION&page=1&page_size=5", headers=auth)
            self.assertEqual(ready.status_code, 200)
            self.assertEqual(ready.json()["pagination"]["total"], 1)
            self.assertEqual(ready.json()["items"][0]["title"], "Ready")
            comma = client.get(
                "/v1/planner/releases?readiness_status=NOT_READY,BLOCKED&include_readiness=true&page=1&page_size=10",
                headers=auth,
            )
            self.assertEqual(comma.status_code, 200)
            self.assertEqual(comma.json()["pagination"]["total"], 2)
            self.assertEqual(comma.json()["readiness_summary"]["attention_count"], 2)

    def test_invalid_readiness_status_returns_prs_invalid_readiness_filter(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?readiness_status=NOPE&page=1&page_size=10", headers=auth)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PRS_INVALID_READINESS_FILTER")

    def test_unavailable_row_does_not_break_list_and_summary_counts_unavailable(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            from services.factory_api.planner import PlannedReleaseReadinessService

            original_eval = PlannedReleaseReadinessService.evaluate_many

            def flaky_eval(self, *, planned_release_ids):
                if planned_release_ids:
                    placeholders = ",".join("?" for _ in planned_release_ids)
                    rows = self._conn.execute(
                        f"SELECT title FROM planned_releases WHERE id IN ({placeholders})",
                        tuple(int(item) for item in planned_release_ids),
                    ).fetchall()
                    if any(str(row["title"]) == "Blocked" for row in rows):
                        raise RuntimeError("synthetic readiness failure")
                return original_eval(self, planned_release_ids=planned_release_ids)

            with patch("services.factory_api.planner.PlannedReleaseReadinessService.evaluate_many", new=flaky_eval):
                resp = client.get("/v1/planner/releases?include_readiness=true&page=1&page_size=5", headers=auth)
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["pagination"]["total"], 3)
            self.assertEqual(body["readiness_summary"]["unavailable"], 1)
            self.assertEqual(body["readiness_summary"]["ready_for_materialization"], 1)
            self.assertEqual(body["readiness_summary"]["not_ready"], 1)
            self.assertEqual(body["readiness_summary"]["blocked"], 0)
            self.assertEqual(body["readiness_summary"]["attention_count"], 1)
            blocked_row = next(item for item in body["items"] if item["title"] == "Blocked")
            self.assertEqual(blocked_row["readiness"]["aggregate_status"], None)
            self.assertEqual(blocked_row["readiness"]["error"]["code"], "PRS_READINESS_UNAVAILABLE")

    def test_unavailable_outside_filtered_scope_is_not_counted_in_summary(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            from services.factory_api.planner import PlannedReleaseReadinessService

            original_eval = PlannedReleaseReadinessService.evaluate_many

            def flaky_eval(self, *, planned_release_ids):
                if planned_release_ids:
                    placeholders = ",".join("?" for _ in planned_release_ids)
                    rows = self._conn.execute(
                        f"SELECT title FROM planned_releases WHERE id IN ({placeholders})",
                        tuple(int(item) for item in planned_release_ids),
                    ).fetchall()
                    if any(str(row["title"]) == "Blocked" for row in rows):
                        raise RuntimeError("synthetic readiness failure")
                return original_eval(self, planned_release_ids=planned_release_ids)

            with patch("services.factory_api.planner.PlannedReleaseReadinessService.evaluate_many", new=flaky_eval):
                resp = client.get(
                    "/v1/planner/releases?include_readiness=true&readiness_status=READY_FOR_MATERIALIZATION&page=1&page_size=5",
                    headers=auth,
                )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["pagination"]["total"], 1)
            self.assertEqual([item["title"] for item in body["items"]], ["Ready"])
            self.assertEqual(body["readiness_summary"]["scope_total"], 1)
            self.assertEqual(body["readiness_summary"]["ready_for_materialization"], 1)
            self.assertEqual(body["readiness_summary"]["not_ready"], 0)
            self.assertEqual(body["readiness_summary"]["blocked"], 0)
            self.assertEqual(body["readiness_summary"]["attention_count"], 0)
            self.assertEqual(body["readiness_summary"]["unavailable"], 0)

    def test_readiness_problem_filters_and_status_precedence(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            blocked = client.get("/v1/planner/releases?readiness_problem=blocked_only&page=1&page_size=5", headers=auth)
            self.assertEqual(blocked.status_code, 200)
            self.assertEqual([item["title"] for item in blocked.json()["items"]], ["Blocked"])

            attention = client.get("/v1/planner/releases?readiness_problem=attention_required&page=1&page_size=5", headers=auth)
            self.assertEqual(attention.status_code, 200)
            self.assertEqual(attention.json()["pagination"]["total"], 2)
            self.assertEqual({item["title"] for item in attention.json()["items"]}, {"Blocked", "NotReady"})

            ready_only = client.get("/v1/planner/releases?readiness_problem=ready_only&page=1&page_size=5", headers=auth)
            self.assertEqual(ready_only.status_code, 200)
            self.assertEqual([item["title"] for item in ready_only.json()["items"]], ["Ready"])

            precedence = client.get(
                "/v1/planner/releases?readiness_status=BLOCKED&readiness_problem=ready_only&page=1&page_size=5",
                headers=auth,
            )
            self.assertEqual(precedence.status_code, 200)
            self.assertEqual([item["title"] for item in precedence.json()["items"]], ["Blocked"])

    def test_sort_by_readiness_priority_attention_first_and_ready_first(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/planner/releases?sort_by=readiness_priority&readiness_priority=attention_first&page=1&page_size=5",
                headers=auth,
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual([item["title"] for item in resp.json()["items"]], ["Blocked", "NotReady", "Ready"])

            ready_first = client.get(
                "/v1/planner/releases?sort_by=readiness_priority&readiness_priority=ready_first&page=1&page_size=5",
                headers=auth,
            )
            self.assertEqual(ready_first.status_code, 200)
            self.assertEqual([item["title"] for item in ready_first.json()["items"]], ["Ready", "NotReady", "Blocked"])

    def test_sort_by_readiness_priority_is_read_only(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            before = self._snapshot(env)
            resp = client.get(
                "/v1/planner/releases?sort_by=readiness_priority&readiness_priority=attention_first&include_readiness_summary=true&page=1&page_size=5",
                headers=auth,
            )
            after = self._snapshot(env)

            self.assertEqual(resp.status_code, 200)
            self.assertEqual(before, after)

    def test_invalid_readiness_problem_and_sort_return_expected_errors(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_readiness_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            invalid_problem = client.get("/v1/planner/releases?readiness_problem=nope&page=1&page_size=5", headers=auth)
            self.assertEqual(invalid_problem.status_code, 400)
            self.assertEqual(invalid_problem.json()["error"]["code"], "PRS_INVALID_READINESS_FILTER")

            missing_priority = client.get("/v1/planner/releases?sort_by=readiness_priority&page=1&page_size=5", headers=auth)
            self.assertEqual(missing_priority.status_code, 400)
            self.assertEqual(missing_priority.json()["error"]["code"], "PRS_INVALID_READINESS_SORT")

            invalid_priority = client.get(
                "/v1/planner/releases?sort_by=readiness_priority&readiness_priority=nope&page=1&page_size=5",
                headers=auth,
            )
            self.assertEqual(invalid_priority.status_code, 400)
            self.assertEqual(invalid_priority.json()["error"]["code"], "PRS_INVALID_READINESS_SORT")

    def test_readiness_status_not_ready_respects_non_readiness_sort(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Zulu Missing Schedule",
                publish_at=None,
                created_at="2025-01-03T00:00:00Z",
            )
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Alpha Missing Schedule",
                publish_at=None,
                created_at="2025-01-02T00:00:00Z",
            )
            self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Blocked Invalid Schedule",
                publish_at="bad-date",
                created_at="2025-01-01T00:00:00Z",
            )

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            resp = client.get(
                "/v1/planner/releases?readiness_status=NOT_READY&sort_by=title&sort_dir=asc&page=1&page_size=10",
                headers=auth,
            )
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["pagination"]["total"], 2)
            self.assertEqual(
                [item["title"] for item in resp.json()["items"]],
                ["Alpha Missing Schedule", "Zulu Missing Schedule"],
            )

    def test_list_includes_materialization_summary_and_diagnostics(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_materialized_state_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?include_readiness=true&page=1&page_size=10", headers=auth)
            self.assertEqual(resp.status_code, 200)
            first = resp.json()["items"][0]
            self.assertIn("materialization_state_summary", first)
            self.assertIn("binding_diagnostics", first)
            self.assertIn("materialization_state", first["materialization_state_summary"])
            self.assertIn("release_id", first["materialization_state_summary"])
            self.assertIn("action_reason", first["materialization_state_summary"])

    def test_materialized_state_filter_materialized_not_materialized_binding_inconsistent(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_materialized_state_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            materialized = client.get("/v1/planner/releases?materialized_state=materialized&page=1&page_size=10", headers=auth)
            self.assertEqual(materialized.status_code, 200)
            self.assertEqual([item["title"] for item in materialized.json()["items"]], ["Materialized Item"])

            not_materialized = client.get(
                "/v1/planner/releases?materialized_state=not_materialized&page=1&page_size=10",
                headers=auth,
            )
            self.assertEqual(not_materialized.status_code, 200)
            self.assertIn("Not Materialized Item", [item["title"] for item in not_materialized.json()["items"]])

            inconsistent = client.get(
                "/v1/planner/releases?materialized_state=binding_inconsistent&page=1&page_size=10",
                headers=auth,
            )
            self.assertEqual(inconsistent.status_code, 200)
            self.assertEqual([item["title"] for item in inconsistent.json()["items"]], ["Binding Inconsistent Item"])

    def test_invalid_materialized_state_filter_returns_400(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            self._seed_materialized_state_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?materialized_state=unknown&page=1&page_size=10", headers=auth)
            self.assertEqual(resp.status_code, 400)
            self.assertEqual(resp.json()["error"]["code"], "PRS_INVALID_MATERIALIZED_STATE_FILTER")

    def test_materialized_state_filter_applies_before_pagination(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            for idx in range(5):
                self._insert_release(
                    env,
                    channel_slug="darkwood-reverie",
                    content_type="LONG",
                    title=f"Unbound {idx}",
                    publish_at=f"2025-06-{10 + idx:02d}T10:00:00+02:00",
                    created_at=f"2025-01-{10 + idx:02d}T00:00:00Z",
                )
            target_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Late Materialized Match",
                publish_at="2025-06-30T10:00:00+02:00",
                created_at="2025-01-01T00:00:00Z",
            )
            conn = dbm.connect(env)
            try:
                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'target', '', '[]', NULL, NULL, 'page-scope-target', 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (release_id, target_id))
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)

            unfiltered = client.get("/v1/planner/releases?page=1&page_size=2", headers=auth)
            self.assertEqual(unfiltered.status_code, 200)
            self.assertGreater(unfiltered.json()["pagination"]["total"], 2)
            self.assertNotIn("Late Materialized Match", [item["title"] for item in unfiltered.json()["items"]])

            filtered = client.get("/v1/planner/releases?materialized_state=materialized&page=1&page_size=2", headers=auth)
            self.assertEqual(filtered.status_code, 200)
            self.assertEqual(filtered.json()["pagination"]["total"], 1)
            self.assertEqual([item["title"] for item in filtered.json()["items"]], ["Late Materialized Match"])

    def test_planner_create_job_endpoint_delegates_to_canonical_service(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_id, _, _ = self._seed_materialized_state_mix(env)
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            with patch("services.factory_api.planner.ReleaseJobCreationService.create_or_select") as mocked_create_or_select:
                mocked_create_or_select.return_value = type("Out", (), {
                    "release_id": 999,
                    "result": "RETURNED_EXISTING_OPEN_JOB",
                    "job": {"id": 777, "release_id": 999, "channel_slug": "darkwood-reverie", "status": "DRAFT"},
                    "current_open_relation": {"release_id": 999, "job_id": 777},
                    "job_creation_state_summary": {"job_creation_state": "HAS_OPEN_JOB", "job_id": 777, "action_reason": None},
                    "open_job_diagnostics": {
                        "release_id": 999,
                        "current_open_job_id": 777,
                        "linked_job_exists": True,
                        "open_jobs_count": 1,
                        "invariant_status": "HAS_OPEN_JOB",
                        "invariant_reason": None,
                    },
                })()
                resp = client.post(f"/v1/planner/planned-releases/{planned_id}/create-job", headers=auth)
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json()["result"], "RETURNED_EXISTING_OPEN_JOB")
            self.assertIn("job_creation_state_summary", resp.json())
            self.assertIn("open_job_diagnostics", resp.json())
            mocked_create_or_select.assert_called_once()

    def test_planner_create_job_endpoint_fails_when_not_materialized(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            not_materialized_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Not Materialized",
                publish_at="2025-06-01T10:00:00+02:00",
            )
            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.post(f"/v1/planner/planned-releases/{not_materialized_id}/create-job", headers=auth)
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PRJ_PLANNED_RELEASE_NOT_MATERIALIZED")

    def test_list_includes_job_creation_payload_and_filter_works(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            planned_id, not_materialized_id, binding_inconsistent_id = self._seed_materialized_state_mix(env)
            inconsistent_open_id = self._insert_release(
                env,
                channel_slug="darkwood-reverie",
                content_type="LONG",
                title="Open Inconsistent",
                publish_at="2025-06-15T10:00:00+02:00",
            )
            conn = dbm.connect(env)
            try:
                row = conn.execute("SELECT materialized_release_id FROM planned_releases WHERE id = ?", (planned_id,)).fetchone()
                release_id = int(row["materialized_release_id"])
                job_id = int(conn.execute(
                    "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'DRAFT', 'DRAFT', 1, 1.0, 1.0)",
                    (release_id,),
                ).lastrowid)
                if job_id != 1:
                    conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (job_id, job_id))
                conn.execute("UPDATE releases SET current_open_job_id = ? WHERE id = ?", (job_id, release_id))

                channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
                bad_release_id = int(
                    conn.execute(
                        """
                        INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
                        VALUES(?, 'bad-open', '', '[]', NULL, NULL, 'bad-open-seed', 1.0)
                        """,
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute("UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?", (bad_release_id, inconsistent_open_id))
                j1 = int(
                    conn.execute(
                        "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'DRAFT', 'DRAFT', 1, 1.0, 1.0)",
                        (bad_release_id,),
                    ).lastrowid
                )
                j2 = int(
                    conn.execute(
                        "INSERT INTO jobs(release_id, job_type, state, stage, root_job_id, created_at, updated_at) VALUES (?, 'RENDER', 'RENDERING', 'RENDERING', 1, 1.0, 1.0)",
                        (bad_release_id,),
                    ).lastrowid
                )
                if j1 != 1:
                    conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (j1, j1))
                if j2 != 1:
                    conn.execute("UPDATE jobs SET root_job_id = ? WHERE id = ?", (j2, j2))
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header(env.basic_user, env.basic_pass)
            resp = client.get("/v1/planner/releases?page=1&page_size=20", headers=auth)
            self.assertEqual(resp.status_code, 200)
            rows = {int(item["id"]): item for item in resp.json()["items"]}
            self.assertIn("job_creation_state_summary", rows[planned_id])
            self.assertIn("open_job_diagnostics", rows[planned_id])
            self.assertEqual(rows[planned_id]["job_creation_state_summary"]["job_creation_state"], "HAS_OPEN_JOB")
            self.assertEqual(rows[not_materialized_id]["job_creation_state_summary"]["job_creation_state"], "ACTION_DISABLED")
            self.assertEqual(rows[binding_inconsistent_id]["job_creation_state_summary"]["job_creation_state"], "ACTION_DISABLED")
            self.assertEqual(rows[inconsistent_open_id]["job_creation_state_summary"]["job_creation_state"], "MULTIPLE_OPEN_INCONSISTENT")

            has_open = client.get("/v1/planner/releases?job_creation_state=has_open_job&page=1&page_size=20", headers=auth)
            self.assertEqual(has_open.status_code, 200)
            self.assertEqual([item["id"] for item in has_open.json()["items"]], [planned_id])

            no_open = client.get("/v1/planner/releases?job_creation_state=no_open_job&page=1&page_size=20", headers=auth)
            self.assertEqual(no_open.status_code, 200)
            self.assertEqual(no_open.json()["items"], [])

            inconsistent = client.get(
                "/v1/planner/releases?job_creation_state=inconsistent_open_job_state&page=1&page_size=20",
                headers=auth,
            )
            self.assertEqual(inconsistent.status_code, 200)
            self.assertEqual([item["id"] for item in inconsistent.json()["items"]], [inconsistent_open_id])


if __name__ == "__main__":
    unittest.main()
