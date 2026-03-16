from __future__ import annotations

import importlib
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from services.common import db as dbm

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlaylistBuilderPreviewApplyApi(unittest.TestCase):
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

    def _seed_tracks(self) -> None:
        conn = dbm.connect(self.env)
        try:
            ts = dbm.now_ts()
            for pk, tid, duration, month in [
                (201, "t201", 240.0, "2024-01"),
                (202, "t202", 260.0, "2024-01"),
                (203, "t203", 280.0, "2024-02"),
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


    def test_create_flow_builder_draft_preview_without_audio_then_apply_updates_playlist(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            conn_lookup = dbm.connect(self.env)
            try:
                ch = dbm.get_channel_by_slug(conn_lookup, "darkwood-reverie")
                assert ch is not None
                channel_id = int(ch["id"])
            finally:
                conn_lookup.close()

            create_resp = client.post(
                "/v1/ui/jobs/playlist-builder-draft",
                headers=headers,
                json={
                    "channel_id": channel_id,
                    "title": "plb-create-flow",
                    "description": "",
                    "tags_csv": "",
                    "cover_name": "",
                    "cover_ext": "",
                    "background_name": "",
                    "background_ext": "",
                },
            )
            self.assertEqual(create_resp.status_code, 200)
            job_id = int(create_resp.json()["job_id"])

            preview_one = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            self.assertEqual(preview_one.status_code, 200)
            preview_id_one = preview_one.json()["preview_id"]

            preview_two = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            self.assertEqual(preview_two.status_code, 200)

            apply_resp = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/apply",
                headers=headers,
                json={"preview_id": preview_id_one},
            )
            self.assertEqual(apply_resp.status_code, 200)

            conn = dbm.connect(self.env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                self.assertIsNotNone(draft)
                self.assertRegex(str(draft["audio_ids_text"]), r"^\d+( \d+)*$")
                draft_count = conn.execute("SELECT COUNT(*) AS c FROM ui_job_drafts WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(int(draft_count["c"]), 1)
            finally:
                conn.close()

    def test_preview_and_apply_success_and_idempotent_reapply(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertIn("preview_id", body)
            self.assertIn("summary", body)
            self.assertGreater(len(body["tracks"]), 0)
            self.assertIn("month_batch", body["tracks"][0])
            self.assertIn(body["tracks"][0]["month_batch"], {"2024-01", "2024-02"})
            preview_id = body["preview_id"]

            apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(apply_resp.status_code, 200)
            first_history_id = int(apply_resp.json()["draft_history_id"])

            conn = dbm.connect(self.env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                self.assertIsNotNone(draft)
                self.assertRegex(str(draft["audio_ids_text"]), r"^\d+( \d+)*$")
                histories = conn.execute(
                    "SELECT id, source_preview_id, history_stage, is_active FROM playlist_history WHERE job_id = ? ORDER BY id ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual(len(histories), 1)
                self.assertEqual(histories[0]["source_preview_id"], preview_id)
                self.assertEqual(histories[0]["history_stage"], "DRAFT")
                items = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history_items WHERE history_id = ?",
                    (first_history_id,),
                ).fetchone()
                self.assertGreater(int(items["c"]), 0)
            finally:
                conn.close()

            reapply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(reapply_resp.status_code, 200)
            self.assertEqual(int(reapply_resp.json()["draft_history_id"]), first_history_id)

            conn = dbm.connect(self.env)
            try:
                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history WHERE source_preview_id = ? AND history_stage = 'DRAFT'",
                    (preview_id,),
                ).fetchone()
                self.assertEqual(int(count["c"]), 1)
            finally:
                conn.close()

    def test_preview_smart_mode_uses_refinement_summary_and_contract(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb-smart")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "smart", "min_duration_min": 10, "max_duration_min": 15}},
            )

            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertIn("preview_id", body)
            self.assertIn("summary", body)
            self.assertGreater(len(body.get("tracks", [])), 0)

            summary = body["summary"]
            self.assertEqual(summary["generation_mode"], "smart")
            duration = summary["duration"]
            self.assertEqual(duration["min"], 10)
            self.assertEqual(duration["max"], 15)
            self.assertEqual(duration["target"], 12.5)
            self.assertEqual(duration["tolerance"], 5)
            self.assertIsInstance(duration["achieved"], float)
            self.assertAlmostEqual(duration["deviation_from_target"], duration["achieved"] - duration["target"], places=3)
            self.assertTrue(any("top-" in w for w in summary.get("warnings", [])))

    def test_preview_curated_mode_contract_and_apply_flow(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb-curated")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "curated", "min_duration_min": 10, "max_duration_min": 15}},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertEqual(body["summary"]["generation_mode"], "curated")
            self.assertGreater(len(body.get("tracks", [])), 0)
            self.assertTrue(any("best-of-" in w for w in body["summary"].get("warnings", [])))

            apply_resp = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/apply",
                headers=headers,
                json={"preview_id": body["preview_id"]},
            )
            self.assertEqual(apply_resp.status_code, 200)
            self.assertTrue(apply_resp.json()["playlist_applied"])

    def test_preview_curated_guardrail_returns_explicit_failure(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb-curated-limit")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            from services.playlist_builder.composition import CuratedOptimizationLimitExceeded

            with patch(
                "services.playlist_builder.core.compose_curated",
                side_effect=CuratedOptimizationLimitExceeded("Curated composition exceeded guardrail: max_iterations=360"),
            ):
                preview = client.post(
                    f"/v1/playlist-builder/jobs/{job_id}/preview",
                    headers=headers,
                    json={"override": {"generation_mode": "curated", "min_duration_min": 10, "max_duration_min": 15}},
                )

            self.assertEqual(preview.status_code, 422)
            payload = preview.json()
            self.assertEqual(payload["error"]["code"], "PLB_CURATED_LIMIT_EXCEEDED")
            self.assertIn("guardrail", payload["error"]["message"].lower())

    def test_preview_expired_cannot_apply(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            preview_id = preview.json()["preview_id"]

            conn = dbm.connect(self.env)
            try:
                conn.execute("UPDATE playlist_build_previews SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (preview_id,))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(apply_resp.status_code, 409)
            self.assertEqual(apply_resp.json()["error"]["code"], "PLB_PREVIEW_EXPIRED")


    def test_preview_expired_apply_persists_expired_status(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            preview_id = preview.json()["preview_id"]

            conn = dbm.connect(self.env)
            try:
                conn.execute("UPDATE playlist_build_previews SET expires_at = '2000-01-01T00:00:00+00:00' WHERE id = ?", (preview_id,))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(apply_resp.status_code, 409)
            self.assertEqual(apply_resp.json()["error"]["code"], "PLB_PREVIEW_EXPIRED")

            conn = dbm.connect(self.env)
            try:
                status_row = conn.execute("SELECT status FROM playlist_build_previews WHERE id = ?", (preview_id,)).fetchone()
                self.assertEqual(str(status_row["status"]), "EXPIRED")
            finally:
                conn.close()

    def test_apply_stale_preview_with_existing_draft_history_is_idempotent(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            preview_id = preview.json()["preview_id"]

            conn = dbm.connect(self.env)
            try:
                row = conn.execute(
                    "SELECT effective_brief_json, preview_result_json FROM playlist_build_previews WHERE id = ?",
                    (preview_id,),
                ).fetchone()
                brief = dbm.json_loads(str(row["effective_brief_json"]))
                result = dbm.json_loads(str(row["preview_result_json"]))
                tracks = [int(v) for v in result["ordered_track_pks"]]
                conn.execute(
                    "INSERT INTO playlist_history(channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        brief["channel_slug"],
                        job_id,
                        "DRAFT",
                        preview_id,
                        brief["generation_mode"],
                        brief["strictness_mode"],
                        float(result["achieved_duration_sec"]),
                        len(tracks),
                        "seed-a",
                        "seed-b",
                        "seed-c",
                        "seed-d",
                        None,
                        None,
                        1,
                        "2025-01-01T00:00:00+00:00",
                    ),
                )
                history_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                conn.execute("UPDATE playlist_build_previews SET status = 'PREVIEW' WHERE id = ?", (preview_id,))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(apply_resp.status_code, 200)
            self.assertEqual(int(apply_resp.json()["draft_history_id"]), history_id)

            conn = dbm.connect(self.env)
            try:
                count = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history WHERE source_preview_id = ? AND history_stage = 'DRAFT'",
                    (preview_id,),
                ).fetchone()
                self.assertEqual(int(count["c"]), 1)
            finally:
                conn.close()

    def test_apply_fails_when_preview_tracks_missing_without_partial_write(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            preview_id = preview.json()["preview_id"]

            conn = dbm.connect(self.env)
            try:
                draft_before = dbm.get_ui_job_draft(conn, job_id)
                self.assertIsNotNone(draft_before)
                audio_before = str(draft_before["audio_ids_text"])
                preview_row = conn.execute("SELECT preview_result_json FROM playlist_build_previews WHERE id = ?", (preview_id,)).fetchone()
                ordered = dbm.json_loads(str(preview_row["preview_result_json"]))["ordered_track_pks"]
                missing_pk = int(ordered[0])
                conn.execute("DELETE FROM track_analysis_flat WHERE track_pk = ?", (missing_pk,))
                conn.commit()
            finally:
                conn.close()

            apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
            self.assertEqual(apply_resp.status_code, 409)
            self.assertEqual(apply_resp.json()["error"]["code"], "PLB_APPLY_CONFLICT")

            conn = dbm.connect(self.env)
            try:
                draft_after = dbm.get_ui_job_draft(conn, job_id)
                self.assertEqual(str(draft_after["audio_ids_text"]), audio_before)
                status_row = conn.execute("SELECT status FROM playlist_build_previews WHERE id = ?", (preview_id,)).fetchone()
                self.assertEqual(str(status_row["status"]), "PREVIEW")
                count = conn.execute("SELECT COUNT(*) AS c FROM playlist_history WHERE source_preview_id = ?", (preview_id,)).fetchone()
                self.assertEqual(int(count["c"]), 0)
            finally:
                conn.close()

    def test_apply_history_write_failure_is_atomic(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            preview_id = preview.json()["preview_id"]

            with patch("services.playlist_builder.workflow._insert_draft_history", side_effect=RuntimeError("boom")):
                apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})

            self.assertEqual(apply_resp.status_code, 500)
            self.assertEqual(apply_resp.json()["error"]["code"], "PLB_HISTORY_WRITE_FAILED")

            conn = dbm.connect(self.env)
            try:
                draft = dbm.get_ui_job_draft(conn, job_id)
                self.assertEqual(str(draft["audio_ids_text"]), "1")
                status_row = conn.execute("SELECT status FROM playlist_build_previews WHERE id = ?", (preview_id,)).fetchone()
                self.assertEqual(status_row["status"], "PREVIEW")
                count = conn.execute("SELECT COUNT(*) AS c FROM playlist_history WHERE source_preview_id = ?", (preview_id,)).fetchone()
                self.assertEqual(int(count["c"]), 0)
            finally:
                conn.close()

    def test_preview_failure_writes_no_history(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft(channel_slug="darkwood-reverie", title="plb")
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=headers,
                json={"override": {"generation_mode": "safe", "required_tags": ["impossible"]}},
            )
            self.assertEqual(preview.status_code, 422)
            self.assertEqual(preview.json()["error"]["code"], "PLB_NO_CANDIDATES")

            conn = dbm.connect(self.env)
            try:
                history_count = conn.execute("SELECT COUNT(*) AS c FROM playlist_history WHERE job_id = ?", (job_id,)).fetchone()
                self.assertEqual(int(history_count["c"]), 0)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
