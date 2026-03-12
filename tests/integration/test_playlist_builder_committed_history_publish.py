from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.playlist_builder.history import list_effective_history

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestPlaylistBuilderCommittedHistoryPublish(unittest.TestCase):
    def _new_client(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return TestClient(mod.app)

    def _create_ui_draft(self) -> int:
        conn = dbm.connect(self.env)
        try:
            ch = dbm.get_channel_by_slug(conn, "darkwood-reverie")
            assert ch is not None
            return dbm.create_ui_job_draft(
                conn,
                channel_id=int(ch["id"]),
                title="plb-commit",
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
                (401, "t401", 240.0, "2024-01"),
                (402, "t402", 260.0, "2024-01"),
                (403, "t403", 280.0, "2024-02"),
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

    def _set_wait_approval(self, job_id: int) -> None:
        conn = dbm.connect(self.env)
        try:
            dbm.update_job_state(conn, job_id, state="WAIT_APPROVAL", stage="APPROVAL")
            conn.commit()
        finally:
            conn.close()

    def _preview_apply(self, client: TestClient, headers: dict[str, str], job_id: int) -> None:
        preview = client.post(
            f"/v1/playlist-builder/jobs/{job_id}/preview",
            headers=headers,
            json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
        )
        self.assertEqual(preview.status_code, 200)
        preview_id = preview.json()["preview_id"]
        apply_resp = client.post(f"/v1/playlist-builder/jobs/{job_id}/apply", headers=headers, json={"preview_id": preview_id})
        self.assertEqual(apply_resp.status_code, 200)

    def test_mark_published_writes_committed_history_once_with_items(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            self._preview_apply(client, headers, job_id)
            self._set_wait_approval(job_id)

            published = client.post(f"/v1/jobs/{job_id}/mark_published", headers=headers, json={})
            self.assertEqual(published.status_code, 200)
            published_again = client.post(f"/v1/jobs/{job_id}/mark_published", headers=headers, json={})
            self.assertEqual(published_again.status_code, 409)

            conn = dbm.connect(self.env)
            try:
                rows = conn.execute(
                    "SELECT id, history_stage FROM playlist_history WHERE job_id = ? ORDER BY id ASC",
                    (job_id,),
                ).fetchall()
                self.assertEqual([str(r["history_stage"]) for r in rows], ["DRAFT", "COMMITTED"])

                committed_id = int(rows[1]["id"])
                committed_items = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history_items WHERE history_id = ?",
                    (committed_id,),
                ).fetchone()
                self.assertGreater(int(committed_items["c"]), 0)
            finally:
                conn.close()

    def test_effective_history_prefers_committed_for_same_job_after_publish(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            self._preview_apply(client, headers, job_id)
            self._set_wait_approval(job_id)
            published = client.post(f"/v1/jobs/{job_id}/mark_published", headers=headers, json={})
            self.assertEqual(published.status_code, 200)

            conn = dbm.connect(self.env)
            try:
                hist = list_effective_history(conn, channel_slug="darkwood-reverie", window=10)
                target = next(h for h in hist if h.job_id == job_id)
                self.assertEqual(target.history_stage, "COMMITTED")
            finally:
                conn.close()

    def test_non_published_paths_do_not_create_committed_history(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            self._preview_apply(client, headers, job_id)
            self._set_wait_approval(job_id)

            approve_resp = client.post(f"/v1/jobs/{job_id}/approve", headers=headers, json={"comment": "ok"})
            self.assertEqual(approve_resp.status_code, 200)

            conn = dbm.connect(self.env)
            try:
                approved_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history WHERE job_id = ? AND history_stage = 'COMMITTED'",
                    (job_id,),
                ).fetchone()
                self.assertEqual(int(approved_count["c"]), 0)
            finally:
                conn.close()

            conn = dbm.connect(self.env)
            try:
                wait_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history WHERE job_id = ? AND history_stage = 'COMMITTED'",
                    (job_id,),
                ).fetchone()
                self.assertEqual(int(wait_count["c"]), 0)
            finally:
                conn.close()

    def test_mark_published_requires_applied_draft_history(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            job_id = self._create_ui_draft()
            self._set_wait_approval(job_id)
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            resp = client.post(f"/v1/jobs/{job_id}/mark_published", headers=headers, json={})
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLB_COMMITTED_HISTORY_MISSING_DRAFT")

            conn = dbm.connect(self.env)
            try:
                job = dbm.get_job(conn, job_id)
                self.assertEqual(str(job["state"]), "WAIT_APPROVAL")
            finally:
                conn.close()


    def test_mark_published_fails_when_draft_playlist_diverges_from_active_draft_history(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            job_id = self._create_ui_draft()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            self._preview_apply(client, headers, job_id)

            conn = dbm.connect(self.env)
            try:
                conn.execute("UPDATE ui_job_drafts SET audio_ids_text = ?, updated_at = ? WHERE job_id = ?", ("401,403", dbm.now_ts(), job_id))
                conn.commit()
            finally:
                conn.close()

            self._set_wait_approval(job_id)
            resp = client.post(f"/v1/jobs/{job_id}/mark_published", headers=headers, json={})
            self.assertEqual(resp.status_code, 409)
            self.assertEqual(resp.json()["error"]["code"], "PLB_COMMITTED_HISTORY_PLAYLIST_MISMATCH")

            conn = dbm.connect(self.env)
            try:
                committed_count = conn.execute(
                    "SELECT COUNT(*) AS c FROM playlist_history WHERE job_id = ? AND history_stage = 'COMMITTED'",
                    (job_id,),
                ).fetchone()
                self.assertEqual(int(committed_count["c"]), 0)
            finally:
                conn.close()

    def test_mark_published_recomputes_relative_metrics_at_publish_time(self) -> None:
        with temp_env() as (_, self.env):
            seed_minimal_db(self.env)
            self._seed_tracks()
            client = self._new_client()
            headers = basic_auth_header(self.env.basic_user, self.env.basic_pass)

            job_a = self._create_ui_draft()
            self._preview_apply(client, headers, job_a)

            conn = dbm.connect(self.env)
            try:
                draft_a = conn.execute(
                    "SELECT id, novelty_against_prev, batch_overlap_score FROM playlist_history WHERE job_id = ? AND history_stage = 'DRAFT' ORDER BY id DESC LIMIT 1",
                    (job_a,),
                ).fetchone()
                self.assertIsNone(draft_a["novelty_against_prev"])
                self.assertIsNone(draft_a["batch_overlap_score"])
            finally:
                conn.close()

            job_b = self._create_ui_draft()
            conn = dbm.connect(self.env)
            try:
                conn.execute("UPDATE ui_job_drafts SET audio_ids_text = ?, updated_at = ? WHERE job_id = ?", ("401,402", dbm.now_ts(), job_b))
                conn.commit()
            finally:
                conn.close()
            self._set_wait_approval(job_b)
            published_b = client.post(f"/v1/jobs/{job_b}/mark_published", headers=headers, json={})
            self.assertEqual(published_b.status_code, 409)

            conn = dbm.connect(self.env)
            try:
                conn.execute(
                    """
                    INSERT INTO playlist_history(channel_slug, job_id, history_stage, source_preview_id, generation_mode,
                    strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint,
                    prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        "darkwood-reverie",
                        job_b,
                        "COMMITTED",
                        None,
                        "safe",
                        "balanced",
                        500.0,
                        2,
                        "x-set",
                        "x-ord",
                        "x-p3",
                        "x-p5",
                        0.0,
                        0.0,
                        1,
                        "2030-01-01T00:00:00+00:00",
                    ),
                )
                committed_b = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                conn.execute(
                    "INSERT INTO playlist_history_items(history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?)",
                    (committed_b, 0, 401, "2024-01", 240.0, "darkwood-reverie"),
                )
                conn.execute(
                    "INSERT INTO playlist_history_items(history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?)",
                    (committed_b, 1, 402, "2024-01", 260.0, "darkwood-reverie"),
                )
                conn.commit()
            finally:
                conn.close()

            self._set_wait_approval(job_a)
            published_a = client.post(f"/v1/jobs/{job_a}/mark_published", headers=headers, json={})
            self.assertEqual(published_a.status_code, 200)

            conn = dbm.connect(self.env)
            try:
                committed_a = conn.execute(
                    "SELECT novelty_against_prev, batch_overlap_score FROM playlist_history WHERE job_id = ? AND history_stage = 'COMMITTED' ORDER BY id DESC LIMIT 1",
                    (job_a,),
                ).fetchone()
                self.assertIsNotNone(committed_a)
                self.assertAlmostEqual(float(committed_a["novelty_against_prev"]), 1.0 / 3.0, places=6)
                self.assertAlmostEqual(float(committed_a["batch_overlap_score"]), 2.0 / 3.0, places=6)
            finally:
                conn.close()



if __name__ == "__main__":
    unittest.main()
