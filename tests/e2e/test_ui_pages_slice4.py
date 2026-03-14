from __future__ import annotations

import importlib
import re
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, insert_release_and_job, seed_minimal_db, temp_env


class TestUiPagesSlice4(unittest.TestCase):
    def _seed_recovery_jobs(self, env: Env) -> dict[str, int]:
        failed_job = insert_release_and_job(env, state="FAILED", stage="RENDER", channel_slug="darkwood-reverie")
        stale_job = insert_release_and_job(env, state="RENDERING", stage="RENDER", channel_slug="channel-b")
        cleanup_pending_job = insert_release_and_job(env, state="PUBLISHED", stage="APPROVAL", channel_slug="channel-c")

        conn = dbm.connect(env)
        try:
            now_ts = dbm.now_ts()
            conn.execute(
                "UPDATE jobs SET error_reason = ?, progress_updated_at = ?, progress_text = ? WHERE id = ?",
                ("ffmpeg mux failed", now_ts - 180.0, "muxing output failed", failed_job),
            )
            conn.execute(
                "UPDATE jobs SET locked_by = ?, locked_at = ?, progress_updated_at = ?, progress_text = ? WHERE id = ?",
                (
                    "worker-stale-1",
                    now_ts - float(env.job_lock_ttl_sec) - 120.0,
                    now_ts - 1200.0,
                    "no render progress heartbeat",
                    stale_job,
                ),
            )
            conn.execute(
                "UPDATE jobs SET delete_mp4_at = ?, progress_updated_at = ?, progress_text = ? WHERE id = ?",
                (now_ts - 30.0, now_ts - 300.0, "published awaiting cleanup", cleanup_pending_job),
            )
            conn.commit()
        finally:
            conn.close()

        return {"failed": failed_job, "stale": stale_job, "cleanup_pending": cleanup_pending_job}

    def test_recovery_ui_seeded_data_proves_operator_triage_behaviors(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)
            seeded = self._seed_recovery_jobs(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            listing = client.get("/v1/ops/recovery/jobs?actionability=any", headers=h)
            self.assertEqual(listing.status_code, 200)
            payload = listing.json()
            items = payload["items"]
            by_id = {int(item["job_id"]): item for item in items}

            self.assertIn(seeded["failed"], by_id)
            self.assertIn("failed", by_id[seeded["failed"]]["categories"])
            self.assertIn(seeded["stale"], by_id)
            self.assertIn("stale", by_id[seeded["stale"]]["categories"])
            self.assertIn(seeded["cleanup_pending"], by_id)
            self.assertIn("cleanup_pending", by_id[seeded["cleanup_pending"]]["categories"])

            self.assertGreaterEqual(payload["summary"]["by_category"].get("failed", 0), 1)
            self.assertGreaterEqual(payload["summary"]["by_category"].get("stale", 0), 1)

            for job_id in (seeded["failed"], seeded["stale"]):
                actions = by_id[job_id]["available_actions"]
                self.assertTrue(any(bool(action.get("allowed")) for action in actions))
                for action in actions:
                    self.assertIn("allowed", action)
                    self.assertIn("risk_level", action)
                    self.assertIn("reason", action)

            failed_only = client.get("/v1/ops/recovery/jobs?category=failed&actionability=any", headers=h)
            self.assertEqual(failed_only.status_code, 200)
            failed_ids = {int(item["job_id"]) for item in failed_only.json()["items"]}
            self.assertIn(seeded["failed"], failed_ids)
            self.assertNotIn(seeded["stale"], failed_ids)

            stale_only = client.get("/v1/ops/recovery/jobs?category=stale&actionability=any", headers=h)
            self.assertEqual(stale_only.status_code, 200)
            stale_ids = {int(item["job_id"]) for item in stale_only.json()["items"]}
            self.assertIn(seeded["stale"], stale_ids)
            self.assertNotIn(seeded["failed"], stale_ids)

            state_only = client.get("/v1/ops/recovery/jobs?state=FAILED&actionability=any", headers=h)
            self.assertEqual(state_only.status_code, 200)
            state_ids = {int(item["job_id"]) for item in state_only.json()["items"]}
            self.assertEqual(state_ids, {seeded["failed"]})

            channel_only = client.get("/v1/ops/recovery/jobs?channel_slug=channel-b&actionability=any", headers=h)
            self.assertEqual(channel_only.status_code, 200)
            channel_ids = {int(item["job_id"]) for item in channel_only.json()["items"]}
            self.assertEqual(channel_ids, {seeded["stale"]})

            risky_present = client.get("/v1/ops/recovery/jobs?actionability=risky_present", headers=h)
            self.assertEqual(risky_present.status_code, 200)
            risky_ids = {int(item["job_id"]) for item in risky_present.json()["items"]}
            self.assertIn(seeded["failed"], risky_ids)
            self.assertIn(seeded["stale"], risky_ids)
            self.assertNotIn(seeded["cleanup_pending"], risky_ids)

            q_only = client.get("/v1/ops/recovery/jobs?q=muxing&actionability=any", headers=h)
            self.assertEqual(q_only.status_code, 200)
            q_ids = {int(item["job_id"]) for item in q_only.json()["items"]}
            self.assertEqual(q_ids, {seeded["failed"]})

            details = client.get(f"/v1/ops/recovery/jobs/{seeded['failed']}", headers=h)
            self.assertEqual(details.status_code, 200)
            detail_item = details.json()["item"]
            self.assertEqual(int(detail_item["job_id"]), seeded["failed"])
            self.assertIn("available_actions", detail_item)
            self.assertIn("recent_audit_entries", detail_item)
            self.assertIn("failure_details", detail_item)

            page = client.get("/ui/recovery", headers=h)
            self.assertEqual(page.status_code, 200)
            self.assertIn("detailsModal.showModal()", page.text)
            self.assertIn("Applied filters:", page.text)
            self.assertIn("formatAppliedFilters(params)", page.text)
            self.assertIn("registerAutoApplyInput(channelInput)", page.text)
            self.assertIn("registerAutoApplyInput(stateInput)", page.text)
            self.assertIn("registerAutoApplyInput(qInput)", page.text)
            self.assertIn("categoryInput.addEventListener('change', loadJobs)", page.text)
            self.assertIn("actionabilityInput.addEventListener('change', loadJobs)", page.text)
            self.assertIn("Available actions (read-only preview)", page.text)
            self.assertIn("Recent recovery audit entries", page.text)
            self.assertIn('<button type="button" disabled title="Read-only slice">', page.text)

    def test_playlist_builder_preview_state_guards(self) -> None:
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

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("const previewStateInputIds = [", r.text)
            self.assertIn("'plb-generation-mode'", r.text)
            self.assertIn("'plb-strictness-mode'", r.text)
            self.assertIn("'plb-min-duration'", r.text)
            self.assertIn("'plb-max-duration'", r.text)
            self.assertIn("'plb-tolerance'", r.text)
            self.assertIn("'plb-allow-cross-channel'", r.text)
            self.assertIn("'plb-preferred-month-batch'", r.text)
            self.assertIn("'plb-preferred-batch-ratio'", r.text)
            self.assertIn("'plb-novelty-min'", r.text)
            self.assertIn("'plb-novelty-max'", r.text)
            self.assertIn("'plb-vocal-policy'", r.text)
            self.assertIn("'plb-required-tags'", r.text)
            self.assertIn("'plb-excluded-tags'", r.text)
            self.assertIn("'plb-notes'", r.text)
            self.assertIn("function invalidatePreviewState()", r.text)
            self.assertIn("applyBtn.disabled = false;", r.text)
            self.assertIn("applyBtn.disabled = true;", r.text)
            self.assertIn("status.textContent = 'Preview outdated; run Preview again';", r.text)
            self.assertIn("lastPreviewOverrideSnapshot = override;", r.text)
            self.assertIn("body: JSON.stringify(lastPreviewOverrideSnapshot)", r.text)
            self.assertNotIn("body: JSON.stringify(buildOverride())", r.text)

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
            self.assertIn('id="playlist-builder-open-btn"', r.text)
            self.assertIn('id="playlist-builder-modal"', r.text)
            self.assertIn('id="plb-preview-btn"', r.text)
            self.assertIn('id="plb-apply-btn"', r.text)
            self.assertIn('id="playlist-builder-save-first"', r.text)
            self.assertIn('Draft will be auto-created on first Preview.', r.text)
            self.assertIn('name="background_name"', r.text)
            self.assertIn('name="background_ext"', r.text)

            r = client.get("/", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn('action="/ui/jobs/render_all"', r.text)
            self.assertIn('method="post"', r.text)
            self.assertIn('id="jobs-status-filter-dropdown"', r.text)
            self.assertIn("function setJobsStatusFilterUnavailable(isUnavailable)", r.text)
            self.assertIn("jobsStatusFilterDropdown.getAttribute('data-unavailable') === 'true'", r.text)
            self.assertIn("setJobsStatusFilterUnavailable(true);", r.text)
            self.assertIn('id="channel-add-btn"', r.text)
            self.assertIn('id="channels-table"', r.text)
            self.assertIn('href="/ui/db-viewer"', r.text)
            self.assertIn('href="/ui/planner"', r.text)
            self.assertIn('href="/ui/track-catalog/custom-tags"', r.text)
            self.assertIn('href="/ui/track-catalog/analysis-report"', r.text)
            self.assertIn('id="nav-recovery-link"', r.text)
            nav_tag_match = re.search(r'<a[^>]*id="nav-recovery-link"[^>]*>', r.text)
            self.assertIsNotNone(nav_tag_match)
            assert nav_tag_match is not None
            href_match = re.search(r'href="([^"]+)"', nav_tag_match.group(0))
            self.assertIsNotNone(href_match)
            assert href_match is not None
            recovery_href = href_match.group(1)
            self.assertRegex(recovery_href, r'/ui/recovery/?$')

            tested_recovery_path = re.sub(r'^https?://[^/]+', '', recovery_href)
            r = client.get(tested_recovery_path, headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Ops Recovery Console", r.text)
            self.assertIn('id="recovery-page"', r.text)
            self.assertIn('id="recovery-summary"', r.text)
            self.assertIn('id="filter-category"', r.text)
            self.assertIn('id="filter-channel"', r.text)
            self.assertIn('id="filter-state"', r.text)
            self.assertIn('id="filter-actionability"', r.text)
            self.assertIn('id="filter-q"', r.text)
            self.assertIn('id="recovery-table"', r.text)
            self.assertIn('id="recovery-details-modal"', r.text)
            self.assertIn('loadJobs();', r.text)
            self.assertIn('renderActions(item.available_actions)', r.text)
            self.assertNotIn("Not Found", r.text)
            self.assertNotIn('{"detail":"Not Found"}', r.text)
            self.assertIn("jobsUrl", r.text)
            self.assertIn("jobDetailsUrlTemplate", r.text)

            r = client.get(tested_recovery_path + "/", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Ops Recovery Console", r.text)

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


            r = client.get("/ui/track-catalog/custom-tags", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn('id="tags-table"', r.text)

            r = client.get("/ui/tags", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("TAGS", r.text)

            self.assertIn("TAGS", r.text)
            self.assertIn('id="tags-table"', r.text)
            self.assertIn('id="tags-import-btn"', r.text)
            self.assertIn('id="tags-export-btn"', r.text)
            self.assertIn('id="tags-dashboard-open-btn"', r.text)
            self.assertIn('id="tags-taxonomy-preview-btn"', r.text)
            self.assertIn('id="tags-taxonomy-confirm-btn"', r.text)
            self.assertIn('id="tags-bulk-tags-enable-btn"', r.text)
            self.assertIn('id="tags-bulk-rules-disable-btn"', r.text)
            self.assertIn('<th>Summary</th>', r.text)
            self.assertIn('id="tag-editor-modal"', r.text)
            self.assertIn('id="tag-json-mode"', r.text)
            self.assertIn('if (editorJsonMode.checked)', r.text)
            self.assertIn("credentials: 'same-origin'", r.text)
            self.assertIn('function resolveApiPath(path)', r.text)
            self.assertIn('Catalog is empty.', r.text)
            self.assertNotIn("fetch('/v1/track-catalog/custom-tags/catalog')", r.text)
            self.assertNotIn('payload.code || editorCode.value', r.text)
            self.assertNotIn('payload.label || editorLabel.value', r.text)
            self.assertIn("window.location.href = '/ui/track-catalog/custom-tags/dashboard/'", r.text)

            r = client.get("/ui/track-catalog/custom-tags/dashboard/darkwood-reverie", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Custom Tags · Channel Dashboard", r.text)
            self.assertIn('<summary><strong>Quick help</strong></summary>', r.text)
            self.assertIn('id="tags-dash-visual-table"', r.text)
            self.assertIn('id="tags-dash-rules-table"', r.text)
            self.assertIn('id="tags-dash-usage-table"', r.text)

            r = client.get("/ui/track-catalog/custom-tags/dashboard", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Custom Tags · Channel Dashboard", r.text)
            self.assertIn('id="tags-dash-channel"', r.text)

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Edit Job", r.text)
            self.assertIn('id="playlist-builder-open-btn"', r.text)
            self.assertIn('/v1/playlist-builder/jobs/${jobId}/preview', r.text)
            self.assertIn("async function ensureJobIdForPlaylistBuilder()", r.text)
            self.assertIn("window.history.replaceState(null, '', `/ui/jobs/${activeJobId}/edit`);", r.text)
            self.assertIn('/v1/playlist-builder/jobs/${jobId}/apply', r.text)
            self.assertIn('existing playlist will be replaced only after Apply', r.text)
            self.assertIn('<th>month_batch</th>', r.text)
            self.assertIn('<th>fit/explanation</th>', r.text)

            r = client.get(f"/jobs/{job_id}", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn('<summary><strong>Quick help</strong></summary>', r.text)

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

    def test_edit_page_reflects_applied_playlist_in_audio_ids_field(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)

            conn = dbm.connect(env)
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
                    audio_ids_text="123",
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            preview = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/preview",
                headers=h,
                json={"override": {"generation_mode": "safe", "min_duration_min": 10, "max_duration_min": 15}},
            )
            self.assertEqual(preview.status_code, 200)
            preview_id = preview.json()["preview_id"]

            apply_resp = client.post(
                f"/v1/playlist-builder/jobs/{job_id}/apply",
                headers=h,
                json={"preview_id": preview_id},
            )
            self.assertEqual(apply_resp.status_code, 200)

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)

            conn2 = dbm.connect(env)
            try:
                draft = dbm.get_ui_job_draft(conn2, job_id)
                expected_audio_ids = str(draft["audio_ids_text"])
            finally:
                conn2.close()
            self.assertIn(f'value="{expected_audio_ids}"', r.text)


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
