from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPagesSlice4(unittest.TestCase):
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
            self.assertIn('data-channel-slug="darkwood-reverie"', r.text)
            self.assertIn('function resolveSelectedChannelSlug()', r.text)
            self.assertIn('channelSlug = resolveSelectedChannelSlug();', r.text)
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
            self.assertIn('href="/ui/ops/recovery"', r.text)

            r = client.get("/ui/ops/recovery", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Ops · Recovery Console", r.text)
            self.assertIn('id="filter-channel"', r.text)
            self.assertIn('/v1/ops/recovery/jobs', r.text)
            self.assertIn('/v1/ops/recovery/audit?limit=30', r.text)

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

            create_slash = client.get("/ui/jobs/create/", headers=h, follow_redirects=False)
            self.assertEqual(create_slash.status_code, 307)
            self.assertEqual(create_slash.headers.get("location"), "/ui/jobs/create")

            edit_slash = client.get(f"/ui/jobs/{job_id}/edit/", headers=h, follow_redirects=False)
            self.assertEqual(edit_slash.status_code, 307)
            self.assertEqual(edit_slash.headers.get("location"), f"/ui/jobs/{job_id}/edit")

            r = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Edit Job", r.text)
            self.assertIn('id="playlist-builder-open-btn"', r.text)
            self.assertIn('/v1/playlist-builder/jobs/${jobId}/preview', r.text)
            self.assertIn("async function ensureJobIdForPlaylistBuilder()", r.text)
            self.assertIn("fetch('/v1/ui/jobs/playlist-builder-draft'", r.text)
            self.assertIn("window.history.replaceState(null, '', `/ui/jobs/${activeJobId}/edit`);", r.text)
            self.assertIn('/v1/playlist-builder/jobs/${jobId}/apply', r.text)
            self.assertIn('/v1/playlist-builder/tags/options', r.text)
            self.assertIn('const briefResp = await fetch(`/v1/playlist-builder/jobs/${activeJobId}/brief`', r.text)
            self.assertIn('channelSlug = resolveSelectedChannelSlug();', r.text)
            self.assertIn('id="plb-required-tags-picker-btn"', r.text)
            self.assertIn('id="plb-excluded-tags-picker-btn"', r.text)
            self.assertIn('id="playlist-builder-tags-picker-modal"', r.text)
            self.assertIn('id="plb-tags-picker-search"', r.text)
            self.assertIn('mergeCanonicalTagsIntoInput', r.text)
            self.assertIn('if (lowerExisting.has(lower)) return;', r.text)
            self.assertIn("Draft creation failed: ${firstFieldError[0]}", r.text)
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
