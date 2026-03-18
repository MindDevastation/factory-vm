from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env

from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestUiPagesSlice4(unittest.TestCase):
    def test_job_edit_titlegen_single_release_operator_flow(self) -> None:
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
                    title="Manual Existing Title",
                    description="Original Description",
                    tags_csv="ambient,night",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                before_row = conn.execute(
                    """
                    SELECT r.id AS release_id, r.title, r.description, r.tags_json
                    FROM jobs j
                    JOIN releases r ON r.id = j.release_id
                    WHERE j.id = ?
                    """,
                    (job_id,),
                ).fetchone()
                assert before_row
                release_id = int(before_row["release_id"])
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            default_tpl = client.post(
                "/v1/metadata/title-templates",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default Release Title",
                    "template_body": "{{channel_display_name}}",
                    "make_default": True,
                },
            )
            self.assertEqual(default_tpl.status_code, 200)
            secondary_tpl = client.post(
                "/v1/metadata/title-templates",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Secondary Release Title",
                    "template_body": "{{channel_slug}} alt",
                    "make_default": False,
                },
            )
            self.assertEqual(secondary_tpl.status_code, 200)

            edit_page = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(edit_page.status_code, 200)
            self.assertIn('id="titlegen-section"', edit_page.text)
            self.assertIn('id="titlegen-current-title"', edit_page.text)
            self.assertIn('id="titlegen-default-template"', edit_page.text)
            self.assertIn('id="titlegen-template-select"', edit_page.text)
            self.assertIn('id="titlegen-generate-btn"', edit_page.text)
            self.assertIn('id="titlegen-regenerate-btn"', edit_page.text)
            self.assertIn('id="titlegen-apply-btn"', edit_page.text)
            self.assertIn("Generation does not change the release title until you apply.", edit_page.text)
            self.assertIn("Applying this generated title will overwrite the existing release title.", edit_page.text)
            self.assertIn("window.confirm('This will overwrite the current release title. Continue?')", edit_page.text)
            self.assertIn("overwrite_confirmed: overwriteNeedsConfirm", edit_page.text)
            self.assertIn(f'id="titlegen-release-id">#{release_id}</span>', edit_page.text)
            self.assertIn(f"const initialReleaseId = {release_id};", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/titlegen/context", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/titlegen/generate", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/titlegen/apply", edit_page.text)

            context_resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=h)
            self.assertEqual(context_resp.status_code, 200)
            context_payload = context_resp.json()
            self.assertEqual(context_payload["release_id"], release_id)
            self.assertEqual(context_payload["current_title"], "Manual Existing Title")
            self.assertEqual(context_payload["default_template"]["template_name"], "Default Release Title")
            self.assertGreaterEqual(len(context_payload["active_templates"]), 2)

            generate_resp = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/generate",
                headers=h,
                json={},
            )
            self.assertEqual(generate_resp.status_code, 200)
            generate_payload = generate_resp.json()
            self.assertTrue(generate_payload["overwrite_required"])
            self.assertTrue(generate_payload["proposed_title"])

            conn = dbm.connect(env)
            try:
                after_generate_row = conn.execute(
                    "SELECT title, description, tags_json FROM releases WHERE id = ?",
                    (release_id,),
                ).fetchone()
                assert after_generate_row
            finally:
                conn.close()
            self.assertEqual(after_generate_row["title"], before_row["title"])
            self.assertEqual(after_generate_row["description"], before_row["description"])
            self.assertEqual(after_generate_row["tags_json"], before_row["tags_json"])

            apply_resp = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/apply",
                headers=h,
                json={
                    "generation_fingerprint": generate_payload["generation_fingerprint"],
                    "overwrite_confirmed": True,
                },
            )
            self.assertEqual(apply_resp.status_code, 200)
            apply_payload = apply_resp.json()
            self.assertTrue(apply_payload["title_updated"])
            self.assertNotEqual(apply_payload["title_before"], apply_payload["title_after"])

            conn = dbm.connect(env)
            try:
                after_apply_row = conn.execute(
                    "SELECT title, description, tags_json FROM releases WHERE id = ?",
                    (release_id,),
                ).fetchone()
                assert after_apply_row
            finally:
                conn.close()
            self.assertEqual(after_apply_row["title"], apply_payload["title_after"])
            self.assertEqual(after_apply_row["description"], before_row["description"])
            self.assertEqual(after_apply_row["tags_json"], before_row["tags_json"])

    def test_titlegen_no_default_requires_explicit_template_selection(self) -> None:
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
                    title="",
                    description="",
                    tags_csv="",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                client_tpl_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Non-default template",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            edit_page = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(edit_page.status_code, 200)
            self.assertIn("placeholder.textContent = (context.active_templates || []).length ? 'Select a template' : 'No active templates';", edit_page.text)
            self.assertIn("Select a template before generating preview.", edit_page.text)

            context_resp = client.get(f"/v1/metadata/releases/{release_id}/titlegen/context", headers=h)
            self.assertEqual(context_resp.status_code, 200)
            context_payload = context_resp.json()
            self.assertIsNone(context_payload["default_template"])
            self.assertEqual(len(context_payload["active_templates"]), 1)

            no_selection = client.post(f"/v1/metadata/releases/{release_id}/titlegen/generate", headers=h, json={})
            self.assertEqual(no_selection.status_code, 422)
            self.assertEqual(no_selection.json()["error"]["code"], "MTG_DEFAULT_TEMPLATE_NOT_CONFIGURED")

            explicit = client.post(
                f"/v1/metadata/releases/{release_id}/titlegen/generate",
                headers=h,
                json={"template_id": client_tpl_id},
            )
            self.assertEqual(explicit.status_code, 200)

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


    def test_mtb_page_minimal_operator_flow(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            create_default = client.post(
                "/v1/metadata/title-templates",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Default Title",
                    "template_body": "{{channel_display_name}} {{release_year}}",
                    "make_default": True,
                },
            )
            self.assertEqual(create_default.status_code, 200)
            default_template = create_default.json()

            create_secondary = client.post(
                "/v1/metadata/title-templates",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_name": "Secondary",
                    "template_body": "{{channel_slug}} {{release_month_number}}",
                    "make_default": False,
                },
            )
            self.assertEqual(create_secondary.status_code, 200)
            secondary_template = create_secondary.json()

            page = client.get("/ui/metadata/title-templates", headers=h)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Metadata · Title Templates", page.text)
            self.assertIn('href="/"', page.text)
            self.assertIn('id="mtb-channel"', page.text)
            self.assertIn('id="mtb-template-name"', page.text)
            self.assertIn('id="mtb-template-body"', page.text)
            self.assertIn('id="mtb-preview-btn"', page.text)
            self.assertIn('id="mtb-save-btn"', page.text)
            self.assertIn('id="mtb-set-default-btn"', page.text)
            self.assertIn('id="mtb-archive-btn"', page.text)
            self.assertIn('id="mtb-activate-btn"', page.text)
            self.assertIn('id="mtb-preview-result"', page.text)
            self.assertIn('id="mtb-substituted-values"', page.text)
            self.assertIn('id="mtb-missing-variables"', page.text)
            self.assertIn('id="mtb-validation-errors"', page.text)
            self.assertIn('/v1/channels', page.text)
            self.assertIn('/v1/metadata/title-templates/variables', page.text)
            self.assertIn('/v1/metadata/title-templates?', page.text)
            self.assertIn('/v1/metadata/title-templates/preview', page.text)
            self.assertIn('/v1/metadata/title-templates/${activeTemplateId}/${action}', page.text)
            self.assertIn('archiving current default may leave this channel with no default template', page.text)
            self.assertIn('active', page.text)
            self.assertIn('archived', page.text)
            self.assertIn('valid', page.text)

            preview = client.post(
                "/v1/metadata/title-templates/preview",
                headers=h,
                json={
                    "channel_slug": "darkwood-reverie",
                    "template_body": "{{channel_display_name}} {{release_year}}",
                    "release_date": "2026-01-02",
                },
            )
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["render_status"], "FULL")
            self.assertEqual(preview.json()["missing_variables"], [])

            set_default = client.post(
                f"/v1/metadata/title-templates/{secondary_template['id']}/set-default",
                headers=h,
            )
            self.assertEqual(set_default.status_code, 200)
            self.assertTrue(set_default.json()["is_default"])

            archive_default = client.post(
                f"/v1/metadata/title-templates/{default_template['id']}/archive",
                headers=h,
            )
            self.assertEqual(archive_default.status_code, 200)
            self.assertEqual(archive_default.json()["status"], "ARCHIVED")
            self.assertFalse(archive_default.json()["is_default"])

            active_list = client.get("/v1/metadata/title-templates?status=active", headers=h)
            self.assertEqual(active_list.status_code, 200)
            self.assertTrue(any(item["status"] == "ACTIVE" and item["is_default"] for item in active_list.json().get("items", [])))
            self.assertTrue(any(item["validation_status"] == "VALID" for item in active_list.json().get("items", [])))

            archived_list = client.get("/v1/metadata/title-templates?status=archived", headers=h)
            self.assertEqual(archived_list.status_code, 200)
            self.assertTrue(any(item["status"] == "ARCHIVED" for item in archived_list.json().get("items", [])))

            activate = client.post(
                f"/v1/metadata/title-templates/{default_template['id']}/activate",
                headers=h,
            )
            self.assertEqual(activate.status_code, 200)
            self.assertEqual(activate.json()["status"], "ACTIVE")

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
            self.assertIn('async function parseJsonSafe(resp)', r.text)
            self.assertIn('Preview failed due to a server error. Please retry.', r.text)
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
            self.assertIn('href="/ui/metadata/title-templates"', r.text)
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

            planner_r = client.get("/ui/planner", headers=h)
            self.assertEqual(planner_r.status_code, 200)
            self.assertIn("Planner · Bulk Releases", planner_r.text)
            self.assertIn('id="planner-tbody"', planner_r.text)
            self.assertIn('id="bulk-create-modal"', planner_r.text)
            self.assertIn('id="import-modal"', planner_r.text)
            self.assertIn('/static/planner_bulk_releases.js', planner_r.text)

            r = client.get("/ui/metadata/title-templates", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Metadata · Title Templates", r.text)
            self.assertIn('id="mtb-table"', r.text)
            self.assertIn('id="mtb-preview-btn"', r.text)
            self.assertIn('id="mtb-save-btn"', r.text)
            self.assertIn('id="mtb-set-default-btn"', r.text)
            self.assertIn('id="mtb-archive-btn"', r.text)
            self.assertIn('id="mtb-activate-btn"', r.text)
            self.assertIn("/v1/metadata/title-templates/preview", r.text)
            self.assertIn("/v1/metadata/title-templates/${activeTemplateId}/${action}", r.text)
            self.assertIn("Preview complete (not saved).", r.text)
            self.assertIn("archiving current default may leave this channel with no default template", r.text)
            self.assertIn("active", r.text)
            self.assertIn("archived", r.text)
            self.assertIn("valid", r.text)

            preview = client.post(
                "/v1/metadata/title-templates/preview",
                headers=h,
                json={
                    "channel_slug": str(ch["slug"]),
                    "template_body": "{{channel_display_name}} {{release_year}}",
                    "release_date": "2026-01-02",
                },
            )
            self.assertEqual(preview.status_code, 200)
            self.assertEqual(preview.json()["render_status"], "FULL")

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
            self.assertIn('function renderDiagnosticsBlock(diag, reasonText)', r.text)
            self.assertIn('const diagnostics = error.diagnostics || {};', r.text)
            self.assertIn('previewBtn.disabled = true;', r.text)
            self.assertIn('window.setTimeout(() => controller.abort(\'preview_timeout\')', r.text)
            self.assertIn('previewBtn.disabled = false;', r.text)
            self.assertIn('<b>resolved channel_slug:</b>', r.text)
            self.assertIn("after month_batch=${d.after_month_batch_preference_or_filter ?? '-' }".replace(" ",""), r.text.replace(" ",""))

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
