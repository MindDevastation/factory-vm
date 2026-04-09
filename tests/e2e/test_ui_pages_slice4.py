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

    def test_job_edit_descriptiongen_single_release_operator_flow(self) -> None:
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
                    title="Night Ritual",
                    description="Manual Existing Description",
                    tags_csv="ambient,night",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                before_row = conn.execute(
                    "SELECT r.id AS release_id, r.title, r.description, r.tags_json FROM jobs j JOIN releases r ON r.id = j.release_id WHERE j.id = ?",
                    (job_id,),
                ).fetchone()
                assert before_row
                release_id = int(before_row["release_id"])
                dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Default Description",
                    template_body="{{channel_display_name}}\n\n{{release_title}}",
                    status="ACTIVE",
                    is_default=True,
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
            self.assertIn('id="descriptiongen-section"', edit_page.text)
            self.assertIn("Generation does not change the release description until you apply.", edit_page.text)
            self.assertIn("Applying this generated description will overwrite the existing release description.", edit_page.text)
            self.assertIn("descriptiongen-overwrite-checkbox", edit_page.text)
            self.assertIn("overwrite_confirmed: overwriteNeedsConfirm", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/descriptiongen/context", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/descriptiongen/generate", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/descriptiongen/apply", edit_page.text)

            generate_resp = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=h, json={})
            self.assertEqual(generate_resp.status_code, 200)
            generate_payload = generate_resp.json()

            conn = dbm.connect(env)
            try:
                after_generate_row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
                assert after_generate_row
            finally:
                conn.close()
            self.assertEqual(after_generate_row["description"], before_row["description"])
            self.assertEqual(after_generate_row["title"], before_row["title"])
            self.assertEqual(after_generate_row["tags_json"], before_row["tags_json"])

            denied_apply = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=h,
                json={
                    "generation_fingerprint": generate_payload["generation_fingerprint"],
                    "overwrite_confirmed": False,
                },
            )
            self.assertEqual(denied_apply.status_code, 422)
            self.assertEqual(denied_apply.json()["error"]["code"], "MTD_OVERWRITE_CONFIRMATION_REQUIRED")

            apply_resp = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/apply",
                headers=h,
                json={
                    "generation_fingerprint": generate_payload["generation_fingerprint"],
                    "overwrite_confirmed": True,
                },
            )
            self.assertEqual(apply_resp.status_code, 200)
            apply_payload = apply_resp.json()
            self.assertTrue(apply_payload["description_updated"])

            conn = dbm.connect(env)
            try:
                after_apply_row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
                assert after_apply_row
            finally:
                conn.close()
            self.assertEqual(after_apply_row["description"], apply_payload["description_after"])
            self.assertEqual(after_apply_row["title"], before_row["title"])
            self.assertEqual(after_apply_row["tags_json"], before_row["tags_json"])

    def test_descriptiongen_no_default_requires_manual_selection(self) -> None:
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
                    title="Night Ritual",
                    description="",
                    tags_csv="",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                explicit_tpl = dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Non-default description template",
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
            self.assertIn("Select a template before generating preview.", edit_page.text)
            self.assertIn("descriptiongenTemplateSelect.value = '';", edit_page.text)

            no_selection = client.post(f"/v1/metadata/releases/{release_id}/descriptiongen/generate", headers=h, json={})
            self.assertEqual(no_selection.status_code, 422)
            self.assertEqual(no_selection.json()["error"]["code"], "MTD_DEFAULT_TEMPLATE_NOT_CONFIGURED")

            explicit = client.post(
                f"/v1/metadata/releases/{release_id}/descriptiongen/generate",
                headers=h,
                json={"template_id": explicit_tpl},
            )
            self.assertEqual(explicit.status_code, 200)

    def test_job_edit_videotagsgen_single_release_operator_flow(self) -> None:
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
                    title="Night Ritual",
                    description="Original Description",
                    tags_csv="ambient,night",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                before_row = conn.execute(
                    "SELECT r.id AS release_id, r.title, r.description, r.tags_json FROM jobs j JOIN releases r ON r.id = j.release_id WHERE j.id = ?",
                    (job_id,),
                ).fetchone()
                assert before_row
                release_id = int(before_row["release_id"])
                dbm.create_video_tag_preset(
                    conn,
                    channel_slug="darkwood-reverie",
                    preset_name="Default Video Tags",
                    preset_body_json=dbm.json_dumps(["{{channel_display_name}}", "{{release_title}}", "ambient", "  ", "ambient"]),
                    status="ACTIVE",
                    is_default=True,
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
            self.assertIn('id="videotagsgen-section"', edit_page.text)
            self.assertIn('id="videotagsgen-current-tags-json"', edit_page.text)
            self.assertIn('id="videotagsgen-default-preset"', edit_page.text)
            self.assertIn('id="videotagsgen-preset-select"', edit_page.text)
            self.assertIn('id="videotagsgen-generate-btn"', edit_page.text)
            self.assertIn('id="videotagsgen-regenerate-btn"', edit_page.text)
            self.assertIn('id="videotagsgen-apply-btn"', edit_page.text)
            self.assertIn("Generation does not change the release tags until you apply.", edit_page.text)
            self.assertIn("Applying this generated tag list will overwrite the existing release tags.", edit_page.text)
            self.assertIn("overwrite_confirmed: overwriteNeedsConfirm", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/video-tags/context", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/video-tags/generate", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/video-tags/apply", edit_page.text)

            generated = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=h, json={})
            self.assertEqual(generated.status_code, 200)
            generated_payload = generated.json()
            self.assertIn("  ", generated_payload["dropped_empty_items"])
            self.assertIn("ambient", generated_payload["removed_duplicates"])

            conn = dbm.connect(env)
            try:
                after_generate_row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
                assert after_generate_row
            finally:
                conn.close()
            self.assertEqual(after_generate_row["title"], before_row["title"])
            self.assertEqual(after_generate_row["description"], before_row["description"])
            self.assertEqual(after_generate_row["tags_json"], before_row["tags_json"])

            denied = client.post(
                f"/v1/metadata/releases/{release_id}/video-tags/apply",
                headers=h,
                json={
                    "generation_fingerprint": generated_payload["generation_fingerprint"],
                    "overwrite_confirmed": False,
                },
            )
            self.assertEqual(denied.status_code, 422)
            self.assertEqual(denied.json()["error"]["code"], "MTV_OVERWRITE_CONFIRMATION_REQUIRED")

            applied = client.post(
                f"/v1/metadata/releases/{release_id}/video-tags/apply",
                headers=h,
                json={
                    "generation_fingerprint": generated_payload["generation_fingerprint"],
                    "overwrite_confirmed": True,
                },
            )
            self.assertEqual(applied.status_code, 200)
            self.assertTrue(applied.json()["tags_updated"])

            conn = dbm.connect(env)
            try:
                after_apply_row = conn.execute("SELECT title, description, tags_json FROM releases WHERE id = ?", (release_id,)).fetchone()
                assert after_apply_row
            finally:
                conn.close()
            self.assertEqual(after_apply_row["title"], before_row["title"])
            self.assertEqual(after_apply_row["description"], before_row["description"])
            self.assertEqual(after_apply_row["tags_json"], dbm.json_dumps(applied.json()["tags_after"]))

    def test_videotagsgen_no_default_requires_manual_selection(self) -> None:
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
                    title="Night Ritual",
                    description="",
                    tags_csv="",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                explicit_preset = dbm.create_video_tag_preset(
                    conn,
                    channel_slug="darkwood-reverie",
                    preset_name="Non-default video tags preset",
                    preset_body_json=dbm.json_dumps(["{{channel_display_name}}"]),
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
            self.assertIn("Select a preset before generating preview.", edit_page.text)
            self.assertIn("videotagsgenPresetSelect.value = '';", edit_page.text)

            no_selection = client.post(f"/v1/metadata/releases/{release_id}/video-tags/generate", headers=h, json={})
            self.assertEqual(no_selection.status_code, 422)
            self.assertEqual(no_selection.json()["error"]["code"], "MTV_DEFAULT_PRESET_NOT_CONFIGURED")

            explicit = client.post(
                f"/v1/metadata/releases/{release_id}/video-tags/generate",
                headers=h,
                json={"preset_id": explicit_preset},
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
            self.assertIn("Title Templates", page.text)
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
            self.assertIn('id="descriptiongen-section"', r.text)
            self.assertIn('id="videotagsgen-section"', r.text)
            self.assertIn('id="titlegen-collapsible"', r.text)
            self.assertIn('id="descriptiongen-collapsible"', r.text)
            self.assertIn('id="videotagsgen-collapsible"', r.text)
            self.assertNotIn('id="titlegen-collapsible" open', r.text)
            self.assertNotIn('id="descriptiongen-collapsible" open', r.text)
            self.assertNotIn('id="videotagsgen-collapsible" open', r.text)
            self.assertIn('async function parseJsonSafe(resp)', r.text)
            self.assertIn('Preview failed due to a server error. Please retry.', r.text)
            self.assertIn('id="playlist-builder-save-first"', r.text)
            self.assertIn('Playlist Preview must not create a job in Create flow.', r.text)
            self.assertIn("Save draft first. Preview must not create a job.", r.text)
            self.assertIn("Creating draft for metadata generators...", r.text)
            self.assertIn('data-channel-slug="darkwood-reverie"', r.text)
            self.assertIn('function resolveSelectedChannelSlug()', r.text)
            self.assertIn('channelSlug = resolveSelectedChannelSlug();', r.text)
            self.assertIn('name="background_name"', r.text)
            self.assertIn('name="background_ext"', r.text)
            self.assertIn('<select name="cover_ext"', r.text)
            self.assertIn('<select name="background_ext"', r.text)
            self.assertIn('<option value="png"', r.text)
            self.assertIn('<option value="jpg"', r.text)
            self.assertIn('<option value="jpeg"', r.text)
            self.assertIn('name="description" rows="6" style="width:86ch;"', r.text)
            self.assertIn('name="audio_ids_text" rows="6" style="width:86ch;"', r.text)
            self.assertIn('name="audience_is_for_kids" value="yes"', r.text)
            self.assertIn('name="audience_is_for_kids" value="no" checked', r.text)
            self.assertIn('name="video_language" value="English"', r.text)
            self.assertIn('id="playlist-checkboxes"', r.text)
            self.assertIn('/v1/channels/${channelId}/playlists', r.text)

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
            self.assertIn('id="header-channels-btn"', r.text)
            self.assertIn('id="channels-modal"', r.text)
            self.assertIn('id="channels-close-btn"', r.text)
            self.assertIn('id="oauth-refresh-btn"', r.text)
            self.assertIn('id="oauth-status-table"', r.text)
            self.assertIn('<h3>OAuth Tokens</h3>', r.text)
            self.assertNotIn('<h2>OAuth Tokens</h2>', r.text)
            self.assertIn('id="jobs-bulk-open-btn"', r.text)
            self.assertIn('id="jobs-bulk-modal"', r.text)
            self.assertIn('id="secondary-surfaces-drawer"', r.text)
            self.assertIn('class="secondary-drawer"', r.text)
            self.assertIn('id="secondary-surfaces-backdrop"', r.text)
            drawer_start = r.text.index('id="secondary-surfaces-drawer"')
            overview_idx = r.text.index("Control Center Overview")
            drawer_close_idx = r.text.index('id="secondary-surfaces-close-btn"')
            self.assertGreater(overview_idx, drawer_start)
            self.assertLess(overview_idx, drawer_close_idx)
            self.assertIn('secondarySurfacesDrawer.classList.add(\'open\')', r.text)
            self.assertNotIn('secondarySurfacesDrawer.showModal()', r.text)
            self.assertIn('details open style="margin:12px 0;"', r.text)
            self.assertIn('href="/ui/db-viewer"', r.text)
            self.assertIn('href="/ui/planner"', r.text)
            self.assertIn('href="/ui/metadata/title-templates"', r.text)
            self.assertIn('href="/ui/track-catalog/custom-tags"', r.text)
            self.assertIn('href="/ui/track-catalog/analysis-report"', r.text)
            self.assertIn('href="/ui/analyzer"', r.text)
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
            self.assertIn('id="metadata-bulk-open"', planner_r.text)
            self.assertIn('id="mass-actions-open"', planner_r.text)
            self.assertIn('id="mass-actions-selected-count"', planner_r.text)
            self.assertIn('id="mass-actions-dialog"', planner_r.text)
            self.assertIn('id="pma-action-type"', planner_r.text)
            self.assertIn('id="pma-preview-btn"', planner_r.text)
            self.assertIn('id="pma-execute-confirm"', planner_r.text)
            self.assertIn('id="pma-execute-btn" disabled', planner_r.text)
            self.assertIn('id="pma-filter-executable-only"', planner_r.text)
            self.assertIn('data-pma-kind-filter="SUCCESS_CREATED_NEW"', planner_r.text)
            self.assertIn('data-pma-kind-filter="SUCCESS_RETURNED_EXISTING"', planner_r.text)
            self.assertIn('data-pma-kind-filter="SKIPPED_NON_EXECUTABLE"', planner_r.text)
            self.assertIn('data-pma-kind-filter="FAILED_INVALID_OR_INCONSISTENT"', planner_r.text)
            self.assertIn('id="pma-stale-banner"', planner_r.text)
            self.assertIn('id="pma-ttl-remaining"', planner_r.text)
            self.assertIn('id="pma-copy-summary-json-btn"', planner_r.text)
            self.assertIn('id="pma-copy-result-json-btn"', planner_r.text)
            self.assertIn("Preview changes nothing.", planner_r.text)
            self.assertIn("Preview is read-only until execute.", planner_r.text)
            self.assertIn("Execute performs only the selected batch action.", planner_r.text)
            self.assertIn("No render/upload/publish steps will start.", planner_r.text)
            self.assertIn('id="pma-result-total"', planner_r.text)
            self.assertIn('id="pma-result-succeeded"', planner_r.text)
            self.assertIn('id="pma-result-failed"', planner_r.text)
            self.assertIn('id="pma-result-skipped"', planner_r.text)
            self.assertIn('id="pma-result-created-new"', planner_r.text)
            self.assertIn('id="pma-result-returned-existing"', planner_r.text)
            self.assertIn("Metadata Bulk Preview", planner_r.text)
            self.assertIn("Preview does not change release metadata until you apply.", planner_r.text)
            self.assertIn("Applying changes affects only the selected fields of the selected release targets.", planner_r.text)
            self.assertIn("Applyable only", planner_r.text)
            self.assertIn("Overwrite only", planner_r.text)
            self.assertIn("Prepared batch state is stale or expired. Create a new bulk preview.", planner_r.text)
            self.assertIn("Create new bulk preview", planner_r.text)
            self.assertIn("Unresolved target", planner_r.text)
            self.assertIn("Duplicate target deduped", planner_r.text)
            self.assertIn("Invalid selection", planner_r.text)
            self.assertIn("overwrite confirmation is required", planner_r.text.lower())
            self.assertIn("Monthly Planning Templates", planner_r.text)
            self.assertIn('id="mpt-list-body"', planner_r.text)
            self.assertIn('id="mpt-detail-pane"', planner_r.text)
            self.assertIn('id="mpt-preview-modal"', planner_r.text)
            self.assertIn('id="mpt-filter-createable-only"', planner_r.text)
            self.assertIn('id="mpt-filter-conflicts-only"', planner_r.text)
            self.assertIn('id="mpt-copy-preview-json-btn"', planner_r.text)
            self.assertIn('id="mpt-copy-apply-json-btn"', planner_r.text)
            self.assertIn('id="mpt-apply-summary"', planner_r.text)
            self.assertIn('id="mpt-apply-total"', planner_r.text)
            self.assertIn('id="mpt-apply-created"', planner_r.text)
            self.assertIn('id="mpt-apply-blocked-duplicate"', planner_r.text)
            self.assertIn('id="mpt-apply-blocked-invalid-date"', planner_r.text)
            self.assertIn('id="mpt-apply-failed"', planner_r.text)
            self.assertIn('id="mpt-apply-overlap-warnings"', planner_r.text)
            self.assertIn('id="mpt-apply-items-body"', planner_r.text)

            analyzer_r = client.get("/ui/analyzer", headers=h)
            self.assertEqual(analyzer_r.status_code, 200)
            self.assertIn("Analyzer · Surface Family", analyzer_r.text)
            self.assertIn('data-analyzer-nav="OVERVIEW"', analyzer_r.text)
            self.assertIn('data-analyzer-nav="PORTFOLIO"', analyzer_r.text)
            self.assertIn('id="analyzer-surface-family-cards"', analyzer_r.text)
            self.assertIn('id="analyzer-selected-surface"', analyzer_r.text)
            self.assertIn('id="analyzer-active-api"', analyzer_r.text)
            self.assertIn('id="analyzer-status"', analyzer_r.text)
            self.assertIn('id="analyzer-data-json"', analyzer_r.text)
            self.assertIn('id="analyzer-chart-layout"', analyzer_r.text)
            self.assertIn('id="analyzer-line-chart"', analyzer_r.text)
            self.assertIn('id="analyzer-bar-chart"', analyzer_r.text)
            self.assertIn('id="analyzer-semantic-chips"', analyzer_r.text)
            self.assertIn('id="analyzer-backfill-panel"', analyzer_r.text)
            self.assertIn('id="backfill-days"', analyzer_r.text)
            self.assertIn('id="backfill-trigger"', analyzer_r.text)
            self.assertIn('id="backfill-runtime"', analyzer_r.text)
            self.assertIn("/v1/analytics/external/backfill", analyzer_r.text)
            self.assertIn('id="analyzer-planning-panel"', analyzer_r.text)
            self.assertIn('id="planning-scenario"', analyzer_r.text)
            self.assertIn('id="planning-windows"', analyzer_r.text)
            self.assertIn('id="planning-generate"', analyzer_r.text)
            self.assertIn('id="planning-output"', analyzer_r.text)
            self.assertIn("/v1/analytics/planning-assistant", analyzer_r.text)
            self.assertIn("generatePlanning", analyzer_r.text)
            self.assertIn("renderPlanningOutput", analyzer_r.text)
            self.assertIn('value="WEEK"', analyzer_r.text)
            self.assertIn('value="MONTH"', analyzer_r.text)
            self.assertIn('value="QUARTER"', analyzer_r.text)
            self.assertIn('id="analyzer-telegram-panel"', analyzer_r.text)
            self.assertIn('id="telegram-channel"', analyzer_r.text)
            self.assertIn('id="telegram-release"', analyzer_r.text)
            self.assertIn('id="telegram-delivery-mode"', analyzer_r.text)
            self.assertIn('id="telegram-generate"', analyzer_r.text)
            self.assertIn('id="telegram-output"', analyzer_r.text)
            self.assertIn("/v1/analytics/telegram/dispatch", analyzer_r.text)
            self.assertIn("generateTelegramSurface", analyzer_r.text)
            self.assertIn("renderTelegramSurface", analyzer_r.text)
            self.assertIn("animateLinePath", analyzer_r.text)
            self.assertIn("animateBars", analyzer_r.text)
            self.assertIn("requestAnimationFrame", analyzer_r.text)
            self.assertIn("loadSurface()", analyzer_r.text)
            self.assertIn("Archived templates are visible but cannot be previewed/applied.", planner_r.text)
            self.assertIn("Preview creates nothing.", planner_r.text)
            self.assertIn("Apply creates planned releases only.", planner_r.text)
            self.assertIn("Apply does not start materialization, job creation, render, upload, or publish.", planner_r.text)
            planner_js_r = client.get("/static/planner_bulk_releases.js", headers=h)
            self.assertEqual(planner_js_r.status_code, 200)
            self.assertIn("selected_items", planner_js_r.text)
            self.assertIn("selected_fields", planner_js_r.text)
            self.assertIn("overwrite_confirmed", planner_js_r.text)
            self.assertIn("updateMassActionSelectionState", planner_js_r.text)
            self.assertIn("createMassActionPreview", planner_js_r.text)
            self.assertIn("executeMassAction", planner_js_r.text)
            self.assertIn("refreshMassActionExecuteAvailability", planner_js_r.text)
            self.assertIn("/v1/planner/mass-actions/preview", planner_js_r.text)
            self.assertIn("data-pma-kind-filter", planner_js_r.text)
            self.assertIn("loadMonthlyTemplates", planner_js_r.text)
            self.assertIn("runMonthlyTemplatePreview", planner_js_r.text)
            self.assertIn("runMonthlyTemplateApply", planner_js_r.text)
            self.assertIn("renderMonthlyTemplateApplyResult", planner_js_r.text)
            self.assertIn("/v1/planner/monthly-planning-templates?", planner_js_r.text)
            self.assertIn("/v1/planner/monthly-planning-templates/${templateId}/preview-apply", planner_js_r.text)
            self.assertIn("/v1/planner/monthly-planning-templates/${templateId}/apply", planner_js_r.text)
            self.assertIn("preview_fingerprint", planner_js_r.text)
            self.assertIn('/static/planner_bulk_releases.js', planner_r.text)

            r = client.get("/ui/metadata/title-templates", headers=h)
            self.assertEqual(r.status_code, 200)
            self.assertIn("Title Templates", r.text)
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
            self.assertIn("Track Catalog", r.text)
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
            self.assertIn(f'name="audio_ids_text" rows="6" style="width:86ch;" >{expected_audio_ids}</textarea>', r.text)


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

    def test_job_edit_metadata_preview_apply_unified_flow(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="",
                    description="Manual Existing Description",
                    tags_csv="ambient,night",
                    cover_name="cover",
                    cover_ext="jpg",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="001",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                title_default_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Default Unified Title",
                    template_body="{{channel_display_name}}",
                    status="ACTIVE",
                    is_default=True,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                desc_default_id = dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Default Unified Description",
                    template_body="{{channel_display_name}} generated",
                    status="ACTIVE",
                    is_default=True,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                desc_explicit_id = dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Explicit Unified Description",
                    template_body="{{channel_slug}} explicit",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                tags_default_id = dbm.create_video_tag_preset(
                    conn,
                    channel_slug="darkwood-reverie",
                    preset_name="Default Unified Tags",
                    preset_body_json=dbm.json_dumps(["{{release_title}}", "ambient"]),
                    status="ACTIVE",
                    is_default=True,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                dbm.upsert_channel_metadata_defaults(
                    conn,
                    channel_slug="darkwood-reverie",
                    default_title_template_id=title_default_id,
                    default_description_template_id=desc_default_id,
                    default_video_tag_preset_id=tags_default_id,
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            edit_page = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(edit_page.status_code, 200)
            self.assertIn('id="mpa-section"', edit_page.text)
            self.assertIn("Preview does not change release metadata until you apply.", edit_page.text)
            self.assertIn("Temporary override affects only this operation.", edit_page.text)
            self.assertIn("Applying generated metadata does not change channel defaults.", edit_page.text)
            self.assertIn("Changing defaults is a separate action.", edit_page.text)
            self.assertIn("Other prepared fields can still be applied.", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/preview-apply/context", edit_page.text)
            self.assertIn("/v1/metadata/releases/${activeReleaseId}/preview-apply/preview", edit_page.text)
            self.assertIn("/v1/metadata/preview-apply/sessions/${sessionId}", edit_page.text)
            self.assertIn("/v1/metadata/preview-apply/sessions/${mpaSession.session_id}/apply", edit_page.text)
            self.assertIn("Explicit overwrite confirmation is required", edit_page.text)
            self.assertIn("No default source and no explicit source selected.", edit_page.text)
            self.assertIn("if (!overwriteEl || !overwriteEl.checked)", edit_page.text)
            self.assertIn("source.template_name || source.preset_name || source.name", edit_page.text)
            self.assertIn("defaultSource.template_name || defaultSource.preset_name || defaultSource.name", edit_page.text)
            self.assertIn("Clear override in preview request to return to channel default.", edit_page.text)
            self.assertIn("Using channel default. Select a source above to set temporary override for this operation.", edit_page.text)
            self.assertIn("Temporary override active", edit_page.text)
            self.assertIn("Use channel default", edit_page.text)
            self.assertIn("Clear override", edit_page.text)
            self.assertIn("clearMpaOverride(selectEl, fieldLabel)", edit_page.text)
            self.assertIn("mpaPreviewSourceModeByField = {", edit_page.text)
            self.assertIn("title: mpaSelectedSourceId(mpaTitleSourceSelect) === null ? 'default' : 'explicit'", edit_page.text)
            self.assertIn("renderMpaCurrentBundle(payload.current || {});", edit_page.text)
            self.assertIn("applyMpaResultToEditableInputs(payload.release_metadata_after || {});", edit_page.text)
            self.assertIn("Generation failed for this field.", edit_page.text)

            context = client.get(f"/v1/metadata/releases/{release_id}/preview-apply/context", headers=h)
            self.assertEqual(context.status_code, 200)
            context_payload = context.json()
            self.assertEqual(context_payload["release_id"], release_id)
            self.assertEqual(context_payload["current"]["title"], "")
            self.assertEqual(context_payload["current"]["description"], "Manual Existing Description")
            self.assertEqual(context_payload["current"]["tags_json"], ["ambient", "night"])

            preview = client.post(
                f"/v1/metadata/releases/{release_id}/preview-apply/preview",
                headers=h,
                json={
                    "fields": ["title", "description", "tags"],
                    "sources": {
                        "description_template_id": desc_explicit_id,
                        "title_template_id": None,
                        "video_tag_preset_id": None,
                    },
                },
            )
            self.assertEqual(preview.status_code, 200)
            preview_payload = preview.json()
            session_id = preview_payload["session_id"]

            session = client.get(f"/v1/metadata/preview-apply/sessions/{session_id}", headers=h)
            self.assertEqual(session.status_code, 200)
            session_payload = session.json()
            self.assertEqual(session_payload["fields"]["title"]["status"], "PROPOSED_READY")
            self.assertEqual(session_payload["fields"]["description"]["status"], "OVERWRITE_READY")
            self.assertEqual(session_payload["fields"]["tags"]["status"], "GENERATION_FAILED")
            self.assertTrue(session_payload["fields"]["title"]["source"]["name"])
            self.assertNotIn("template_name", session_payload["fields"]["title"]["source"])
            self.assertNotIn("preset_name", session_payload["fields"]["title"]["source"])
            self.assertTrue(session_payload["fields"]["description"]["source"]["name"])
            self.assertNotIn("template_name", session_payload["fields"]["description"]["source"])
            self.assertNotIn("preset_name", session_payload["fields"]["description"]["source"])
            self.assertEqual(session_payload["fields"]["description"]["source"]["id"], desc_explicit_id)
            self.assertIn("tags", session_payload["summary"]["failed_fields"])
            self.assertIn(desc_default_id, [item["id"] for item in context_payload["active_sources"]["description_templates"]])
            tags_default = context_payload["defaults"]["video_tag_preset"]
            self.assertTrue(tags_default.get("name"))
            self.assertNotIn("template_name", tags_default)
            self.assertNotIn("preset_name", tags_default)

            denied = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=h,
                json={"selected_fields": ["description"], "overwrite_confirmed_fields": []},
            )
            self.assertEqual(denied.status_code, 422)
            self.assertEqual(denied.json()["error"]["code"], "MPA_OVERWRITE_CONFIRMATION_REQUIRED")

            applied = client.post(
                f"/v1/metadata/preview-apply/sessions/{session_id}/apply",
                headers=h,
                json={"selected_fields": ["description"], "overwrite_confirmed_fields": ["description"]},
            )
            self.assertEqual(applied.status_code, 200)
            apply_payload = applied.json()
            self.assertEqual(apply_payload["applied_fields"], ["description"])
            self.assertNotIn("tags", apply_payload["applied_fields"])
            self.assertEqual(apply_payload["release_metadata_after"]["title"], "")
            self.assertEqual(apply_payload["release_metadata_after"]["tags_json"], ["ambient", "night"])
            self.assertEqual(apply_payload["release_metadata_after"]["description"], "darkwood-reverie explicit")

    def test_job_edit_visual_workflow_surface_history_reuse_and_batch_entry_points(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel
                channel_id = int(channel["id"])
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=channel_id,
                    title="visual ui",
                    description="d",
                    tags_csv="a",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="png",
                    audio_ids_text="1",
                )
                release_id = int(conn.execute("SELECT release_id FROM jobs WHERE id = ?", (job_id,)).fetchone()["release_id"])
                bg_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://ui-bg",
                    name="ui-bg.png",
                    path="/tmp/ui-bg.png",
                )
                cover_asset_id = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://ui-cover",
                    name="ui-cover.png",
                    path="/tmp/ui-cover.png",
                )
                prior_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'prior-ui', 'd', '[]', 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute(
                    """
                    INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at)
                    VALUES(?, ?, ?, NULL, 'seed', '2026-01-01T00:00:00+00:00')
                    """,
                    (prior_release, bg_asset_id, cover_asset_id),
                )
                conn.execute(
                    """
                    INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at)
                    VALUES(?, ?, ?, NULL, 'seed', '2026-01-02T00:00:00+00:00')
                    """,
                    (release_id, bg_asset_id, cover_asset_id),
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            edit_page = client.get(f"/ui/jobs/{job_id}/edit", headers=h)
            self.assertEqual(edit_page.status_code, 200)
            self.assertIn('id="visual-workflow-section"', edit_page.text)
            self.assertIn('id="visual-workflow-legend"', edit_page.text)
            self.assertIn("proposed visual package", edit_page.text)
            self.assertIn("thumbnail is derived from cover asset", edit_page.text)
            self.assertIn("Template management (Epic 5)", edit_page.text)
            self.assertIn("API-first operator flow", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/releases/${activeReleaseId}/effective", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/${templateId}", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/${templateId}/activate", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/${templateId}/set-default", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/releases/${activeReleaseId}/override", edit_page.text)
            self.assertIn("/v1/metadata/channel-visual-style-templates/releases/${activeReleaseId}/override/clear", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/background/candidates", edit_page.text)
            self.assertIn("Cover Candidates", edit_page.text)
            self.assertIn("Cover Preview", edit_page.text)
            self.assertIn("Cover Select", edit_page.text)
            self.assertIn("Cover Approve", edit_page.text)
            self.assertIn("Cover Apply", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/cover/candidates", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/cover/candidates/${encodeURIComponent(candidateId)}/preview", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/cover/select", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/cover/approve", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/cover/apply", edit_page.text)
            self.assertIn("/v1/visual/releases/${activeReleaseId}/history?limit=20", edit_page.text)
            self.assertIn("/v1/visual/batch/preview", edit_page.text)
            self.assertIn("/v1/visual/batch/execute", edit_page.text)

            history = client.get(f"/v1/visual/releases/{release_id}/history?limit=20", headers=h)
            self.assertEqual(history.status_code, 200)
            self.assertEqual(history.json()["release_id"], release_id)
            self.assertIsInstance(history.json()["items"], list)

            batch_preview = client.post(
                "/v1/visual/batch/preview",
                headers=h,
                json={
                    "action_type": "BULK_ASSIGN_BACKGROUND",
                    "selected_release_ids": [release_id],
                    "action_payload": {"background_asset_id": bg_asset_id},
                },
            )
            self.assertEqual(batch_preview.status_code, 200)
            item = batch_preview.json()["items"][0]
            self.assertIn("warning_codes", item)
            self.assertIn("REUSE_OVERRIDE_REQUIRED", item["warning_codes"])
            self.assertIn("reuse_warning", item)
            self.assertGreaterEqual(len(item["reuse_warning"]["prior_usage"]), 1)

            batch_execute = client.post(
                "/v1/visual/batch/execute",
                headers=h,
                json={
                    "preview_session_id": batch_preview.json()["preview_session_id"],
                    "selected_release_ids": [release_id],
                    "overwrite_confirmed": True,
                    "reuse_override_confirmed": False,
                },
            )
            self.assertEqual(batch_execute.status_code, 200)
            self.assertEqual(batch_execute.json()["items"][0]["status"], "BLOCKED")
            self.assertEqual(batch_execute.json()["items"][0]["reason_code"], "VBG_REUSE_OVERRIDE_REQUIRED")

            batch_preview_override = client.post(
                "/v1/visual/batch/preview",
                headers=h,
                json={
                    "action_type": "BULK_ASSIGN_BACKGROUND",
                    "selected_release_ids": [release_id],
                    "action_payload": {"background_asset_id": bg_asset_id},
                },
            )
            self.assertEqual(batch_preview_override.status_code, 200)
            batch_execute_override = client.post(
                "/v1/visual/batch/execute",
                headers=h,
                json={
                    "preview_session_id": batch_preview_override.json()["preview_session_id"],
                    "selected_release_ids": [release_id],
                    "overwrite_confirmed": True,
                    "reuse_override_confirmed": True,
                },
            )
            self.assertEqual(batch_execute_override.status_code, 200)
            self.assertEqual(batch_execute_override.json()["items"][0]["status"], "APPLIED")

            # Regression: existing non-visual page still renders.
            create_page = client.get("/ui/jobs/create", headers=h)
            self.assertEqual(create_page.status_code, 200)
            self.assertIn("Create Job", create_page.text)

    def test_channel_metadata_defaults_page_operator_flow(self) -> None:
        with temp_env() as (_, _):
            env = Env.load()
            seed_minimal_db(env)

            conn = dbm.connect(env)
            try:
                title_default_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Title Default",
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
                title_alt_id = dbm.create_title_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Title Alt",
                    template_body="{{channel_slug}} alt",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                desc_default_id = dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Description Default",
                    template_body="{{channel_display_name}} description",
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                desc_archived_id = dbm.create_description_template(
                    conn,
                    channel_slug="darkwood-reverie",
                    template_name="Description Archived Default",
                    template_body="{{channel_display_name}} old description",
                    status="ARCHIVED",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at="2026-02-01T00:00:00+00:00",
                )
                tags_default_id = dbm.create_video_tag_preset(
                    conn,
                    channel_slug="darkwood-reverie",
                    preset_name="Tags Default",
                    preset_body_json=dbm.json_dumps(["ambient"]),
                    status="ACTIVE",
                    is_default=False,
                    validation_status="VALID",
                    validation_errors_json=None,
                    last_validated_at="2026-01-01T00:00:00+00:00",
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    archived_at=None,
                )
                dbm.upsert_channel_metadata_defaults(
                    conn,
                    channel_slug="darkwood-reverie",
                    default_title_template_id=title_default_id,
                    default_description_template_id=desc_archived_id,
                    default_video_tag_preset_id=tags_default_id,
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                )
                conn.commit()
            finally:
                conn.close()

            mod = importlib.import_module("services.factory_api.app")
            importlib.reload(mod)
            client = TestClient(mod.app)
            h = basic_auth_header(env.basic_user, env.basic_pass)

            page = client.get("/ui/channels/darkwood-reverie/metadata-defaults", headers=h)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Channel Details → Metadata Defaults", page.text)
            self.assertIn("Channel defaults are canonical source settings.", page.text)
            self.assertIn("Changing defaults is a separate action.", page.text)
            self.assertIn("Future preview without temporary override will result in configuration missing for this field.", page.text)
            self.assertIn("window.confirm(warning)", page.text)
            self.assertIn('id="mdo-select-title"', page.text)
            self.assertIn('id="mdo-select-description"', page.text)
            self.assertIn('id="mdo-select-tags"', page.text)
            self.assertIn("configured + active/valid", page.text)
            self.assertIn("configured but unavailable", page.text)
            self.assertIn("not configured", page.text)
            self.assertIn("const activeSource = fieldSources.find((row) => Number(row.id) === Number(item.id));", page.text)
            self.assertIn("if (String(activeSource.validation_status || '') !== 'VALID') {", page.text)
            self.assertIn("/v1/metadata/channels/${encodeURIComponent(slug)}/defaults", page.text)
            self.assertIn("default_title_template_id", page.text)
            self.assertIn("default_description_template_id", page.text)
            self.assertIn("default_video_tag_preset_id", page.text)

            before = client.get("/v1/metadata/channels/darkwood-reverie/defaults", headers=h)
            self.assertEqual(before.status_code, 200)
            self.assertEqual(before.json()["defaults"]["title_template"]["id"], title_default_id)
            self.assertEqual(before.json()["defaults"]["description_template"]["id"], desc_archived_id)
            self.assertEqual(before.json()["defaults"]["video_tag_preset"]["id"], tags_default_id)

            active_desc = client.get("/v1/metadata/description-templates?channel_slug=darkwood-reverie&status=active", headers=h)
            self.assertEqual(active_desc.status_code, 200)
            active_desc_ids = [int(item["id"]) for item in active_desc.json()["items"]]
            self.assertIn(desc_default_id, active_desc_ids)
            self.assertNotIn(desc_archived_id, active_desc_ids)

            def _ui_status(default_ref: dict | None, active_items: list[dict]) -> str:
                if not default_ref:
                    return "not configured"
                match = next((row for row in active_items if int(row["id"]) == int(default_ref["id"])), None)
                if not match:
                    return "configured but unavailable"
                return "configured + active/valid" if str(match.get("validation_status") or "") == "VALID" else "configured but unavailable"

            runtime_status = _ui_status(before.json()["defaults"]["description_template"], active_desc.json()["items"])
            self.assertEqual(runtime_status, "configured but unavailable")

            update = client.put(
                "/v1/metadata/channels/darkwood-reverie/defaults",
                headers=h,
                json={
                    "default_title_template_id": title_alt_id,
                    "default_description_template_id": desc_default_id,
                    "default_video_tag_preset_id": tags_default_id,
                },
            )
            self.assertEqual(update.status_code, 200)
            self.assertEqual(update.json()["defaults"]["title_template"]["id"], title_alt_id)

            clear = client.put(
                "/v1/metadata/channels/darkwood-reverie/defaults",
                headers=h,
                json={
                    "default_title_template_id": title_alt_id,
                    "default_description_template_id": None,
                    "default_video_tag_preset_id": tags_default_id,
                },
            )
            self.assertEqual(clear.status_code, 200)
            self.assertIsNone(clear.json()["defaults"]["description_template"])


if __name__ == "__main__":
    unittest.main()
