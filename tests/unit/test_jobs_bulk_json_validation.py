from __future__ import annotations

import importlib
import os
import unittest

from services.common import db as dbm

from tests._helpers import seed_minimal_db, temp_env


class TestJobsBulkJsonValidation(unittest.TestCase):
    def _load_app_module(self):
        mod = importlib.import_module("services.factory_api.app")
        importlib.reload(mod)
        return mod

    def _valid_item(self, channel_id: int) -> dict[str, object]:
        return {
            "channel_id": channel_id,
            "title": "Bulk title",
            "description": "desc",
            "tags_csv": "one,two",
            "cover_name": "cover",
            "cover_ext": "png",
            "background_name": "bg",
            "background_ext": "jpg",
            "audio_ids_text": "1,2",
        }

    def test_parse_bulk_payload_validates_mode_and_items(self) -> None:
        mod = self._load_app_module()

        mode, error = mod._parse_bulk_payload(mod.UiJobsBulkJsonPayload(mode="bad", items=[{}]))
        self.assertIsNone(mode)
        self.assertEqual(error.status_code, 400)

        mode, error = mod._parse_bulk_payload(mod.UiJobsBulkJsonPayload(mode="create_draft_jobs", items=[]))
        self.assertIsNone(mode)
        self.assertEqual(error.status_code, 400)

        mode, error = mod._parse_bulk_payload(mod.UiJobsBulkJsonPayload(mode="enqueue_existing_jobs", items=[{"job_id": 1}]))
        self.assertEqual(mode, "enqueue_existing_jobs")
        self.assertIsNone(error)

    def test_validate_create_item_reuses_single_item_validation(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            mod = self._load_app_module()
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None

                parsed, err = mod._validate_create_item(conn, self._valid_item(int(channel["id"])))
                self.assertIsNotNone(parsed)
                self.assertIsNone(err)

                _, missing_title_err = mod._validate_create_item(
                    conn,
                    {**self._valid_item(int(channel["id"])), "title": "   "},
                )
                self.assertEqual(missing_title_err["code"], "UIJ_INVALID_INPUT")
                self.assertIn("title", missing_title_err["field_errors"])

                _, missing_project_err = mod._validate_create_item(
                    conn,
                    {**self._valid_item(999999)},
                )
                self.assertIn("project", missing_project_err["field_errors"])
            finally:
                conn.close()

    def test_preview_enqueue_existing_item_uses_guard_semantics(self) -> None:
        with temp_env() as (_, env):
            os.environ["GDRIVE_TOKENS_DIR"] = os.path.join(os.environ["FACTORY_STORAGE_ROOT"], "gdrive_tokens")
            seed_minimal_db(env)
            mod = self._load_app_module()

            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                job_id = dbm.create_ui_job_draft(
                    conn,
                    channel_id=int(channel["id"]),
                    title="To Preview",
                    description="",
                    tags_csv="",
                    cover_name="cover",
                    cover_ext="png",
                    background_name="bg",
                    background_ext="jpg",
                    audio_ids_text="1",
                    job_type="UI",
                )

                eligible = mod._preview_enqueue_existing_item(conn, {"job_id": job_id})
                self.assertTrue(eligible["enqueued"])

                dbm.update_job_state(conn, job_id, state="READY_FOR_RENDER", stage="FETCH")
                not_allowed = mod._preview_enqueue_existing_item(conn, {"job_id": job_id})
                self.assertEqual(not_allowed["error"]["code"], "UIJ_RENDER_NOT_ALLOWED")

                missing = mod._preview_enqueue_existing_item(conn, {"job_id": "x"})
                self.assertEqual(missing["error"]["code"], "UIJ_JOB_NOT_FOUND")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
