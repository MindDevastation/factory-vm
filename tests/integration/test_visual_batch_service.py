from __future__ import annotations

from datetime import datetime, timedelta, timezone
import unittest
from unittest import mock

from services.common import db as dbm
from services.planner import visual_batch_service as vbatch
from tests._helpers import seed_minimal_db, temp_env


class TestVisualBatchService(unittest.TestCase):
    def _seed_release(self, conn: object, *, title: str, with_applied_package: bool) -> tuple[int, int]:
        channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
        assert channel is not None
        channel_id = int(channel["id"])
        release_id = int(
            conn.execute(
                """
                INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, current_open_job_id, created_at)
                VALUES(?, ?, 'desc', '[]', NULL, NULL, ?, NULL, 1.0)
                """,
                (channel_id, title, f"origin-{title}"),
            ).lastrowid
        )
        background_asset_id = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="LOCAL",
            origin_id=f"local://bg-{title}",
            name=f"bg-{title}.png",
            path=f"/tmp/bg-{title}.png",
        )
        cover_asset_id = dbm.create_asset(
            conn,
            channel_id=channel_id,
            kind="IMAGE",
            origin="LOCAL",
            origin_id=f"local://cover-{title}",
            name=f"cover-{title}.png",
            path=f"/tmp/cover-{title}.png",
        )
        if with_applied_package:
            conn.execute(
                """
                INSERT INTO release_visual_applied_packages(
                    release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at
                ) VALUES(?, ?, ?, NULL, 'seed', ?)
                """,
                (release_id, int(background_asset_id), int(cover_asset_id), datetime.now(timezone.utc).isoformat()),
            )
        return release_id, int(background_asset_id)

    def test_preview_and_execute_contract_shapes_for_all_actions(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                r1, bg1 = self._seed_release(conn, title="shape-a", with_applied_package=True)
                r2, _ = self._seed_release(conn, title="shape-b", with_applied_package=False)

                for action in sorted(vbatch.ALLOWED_ACTION_TYPES):
                    preview = vbatch.create_visual_batch_preview_session(
                        conn,
                        action_type=action,
                        selected_release_ids=[r1, r2],
                        created_by="operator",
                        action_payload={"background_asset_id": bg1},
                    )
                    self.assertIn("preview_session_id", preview)
                    self.assertIn("aggregate", preview)
                    self.assertIn("items", preview)
                    self.assertEqual(preview["aggregate"]["scope_total"], 2)
                    self.assertEqual(len(preview["items"]), 2)
                    for item in preview["items"]:
                        self.assertIn("release_id", item)
                        self.assertIn("status", item)
                        self.assertIn("warning_codes", item)
                        self.assertIn("applied_package_exists", item)

                    with mock.patch(
                        "services.planner.visual_batch_service.cover_assignment_service.apply_cover_candidate",
                        return_value={"preview_id": "mock-cover-preview"},
                    ), mock.patch(
                        "services.planner.visual_batch_service.background_assignment_service.apply_background_assignment",
                        return_value={"preview_id": "mock-bg-preview"},
                    ):
                        executed = vbatch.execute_visual_batch_preview_session(
                            conn,
                            preview_session_id=preview["preview_session_id"],
                            selected_release_ids=[r1, r2],
                            overwrite_confirmed=True,
                            reuse_override_confirmed=True,
                            executed_by="operator",
                        )
                    self.assertEqual(executed["aggregate"]["scope_total"], 2)
                    self.assertIn("executed_count", executed["aggregate"])
                    self.assertIn("blocked_count", executed["aggregate"])
                    self.assertEqual(len(executed["items"]), 2)
                    for result in executed["items"]:
                        self.assertIn("release_id", result)
                        self.assertIn("status", result)
            finally:
                conn.close()

    def test_execute_invalidation_reason_details_and_scope_safety(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id, _ = self._seed_release(conn, title="invalidation", with_applied_package=False)
                preview = vbatch.create_visual_batch_preview_session(
                    conn,
                    action_type="BULK_GENERATE_PREVIEWS",
                    selected_release_ids=[release_id],
                    created_by="operator",
                )
                session_id = preview["preview_session_id"]

                conn.execute(
                    "UPDATE release_visual_batch_preview_sessions SET expires_at = ? WHERE id = ?",
                    ((datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(), session_id),
                )
                conn.commit()

                with self.assertRaises(vbatch.VisualBatchError) as expired:
                    vbatch.execute_visual_batch_preview_session(
                        conn,
                        preview_session_id=session_id,
                        selected_release_ids=[release_id],
                        overwrite_confirmed=True,
                        reuse_override_confirmed=False,
                        executed_by="operator",
                    )
                self.assertEqual(expired.exception.code, "VBATCH_PREVIEW_SESSION_EXPIRED")
                self.assertEqual(expired.exception.details["invalidation_reason_code"], "PREVIEW_SESSION_EXPIRED")

                preview2 = vbatch.create_visual_batch_preview_session(
                    conn,
                    action_type="BULK_GENERATE_PREVIEWS",
                    selected_release_ids=[release_id],
                    created_by="operator",
                )
                with self.assertRaises(vbatch.VisualBatchError) as scope_invalid:
                    vbatch.execute_visual_batch_preview_session(
                        conn,
                        preview_session_id=preview2["preview_session_id"],
                        selected_release_ids=[release_id, 999999],
                        overwrite_confirmed=True,
                        reuse_override_confirmed=False,
                        executed_by="operator",
                    )
                self.assertEqual(scope_invalid.exception.code, "VBATCH_PREVIEW_SCOPE_INVALIDATED")
                self.assertEqual(scope_invalid.exception.details["invalidation_reason_code"], "PREVIEW_SCOPE_INVALIDATED")
            finally:
                conn.close()

    def test_overwrite_requires_explicit_confirmation_and_no_silent_overwrite(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                applied_release, _ = self._seed_release(conn, title="applied", with_applied_package=True)
                fresh_release, _ = self._seed_release(conn, title="fresh", with_applied_package=False)
                preview = vbatch.create_visual_batch_preview_session(
                    conn,
                    action_type="BULK_APPROVE_APPLY",
                    selected_release_ids=[applied_release, fresh_release],
                    created_by="operator",
                )
                with mock.patch(
                    "services.planner.visual_batch_service.cover_assignment_service.apply_cover_candidate",
                    return_value={"preview_id": "mock-cover-preview"},
                ), mock.patch(
                    "services.planner.visual_batch_service.background_assignment_service.apply_background_assignment",
                    return_value={"preview_id": "mock-bg-preview"},
                ):
                    executed = vbatch.execute_visual_batch_preview_session(
                        conn,
                        preview_session_id=preview["preview_session_id"],
                        selected_release_ids=[applied_release, fresh_release],
                        overwrite_confirmed=False,
                        reuse_override_confirmed=False,
                        executed_by="operator",
                    )

                blocked = {int(item["release_id"]): item for item in executed["items"]}
                self.assertEqual(blocked[applied_release]["status"], "BLOCKED")
                self.assertEqual(blocked[applied_release]["reason_code"], "OVERWRITE_REQUIRES_EXPLICIT_DECISION")
                self.assertTrue(blocked[applied_release]["overwrite_requires_confirmation"])
                self.assertEqual(blocked[fresh_release]["status"], "APPLIED")
            finally:
                conn.close()

    def test_batch_preview_surfaces_reuse_warning_with_prior_usage_and_override_path(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                channel = dbm.get_channel_by_slug(conn, "darkwood-reverie")
                assert channel is not None
                channel_id = int(channel["id"])

                shared_cover = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://shared-cover",
                    name="shared-cover.png",
                    path="/tmp/shared-cover.png",
                )
                reused_background = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://reused-bg",
                    name="reused-bg.png",
                    path="/tmp/reused-bg.png",
                )
                alt_background = dbm.create_asset(
                    conn,
                    channel_id=channel_id,
                    kind="IMAGE",
                    origin="LOCAL",
                    origin_id="local://alt-bg",
                    name="alt-bg.png",
                    path="/tmp/alt-bg.png",
                )

                prior_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'prior', 'd', '[]', 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                target_release = int(
                    conn.execute(
                        "INSERT INTO releases(channel_id, title, description, tags_json, created_at) VALUES(?, 'target', 'd', '[]', 1.0)",
                        (channel_id,),
                    ).lastrowid
                )
                conn.execute(
                    """
                    INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at)
                    VALUES(?, ?, ?, NULL, 'seed', ?)
                    """,
                    (prior_release, int(reused_background), int(shared_cover), datetime.now(timezone.utc).isoformat()),
                )
                conn.execute(
                    """
                    INSERT INTO release_visual_applied_packages(release_id, background_asset_id, cover_asset_id, source_preview_id, applied_by, applied_at)
                    VALUES(?, ?, ?, NULL, 'seed', ?)
                    """,
                    (target_release, int(alt_background), int(shared_cover), datetime.now(timezone.utc).isoformat()),
                )

                preview = vbatch.create_visual_batch_preview_session(
                    conn,
                    action_type="BULK_ASSIGN_BACKGROUND",
                    selected_release_ids=[target_release],
                    created_by="operator",
                    action_payload={"background_asset_id": int(reused_background)},
                )
                item = preview["items"][0]
                self.assertIn("REUSE_OVERRIDE_REQUIRED", item["warning_codes"])
                self.assertIn("reuse_warning", item)
                self.assertTrue(item["reuse_warning"]["requires_override"])
                self.assertEqual(int(item["reuse_warning"]["prior_usage"][0]["release_id"]), prior_release)

                blocked = vbatch.execute_visual_batch_preview_session(
                    conn,
                    preview_session_id=preview["preview_session_id"],
                    selected_release_ids=[target_release],
                    overwrite_confirmed=True,
                    reuse_override_confirmed=False,
                    executed_by="operator",
                )
                self.assertEqual(blocked["items"][0]["status"], "BLOCKED")
                self.assertEqual(blocked["items"][0]["reason_code"], "VBG_REUSE_OVERRIDE_REQUIRED")

                preview_override = vbatch.create_visual_batch_preview_session(
                    conn,
                    action_type="BULK_ASSIGN_BACKGROUND",
                    selected_release_ids=[target_release],
                    created_by="operator",
                    action_payload={"background_asset_id": int(reused_background)},
                )
                allowed = vbatch.execute_visual_batch_preview_session(
                    conn,
                    preview_session_id=preview_override["preview_session_id"],
                    selected_release_ids=[target_release],
                    overwrite_confirmed=True,
                    reuse_override_confirmed=True,
                    executed_by="operator",
                )
                self.assertEqual(allowed["items"][0]["status"], "APPLIED")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
