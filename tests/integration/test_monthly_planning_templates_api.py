from __future__ import annotations

import importlib
import unittest

from fastapi.testclient import TestClient

from services.common import db as dbm
from services.common.env import Env
from tests._helpers import basic_auth_header, seed_minimal_db, temp_env


class TestMonthlyPlanningTemplatesApi(unittest.TestCase):
    def _snapshot_counts(self, env: Env) -> dict[str, int]:
        conn = dbm.connect(env)
        try:
            return {
                "planned_releases": int(conn.execute("SELECT COUNT(*) AS c FROM planned_releases").fetchone()["c"]),
                "jobs": int(conn.execute("SELECT COUNT(*) AS c FROM jobs").fetchone()["c"]),
                "releases": int(conn.execute("SELECT COUNT(*) AS c FROM releases").fetchone()["c"]),
                "apply_runs": int(conn.execute("SELECT COUNT(*) AS c FROM monthly_planning_template_apply_runs").fetchone()["c"]),
                "apply_run_items": int(conn.execute("SELECT COUNT(*) AS c FROM monthly_planning_template_apply_run_items").fetchone()["c"]),
            }
        finally:
            conn.close()

    def _create_template(self, client: TestClient, auth: dict[str, str]) -> int:
        resp = client.post(
            "/v1/planner/monthly-planning-templates",
            headers=auth,
            json={
                "channel_id": 1,
                "template_name": "April core batch",
                "content_type": "LONG",
                "items": [
                    {
                        "item_key": "day-01-main",
                        "slot_code": "day_01_main",
                        "position": 1,
                        "title": "Release 01",
                        "day_of_month": 1,
                        "notes": "optional",
                    }
                ],
            },
        )
        self.assertEqual(resp.status_code, 201)
        return int(resp.json()["id"])

    def test_crud_and_archived_visibility(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            before = self._snapshot_counts(env)

            create_resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "April core batch",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "day-01-main",
                            "slot_code": "day_01_main",
                            "position": 1,
                            "title": "Release 01",
                            "day_of_month": 1,
                            "notes": "optional",
                        }
                    ],
                },
            )
            self.assertEqual(create_resp.status_code, 201)
            created = create_resp.json()
            self.assertEqual(created["status"], "ACTIVE")
            self.assertEqual(created["apply_run_count"], 0)
            tid = int(created["id"])

            list_resp = client.get("/v1/planner/monthly-planning-templates?channel_id=1&status=ACTIVE&q=april", headers=auth)
            self.assertEqual(list_resp.status_code, 200)
            list_body = list_resp.json()
            self.assertEqual(list_body["total"], 1)
            self.assertEqual(list_body["items"][0]["item_count"], 1)

            detail_resp = client.get(f"/v1/planner/monthly-planning-templates/{tid}", headers=auth)
            self.assertEqual(detail_resp.status_code, 200)
            self.assertEqual(detail_resp.json()["template_name"], "April core batch")

            patch_resp = client.patch(
                f"/v1/planner/monthly-planning-templates/{tid}",
                headers=auth,
                json={
                    "template_name": "April core batch v2",
                    "items": [
                        {
                            "item_key": "day-02-main",
                            "slot_code": "day_02_main",
                            "position": 1,
                            "title": "Release 02",
                            "day_of_month": 2,
                            "notes": None,
                        }
                    ],
                },
            )
            self.assertEqual(patch_resp.status_code, 200)
            patched = patch_resp.json()
            self.assertEqual(patched["template_name"], "April core batch v2")
            self.assertEqual(patched["items"][0]["item_key"], "day-02-main")

            archive_resp = client.post(f"/v1/planner/monthly-planning-templates/{tid}/archive", headers=auth)
            self.assertEqual(archive_resp.status_code, 200)
            self.assertEqual(archive_resp.json()["status"], "ARCHIVED")

            archived_list = client.get("/v1/planner/monthly-planning-templates?status=ARCHIVED", headers=auth)
            self.assertEqual(archived_list.status_code, 200)
            self.assertEqual(archived_list.json()["total"], 1)

            archived_detail = client.get(f"/v1/planner/monthly-planning-templates/{tid}", headers=auth)
            self.assertEqual(archived_detail.status_code, 200)
            self.assertEqual(archived_detail.json()["status"], "ARCHIVED")

            after = self._snapshot_counts(env)
            self.assertEqual(before, after)

    def test_validation_errors_use_error_envelope(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)

            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "bad",
                    "content_type": "",
                    "items": [
                        {
                            "item_key": "bad-key",
                            "slot_code": "slot_1",
                            "position": 1,
                            "title": "x",
                            "day_of_month": 1,
                            "notes": None,
                        }
                    ],
                },
            )
            self.assertEqual(resp.status_code, 400)
            body = resp.json()
            self.assertEqual(body["error"]["code"], "MPT_INVALID_CONTENT_TYPE")

            invalid_position = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "bad-pos",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "k1",
                            "slot_code": "s1",
                            "position": "oops",
                            "title": "x",
                            "day_of_month": 1,
                            "notes": None,
                        }
                    ],
                },
            )
            self.assertEqual(invalid_position.status_code, 400)
            invalid_body = invalid_position.json()
            self.assertEqual(invalid_body["error"]["code"], "MPT_INVALID_ITEM_POSITION")

            duplicate_position = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "dup-pos",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "k1",
                            "slot_code": "s1",
                            "position": 1,
                            "title": "x",
                            "day_of_month": 1,
                            "notes": None,
                        },
                        {
                            "item_key": "k2",
                            "slot_code": "s2",
                            "position": 1,
                            "title": "y",
                            "day_of_month": 2,
                            "notes": None,
                        },
                    ],
                },
            )
            self.assertEqual(duplicate_position.status_code, 400)
            duplicate_body = duplicate_position.json()
            self.assertEqual(duplicate_body["error"]["code"], "MPT_DUPLICATE_POSITION")

    def test_preview_apply_zero_conflicts(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)

            resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertTrue(body["preview_fingerprint"])
            self.assertEqual(body["summary"]["total_items"], 1)
            self.assertEqual(body["summary"]["would_create"], 1)
            self.assertEqual(body["summary"]["blocked_duplicates"], 0)
            self.assertEqual(body["summary"]["blocked_invalid_dates"], 0)
            self.assertEqual(body["summary"]["overlap_warnings"], 0)
            self.assertEqual(body["items"][0]["outcome"], "WOULD_CREATE")
            self.assertEqual(body["items"][0]["reasons"], [])
            self.assertEqual(body["items"][0]["overlap_warnings"], [])

    def test_preview_apply_hard_duplicate(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )
            finally:
                conn.close()

            resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"]["blocked_duplicates"], 1)
            self.assertEqual(body["items"][0]["outcome"], "BLOCKED_DUPLICATE")

    def test_preview_apply_hard_duplicate_by_provenance_even_when_publish_at_month_differs(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO planned_releases(
                        channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at,
                        source_template_id, source_template_item_key, source_template_target_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "darkwood-reverie",
                        "LONG",
                        "existing",
                        "2026-03-01",
                        None,
                        "PLANNED",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        tid,
                        "day-01-main",
                        "2026-04",
                    ),
                )
            finally:
                conn.close()

            resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"]["blocked_duplicates"], 1)
            self.assertEqual(body["items"][0]["outcome"], "BLOCKED_DUPLICATE")
            self.assertEqual(body["items"][0]["reasons"][0]["code"], "MPT_DUPLICATE_PLANNING_SLOT")
    def test_preview_apply_soft_overlap(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "other_slot"),
                )
            finally:
                conn.close()

            resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(resp.status_code, 200)
            body = resp.json()
            self.assertEqual(body["summary"]["would_create"], 1)
            self.assertEqual(body["summary"]["overlap_warnings"], 1)
            self.assertEqual(body["items"][0]["outcome"], "WOULD_CREATE")
            self.assertEqual(len(body["items"][0]["overlap_warnings"]), 1)

    def test_preview_apply_invalid_day_for_month(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "April invalid day",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "day-31-main",
                            "slot_code": "day_31_main",
                            "position": 1,
                            "title": "Release 31",
                            "day_of_month": 31,
                            "notes": "optional",
                        }
                    ],
                },
            )
            tid = int(resp.json()["id"])

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 200)
            body = preview.json()
            self.assertEqual(body["summary"]["blocked_invalid_dates"], 1)
            self.assertEqual(body["items"][0]["outcome"], "BLOCKED_INVALID_DATE")
            self.assertEqual(body["items"][0]["reasons"][0]["code"], "MPT_INVALID_ITEM_DAY_FOR_MONTH")

    def test_preview_apply_archived_template_blocked(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            client.post(f"/v1/planner/monthly-planning-templates/{tid}/archive", headers=auth)

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 409)
            self.assertEqual(preview.json()["error"]["code"], "MPT_TEMPLATE_ARCHIVED")

    def test_preview_apply_scope_mismatch_blocked(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 2, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 409)
            self.assertEqual(preview.json()["error"]["code"], "MPT_SCOPE_MISMATCH")

    def test_preview_apply_has_no_persistence_side_effects(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            before = self._snapshot_counts(env)

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 200)
            after = self._snapshot_counts(env)
            self.assertEqual(before, after)

    def test_apply_success_persists_audit_and_provenance_without_downstream_side_effects(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)
            before = self._snapshot_counts(env)

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 200)
            fp = preview.json()["preview_fingerprint"]
            apply_resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": fp},
            )
            self.assertEqual(apply_resp.status_code, 200)
            body = apply_resp.json()
            self.assertEqual(body["summary"]["created"], 1)
            self.assertEqual(body["summary"]["failed"], 0)
            self.assertEqual(body["items"][0]["outcome"], "CREATED")

            conn = dbm.connect(env)
            try:
                run = conn.execute(
                    "SELECT created_count, blocked_duplicate_count, blocked_invalid_date_count, failed_count FROM monthly_planning_template_apply_runs WHERE id = ?",
                    (int(body["apply_run_id"]),),
                ).fetchone()
                self.assertEqual(int(run["created_count"]), 1)
                release = conn.execute(
                    """
                    SELECT planning_slot_code, source_template_id, source_template_item_key, source_template_target_month, source_template_apply_run_id
                    FROM planned_releases
                    WHERE id = ?
                    """,
                    (int(body["items"][0]["planned_release_id"]),),
                ).fetchone()
                self.assertEqual(str(release["planning_slot_code"]), "day_01_main")
                self.assertEqual(int(release["source_template_id"]), tid)
                self.assertEqual(str(release["source_template_item_key"]), "day-01-main")
                self.assertEqual(str(release["source_template_target_month"]), "2026-04")
                self.assertEqual(int(release["source_template_apply_run_id"]), int(body["apply_run_id"]))
            finally:
                conn.close()

            after = self._snapshot_counts(env)
            self.assertEqual(after["planned_releases"], before["planned_releases"] + 1)
            self.assertEqual(after["apply_runs"], before["apply_runs"] + 1)
            self.assertEqual(after["apply_run_items"], before["apply_run_items"] + 1)
            self.assertEqual(after["jobs"], before["jobs"])
            self.assertEqual(after["releases"], before["releases"])

    def test_apply_blocks_invalid_day_and_duplicate_and_requires_fingerprint(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            create_resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "April mixed",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "day-01-main",
                            "slot_code": "day_01_main",
                            "position": 1,
                            "title": "Release 01",
                            "day_of_month": 1,
                            "notes": None,
                        },
                        {
                            "item_key": "day-31-main",
                            "slot_code": "day_31_main",
                            "position": 2,
                            "title": "Release 31",
                            "day_of_month": 31,
                            "notes": None,
                        },
                    ],
                },
            )
            tid = int(create_resp.json()["id"])
            missing_fp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(missing_fp.status_code, 400)
            self.assertEqual(missing_fp.json()["error"]["code"], "MPT_PREVIEW_FINGERPRINT_REQUIRED")

            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "day_01_main"),
                )
            finally:
                conn.close()

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            fp = preview.json()["preview_fingerprint"]
            apply_resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": fp},
            )
            self.assertEqual(apply_resp.status_code, 200)
            body = apply_resp.json()
            self.assertEqual(body["summary"]["created"], 0)
            self.assertEqual(body["summary"]["blocked_duplicates"], 1)
            self.assertEqual(body["summary"]["blocked_invalid_dates"], 1)
            self.assertEqual({row["outcome"] for row in body["items"]}, {"BLOCKED_DUPLICATE", "BLOCKED_INVALID_DATE"})

    def test_apply_scope_archived_and_stale_preview(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            fp = preview.json()["preview_fingerprint"]

            scope_mismatch = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 2, "target_month": "2026-04", "preview_fingerprint": fp},
            )
            self.assertEqual(scope_mismatch.status_code, 409)
            self.assertEqual(scope_mismatch.json()["error"]["code"], "MPT_SCOPE_MISMATCH")

            client.post(f"/v1/planner/monthly-planning-templates/{tid}/archive", headers=auth)
            archived_apply = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": fp},
            )
            self.assertEqual(archived_apply.status_code, 409)
            self.assertEqual(archived_apply.json()["error"]["code"], "MPT_TEMPLATE_ARCHIVED")

            tid2 = self._create_template(client, auth)
            p1 = client.post(
                f"/v1/planner/monthly-planning-templates/{tid2}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            ).json()["preview_fingerprint"]
            client.patch(
                f"/v1/planner/monthly-planning-templates/{tid2}",
                headers=auth,
                json={
                    "template_name": "April core batch v2",
                    "items": [
                        {
                            "item_key": "day-02-main",
                            "slot_code": "day_02_main",
                            "position": 1,
                            "title": "Release 02",
                            "day_of_month": 2,
                            "notes": None,
                        }
                    ],
                },
            )
            stale = client.post(
                f"/v1/planner/monthly-planning-templates/{tid2}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": p1},
            )
            self.assertEqual(stale.status_code, 409)
            self.assertEqual(stale.json()["error"]["code"], "MPT_PREVIEW_STALE")

    def test_apply_repeat_with_refreshed_preview_produces_blocked_duplicates_without_new_inserts(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")
            tid = self._create_template(client, auth)

            before = self._snapshot_counts(env)
            first_preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(first_preview.status_code, 200)
            first_fp = first_preview.json()["preview_fingerprint"]

            first_apply = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": first_fp},
            )
            self.assertEqual(first_apply.status_code, 200)
            first_body = first_apply.json()
            self.assertEqual(first_body["summary"]["created"], 1)
            self.assertEqual(first_body["summary"]["blocked_duplicates"], 0)
            self.assertEqual(first_body["items"][0]["outcome"], "CREATED")

            second_preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(second_preview.status_code, 200)
            second_body_preview = second_preview.json()
            self.assertEqual(second_body_preview["summary"]["would_create"], 0)
            self.assertEqual(second_body_preview["summary"]["blocked_duplicates"], 1)
            self.assertEqual(second_body_preview["items"][0]["outcome"], "BLOCKED_DUPLICATE")
            second_fp = second_body_preview["preview_fingerprint"]

            second_apply = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": second_fp},
            )
            self.assertEqual(second_apply.status_code, 200)
            second_body = second_apply.json()
            self.assertEqual(second_body["summary"]["created"], 0)
            self.assertEqual(second_body["summary"]["blocked_duplicates"], 1)
            self.assertEqual(second_body["items"][0]["outcome"], "BLOCKED_DUPLICATE")

            after = self._snapshot_counts(env)
            self.assertEqual(after["planned_releases"], before["planned_releases"] + 1)
            self.assertEqual(after["jobs"], before["jobs"])
            self.assertEqual(after["releases"], before["releases"])
            self.assertEqual(after["apply_runs"], before["apply_runs"] + 2)
            self.assertEqual(after["apply_run_items"], before["apply_run_items"] + 2)

    def test_apply_mixed_legacy_and_template_created_collision_set_is_deterministic(self) -> None:
        with temp_env() as (_td, _):
            env = Env.load()
            seed_minimal_db(env)
            mod = importlib.import_module("services.factory_api.app")
            mod = importlib.reload(mod)
            client = TestClient(mod.app)
            auth = basic_auth_header("admin", "testpass")

            create_resp = client.post(
                "/v1/planner/monthly-planning-templates",
                headers=auth,
                json={
                    "channel_id": 1,
                    "template_name": "April mixed collisions",
                    "content_type": "LONG",
                    "items": [
                        {
                            "item_key": "slot-duplicate",
                            "slot_code": "legacy_slot",
                            "position": 1,
                            "title": "Legacy slot duplicate",
                            "day_of_month": 1,
                            "notes": None,
                        },
                        {
                            "item_key": "provenance-duplicate",
                            "slot_code": "new_slot",
                            "position": 2,
                            "title": "Provenance duplicate",
                            "day_of_month": 2,
                            "notes": None,
                        },
                    ],
                },
            )
            self.assertEqual(create_resp.status_code, 201)
            tid = int(create_resp.json()["id"])

            conn = dbm.connect(env)
            try:
                conn.execute(
                    """
                    INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at, planning_slot_code)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("darkwood-reverie", "LONG", "legacy-existing", "2026-04-01", None, "PLANNED", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "legacy_slot"),
                )
                conn.execute(
                    """
                    INSERT INTO planned_releases(
                        channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at,
                        source_template_id, source_template_item_key, source_template_target_month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "darkwood-reverie",
                        "LONG",
                        "template-existing",
                        "2026-03-03",
                        None,
                        "PLANNED",
                        "2026-01-01T00:00:00Z",
                        "2026-01-01T00:00:00Z",
                        tid,
                        "provenance-duplicate",
                        "2026-04",
                    ),
                )
            finally:
                conn.close()

            preview = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/preview-apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04"},
            )
            self.assertEqual(preview.status_code, 200)
            preview_body = preview.json()
            self.assertEqual(preview_body["summary"]["would_create"], 0)
            self.assertEqual(preview_body["summary"]["blocked_duplicates"], 2)
            self.assertEqual({item["outcome"] for item in preview_body["items"]}, {"BLOCKED_DUPLICATE"})
            fp = preview_body["preview_fingerprint"]

            apply_resp = client.post(
                f"/v1/planner/monthly-planning-templates/{tid}/apply",
                headers=auth,
                json={"channel_id": 1, "target_month": "2026-04", "preview_fingerprint": fp},
            )
            self.assertEqual(apply_resp.status_code, 200)
            body = apply_resp.json()
            self.assertEqual(body["summary"]["created"], 0)
            self.assertEqual(body["summary"]["blocked_duplicates"], 2)
            self.assertEqual({item["outcome"] for item in body["items"]}, {"BLOCKED_DUPLICATE"})


if __name__ == "__main__":
    unittest.main()
