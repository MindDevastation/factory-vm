from __future__ import annotations

import unittest

from services.common import db as dbm
from services.planner.materialization_foundation import (
    BindingInvariantResult,
    build_release_payload_from_planned_release,
    derive_binding_diagnostics_inputs,
    derive_materialization_state_summary_inputs,
    find_planned_release_by_materialized_release_id,
    get_bound_release_for_planned_release,
    get_planned_release_by_id,
    set_materialized_release_id,
    validate_binding_invariants,
)
from tests._helpers import seed_minimal_db, temp_env


class TestPlannerMaterializationFoundation(unittest.TestCase):
    def _insert_planned_release(
        self,
        conn,
        *,
        title: str = "Planned title",
        publish_at: str = "2026-01-01T00:00:00Z",
    ) -> int:
        cur = conn.execute(
            """
            INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at)
            VALUES('darkwood-reverie', 'LONG', ?, ?, 'seed notes', 'PLANNED', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (title, publish_at),
        )
        return int(cur.lastrowid)

    def _insert_release(self, conn, *, meta_id: str = "meta-prm-1") -> int:
        channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = 'darkwood-reverie'").fetchone()["id"])
        cur = conn.execute(
            """
            INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_release_folder_id, origin_meta_file_id, created_at)
            VALUES(?, 'R', 'D', '[]', '2026-01-01T00:00:00Z', NULL, ?, 1.0)
            """,
            (channel_id, meta_id),
        )
        return int(cur.lastrowid)

    def test_binding_validator_happy_path(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                pr_id = self._insert_planned_release(conn)
                release_id = self._insert_release(conn)
                set_materialized_release_id(conn, planned_release_id=pr_id, materialized_release_id=release_id)

                planned = get_planned_release_by_id(conn, planned_release_id=pr_id)
                assert planned is not None
                result = validate_binding_invariants(conn, planned_release=planned)

                self.assertEqual(result.invariant_status, "OK")
                self.assertTrue(result.linked_release_exists)
            finally:
                conn.close()

    def test_release_missing_behind_materialized_release_id(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                pr_id = self._insert_planned_release(conn)
                release_id = self._insert_release(conn)
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute(
                    "UPDATE planned_releases SET materialized_release_id = ? WHERE id = ?",
                    (release_id, pr_id),
                )
                conn.execute("DELETE FROM releases WHERE id = ?", (release_id,))
                conn.execute("PRAGMA foreign_keys=ON")

                planned = get_planned_release_by_id(conn, planned_release_id=pr_id)
                assert planned is not None
                result = validate_binding_invariants(conn, planned_release=planned)

                self.assertEqual(result.invariant_status, "INCONSISTENT")
                self.assertEqual(result.invariant_reason, "MATERIALIZED_RELEASE_MISSING")
            finally:
                conn.close()

    def test_duplicate_reverse_binding_detection(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                release_id = self._insert_release(conn)
                first = self._insert_planned_release(conn, title="one", publish_at="2026-01-01T00:00:00Z")
                second = self._insert_planned_release(conn, title="two", publish_at="2026-01-02T00:00:00Z")
                conn.execute("DROP INDEX IF EXISTS idx_pr_materialized_release_unique")
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.execute("PRAGMA ignore_check_constraints=ON")
                conn.execute(
                    "UPDATE planned_releases SET materialized_release_id = ? WHERE id IN (?, ?)",
                    (release_id, first, second),
                )
                conn.execute("PRAGMA foreign_keys=ON")

                row = get_planned_release_by_id(conn, planned_release_id=first)
                assert row is not None
                result = validate_binding_invariants(conn, planned_release=row)
                self.assertEqual(result.invariant_status, "INCONSISTENT")
                self.assertEqual(result.invariant_reason, "RELEASE_BOUND_TO_ANOTHER_PLANNED_RELEASE")
            finally:
                conn.close()

    def test_mapping_builder_uses_only_canonical_existing_fields(self) -> None:
        payload = build_release_payload_from_planned_release(
            planned_release={
                "id": 1,
                "channel_slug": "darkwood-reverie",
                "publish_at": "2026-01-01T00:00:00Z",
                "title": "  Planned title  ",
                "notes": "should not become description",
            }
        )
        self.assertEqual(
            payload,
            {
                "channel_slug": "darkwood-reverie",
                "planned_at": "2026-01-01T00:00:00Z",
                "title": "Planned title",
            },
        )

    def test_mapping_builder_does_not_invent_or_generate_missing_data(self) -> None:
        payload = build_release_payload_from_planned_release(
            planned_release={
                "id": 1,
                "channel_slug": "darkwood-reverie",
                "publish_at": None,
                "title": "   ",
                "notes": "free-form notes",
            }
        )
        self.assertEqual(payload, {"channel_slug": "darkwood-reverie", "planned_at": None})
        self.assertNotIn("description", payload)
        self.assertNotIn("tags_json", payload)

    def test_materialization_state_summary_helper_input_derivation(self) -> None:
        planned = {"id": 10, "materialized_release_id": None}
        summary = derive_materialization_state_summary_inputs(
            planned_release=planned,
            invariant_result=BindingInvariantResult(
                invariant_status="OK",
                invariant_reason=None,
                linked_release_exists=False,
            ),
            action_enabled=True,
        )
        self.assertEqual(summary["materialization_state"], "NOT_MATERIALIZED")

    def test_binding_diagnostics_helper_input_derivation(self) -> None:
        diagnostics = derive_binding_diagnostics_inputs(
            planned_release={"id": 11, "materialized_release_id": 22},
            invariant_result=BindingInvariantResult(
                invariant_status="INCONSISTENT",
                invariant_reason="CONTRADICTORY_LINKAGE_STATE",
                linked_release_exists=True,
            ),
        )
        self.assertEqual(diagnostics["planned_release_id"], 11)
        self.assertEqual(diagnostics["materialized_release_id"], 22)
        self.assertEqual(diagnostics["invariant_status"], "INCONSISTENT")

    def test_repository_helpers_read_binding(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                pr_id = self._insert_planned_release(conn)
                release_id = self._insert_release(conn, meta_id="meta-prm-helpers")
                set_materialized_release_id(conn, planned_release_id=pr_id, materialized_release_id=release_id)

                bound_release = get_bound_release_for_planned_release(conn, planned_release_id=pr_id)
                reverse = find_planned_release_by_materialized_release_id(
                    conn,
                    materialized_release_id=release_id,
                )
                assert bound_release is not None
                assert reverse is not None
                self.assertEqual(int(bound_release["id"]), release_id)
                self.assertEqual(int(reverse["id"]), pr_id)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
