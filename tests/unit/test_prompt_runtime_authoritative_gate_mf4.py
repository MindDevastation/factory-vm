from __future__ import annotations

import sqlite3
import unittest

from services.prompt_registry.authoritative_gate import (
    TargetCompatibilityService,
    TargetResolverRegistryService,
    TargetSnapshotResolverRegistry,
    TargetSnapshotService,
    canonical_snapshot_json,
    compute_snapshot_hash,
    validate_snapshot_envelope,
)
from tests._helpers import seed_minimal_db, temp_env


def snapshot_payload(*, target_ref: str = "target-1", state: str = "ready") -> dict:
    return {
        "target_type": "channel",
        "target_ref": target_ref,
        "target_display_label": f"Channel {target_ref}",
        "target_state_code": state,
        "target_exists": True,
        "target_updated_at": "2026-01-01T00:00:00Z",
        "compatibility_inputs": {"kind": "LONG"},
        "resolver_metadata": {"resolver": "fake"},
    }


def register_authorities(conn: sqlite3.Connection, *, compatibility_status: str = "allowed", resolver_code: str = "fake_resolver") -> None:
    TargetResolverRegistryService(conn).upsert(
        "CREATE_BULK_JSON_DRAFT",
        "channel",
        {"resolver_code": resolver_code, "snapshot_schema_version": "v1", "is_enabled": True},
        updated_by_operator="admin",
    )
    TargetCompatibilityService(conn).upsert(
        "CREATE_BULK_JSON_DRAFT",
        "channel",
        {"compatibility_status": compatibility_status, "policy_code": f"policy_{compatibility_status}"},
        updated_by_operator="admin",
    )


class TestPromptRuntimeAuthoritativeGateMf4(unittest.TestCase):
    def test_schema_table_columns_and_index_exist(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            try:
                tables = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("prompt_runtime_target_snapshot_ledger", tables)
                columns = {r["name"] for r in conn.execute("PRAGMA table_info(prompt_runtime_target_snapshot_ledger)").fetchall()}
                self.assertTrue(
                    {
                        "id",
                        "capability_code",
                        "target_type",
                        "target_ref",
                        "resolver_code",
                        "snapshot_schema_version",
                        "snapshot_payload_json",
                        "snapshot_hash",
                        "compatibility_status_at_capture",
                        "resolved_at",
                        "created_at",
                    }.issubset(columns)
                )
                indexes = {r["name"] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertIn("idx_prompt_runtime_target_snapshot_ledger_lookup", indexes)
            finally:
                conn.close()

    def test_canonical_serialization_and_hash_are_deterministic(self) -> None:
        payload_a = snapshot_payload()
        payload_b = {
            "resolver_metadata": {"resolver": "fake"},
            "compatibility_inputs": {"kind": "LONG"},
            "target_updated_at": "2026-01-01T00:00:00Z",
            "target_exists": True,
            "target_state_code": "ready",
            "target_display_label": "Channel target-1",
            "target_ref": "target-1",
            "target_type": "channel",
        }
        self.assertEqual(canonical_snapshot_json(payload_a), canonical_snapshot_json(payload_b))
        self.assertEqual(compute_snapshot_hash(payload_a), compute_snapshot_hash(payload_b))
        changed = snapshot_payload(state="changed")
        self.assertNotEqual(compute_snapshot_hash(payload_a), compute_snapshot_hash(changed))

    def test_missing_required_or_unserializable_snapshot_fails_closed(self) -> None:
        missing = snapshot_payload()
        missing.pop("target_display_label")
        with self.assertRaises(ValueError):
            validate_snapshot_envelope(missing)
        unserializable = snapshot_payload()
        unserializable["resolver_metadata"] = {"bad": {1, 2}}
        with self.assertRaises(ValueError):
            canonical_snapshot_json(unserializable)

    def test_resolver_and_compatibility_fail_closed_paths(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            registry = TargetSnapshotResolverRegistry()
            registry.register("fake_resolver", lambda **_: snapshot_payload())
            try:
                service = TargetSnapshotService(conn, registry)
                missing_resolver = service.resolve_preview(capability_code="CREATE_BULK_JSON_DRAFT", target_type="channel", target_ref="target-1")
                self.assertEqual(missing_resolver.failure_reason_code, "missing_target_resolver_authority")

                register_authorities(conn, resolver_code="unregistered_resolver")
                missing_impl = service.resolve_preview(capability_code="CREATE_BULK_JSON_DRAFT", target_type="channel", target_ref="target-1")
                self.assertEqual(missing_impl.failure_reason_code, "target_resolver_implementation_missing")

                for status in ("blocked", "deprecated"):
                    conn.execute("DELETE FROM prompt_runtime_target_resolver_registry")
                    conn.execute("DELETE FROM prompt_runtime_target_compatibility_policy")
                    register_authorities(conn, compatibility_status=status, resolver_code="fake_resolver")
                    blocked = service.resolve_preview(capability_code="CREATE_BULK_JSON_DRAFT", target_type="channel", target_ref="target-1")
                    self.assertEqual(blocked.failure_reason_code, f"target_compatibility_{status}")

                conn.execute("DELETE FROM prompt_runtime_target_compatibility_policy")
                missing_compat = service.resolve_preview(capability_code="CREATE_BULK_JSON_DRAFT", target_type="channel", target_ref="target-1")
                self.assertEqual(missing_compat.failure_reason_code, "missing_target_compatibility_authority")
            finally:
                conn.close()

    def test_allowed_compatibility_and_registered_resolver_persist_snapshot_and_hash(self) -> None:
        with temp_env() as (_td, env):
            seed_minimal_db(env)
            conn = sqlite3.connect(env.db_path)
            conn.row_factory = sqlite3.Row
            registry = TargetSnapshotResolverRegistry()
            registry.register("fake_resolver", lambda **kwargs: snapshot_payload(target_ref=kwargs["target_ref"]))
            try:
                register_authorities(conn)
                service = TargetSnapshotService(conn, registry)
                result = service.resolve_preview(capability_code="CREATE_BULK_JSON_DRAFT", target_type="channel", target_ref="target-1")
                self.assertEqual(result.admission_status, "admissible")
                self.assertIsNotNone(result.ledger_id)
                self.assertEqual(result.snapshot_hash, compute_snapshot_hash(result.snapshot_payload))
                row = conn.execute("SELECT * FROM prompt_runtime_target_snapshot_ledger WHERE id=?", (result.ledger_id,)).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["snapshot_hash"], result.snapshot_hash)
                self.assertEqual(row["compatibility_status_at_capture"], "allowed")
                self.assertEqual(TargetSnapshotService(conn, registry).resolve_preview(capability_code="*", target_type="channel", target_ref="target-1").failure_reason_code, "missing_target_resolver_authority")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
