from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.telegram_operator import (
    ACTION_TRANSPORT_TYPES,
    BINDING_STATUSES,
    CHAT_BINDING_KINDS,
    GATEWAY_RESULTS,
    PERMISSION_ACCESS_CLASSES,
    TELEGRAM_ACCESS_STATUSES,
    ensure_action_transport_type,
    ensure_binding_status,
    ensure_chat_binding_kind,
    ensure_gateway_result,
    ensure_permission_access_class,
    ensure_telegram_access_status,
)
from tests._helpers import temp_env


class TestE6ASlice1SchemaFoundation(unittest.TestCase):
    def test_migrate_creates_telegram_operator_layer_tables_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                table_names = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                }
                self.assertTrue(
                    {
                        "telegram_operator_identities",
                        "telegram_chat_bindings",
                        "telegram_action_gateway_events",
                    }.issubset(table_names)
                )

                index_names = {
                    str(r["name"])
                    for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
                }
                self.assertTrue(
                    {
                        "idx_telegram_operator_identities_operator",
                        "idx_telegram_chat_bindings_operator_status",
                        "idx_telegram_chat_bindings_chat_status",
                        "idx_telegram_action_gateway_events_operator_time",
                        "idx_telegram_action_gateway_events_transport",
                        "idx_telegram_action_gateway_events_correlation",
                    }.issubset(index_names)
                )
            finally:
                conn.close()

    def test_identity_and_binding_uniqueness_constraints(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                conn.execute(
                    """
                    INSERT INTO telegram_operator_identities(
                        product_operator_id, telegram_user_id, telegram_access_status,
                        max_permission_class, enrolled_at, created_at, updated_at
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    ("op-1", 1001, "ACTIVE", "READ_ONLY", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO telegram_operator_identities(
                            product_operator_id, telegram_user_id, telegram_access_status,
                            max_permission_class, enrolled_at, created_at, updated_at
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        ("op-2", 1001, "ACTIVE", "STANDARD_OPERATOR_MUTATE", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                    )

                conn.execute(
                    """
                    INSERT INTO telegram_chat_bindings(
                        product_operator_id, telegram_user_id, chat_id, thread_id,
                        chat_binding_kind, binding_status, created_at
                    ) VALUES(?,?,?,?,?,?,?)
                    """,
                    ("op-1", 1001, -2001, None, "GROUP_CHAT", "PENDING", "2026-01-01T00:00:00Z"),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO telegram_chat_bindings(
                            product_operator_id, telegram_user_id, chat_id, thread_id,
                            chat_binding_kind, binding_status, created_at
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        ("op-1", 1001, -2001, None, "GROUP_CHAT", "ACTIVE", "2026-01-01T00:00:01Z"),
                    )
            finally:
                conn.close()

    def test_frozen_literal_helpers_validate_allowed_values(self) -> None:
        self.assertEqual(ensure_telegram_access_status("active"), "ACTIVE")
        self.assertEqual(ensure_chat_binding_kind("private_chat"), "PRIVATE_CHAT")
        self.assertEqual(ensure_binding_status("disabled"), "DISABLED")
        self.assertEqual(ensure_action_transport_type("callback"), "CALLBACK")
        self.assertEqual(ensure_permission_access_class("read_only"), "READ_ONLY")
        self.assertEqual(ensure_gateway_result("stale"), "STALE")

        with self.assertRaises(ValueError):
            ensure_telegram_access_status("BROKEN")
        with self.assertRaises(ValueError):
            ensure_chat_binding_kind("DM")

    def test_literal_sets_are_frozen_contracts(self) -> None:
        self.assertEqual(TELEGRAM_ACCESS_STATUSES, ("ACTIVE", "INACTIVE", "REVOKED"))
        self.assertEqual(CHAT_BINDING_KINDS, ("PRIVATE_CHAT", "GROUP_CHAT", "GROUP_THREAD"))
        self.assertEqual(BINDING_STATUSES, ("PENDING", "ACTIVE", "DISABLED", "REVOKED"))
        self.assertEqual(ACTION_TRANSPORT_TYPES, ("COMMAND", "CALLBACK"))
        self.assertEqual(
            PERMISSION_ACCESS_CLASSES,
            ("READ_ONLY", "STANDARD_OPERATOR_MUTATE", "GUARDED_OPERATOR_MUTATE", "PRIVILEGED_OPERATOR_MUTATE"),
        )
        self.assertEqual(GATEWAY_RESULTS, ("ALLOWED", "DENIED", "STALE", "EXPIRED", "INVALID"))

    def test_slice1_does_not_introduce_domain_publish_tables(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                rows = conn.execute(
                    "SELECT DISTINCT action_type FROM telegram_action_gateway_events WHERE action_type LIKE 'PUBLISH_%'"
                ).fetchall()
                self.assertEqual(rows, [])
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
