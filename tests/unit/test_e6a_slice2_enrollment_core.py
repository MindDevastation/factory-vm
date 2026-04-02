from __future__ import annotations

import sqlite3
import unittest

from services.common import db as dbm
from services.telegram_operator import (
    TelegramOperatorRegistry,
    build_binding_fixture,
    build_identity_fixture,
    ensure_binding_status,
    ensure_chat_binding_kind,
    ensure_telegram_access_status,
    normalize_binding_context,
)
from tests._helpers import temp_env


class TestE6ASlice2EnrollmentCoreUnit(unittest.TestCase):
    def test_status_kind_validations(self) -> None:
        self.assertEqual(ensure_telegram_access_status("active"), "ACTIVE")
        self.assertEqual(ensure_binding_status("pending"), "PENDING")
        self.assertEqual(ensure_chat_binding_kind("group_thread"), "GROUP_THREAD")

    def test_binding_context_normalizer_thread_semantics(self) -> None:
        private_ctx = normalize_binding_context(telegram_user_id=1001, chat_id=-1, thread_id=None, chat_binding_kind="PRIVATE_CHAT")
        self.assertEqual(private_ctx["thread_id"], None)

        group_ctx = normalize_binding_context(telegram_user_id=1001, chat_id=-10, thread_id=55, chat_binding_kind="GROUP_CHAT")
        self.assertEqual(group_ctx["thread_id"], None)

        thread_ctx = normalize_binding_context(telegram_user_id=1001, chat_id=-10, thread_id=77, chat_binding_kind="GROUP_THREAD")
        self.assertEqual(thread_ctx["thread_id"], 77)

        with self.assertRaises(ValueError):
            normalize_binding_context(telegram_user_id=1001, chat_id=-10, thread_id=None, chat_binding_kind="GROUP_THREAD")

    def test_identity_uniqueness_validation(self) -> None:
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
                    ("operator-1", 1001, "ACTIVE", "READ_ONLY", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                )
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO telegram_operator_identities(
                            product_operator_id, telegram_user_id, telegram_access_status,
                            max_permission_class, enrolled_at, created_at, updated_at
                        ) VALUES(?,?,?,?,?,?,?)
                        """,
                        ("operator-2", 1001, "ACTIVE", "READ_ONLY", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
                    )
            finally:
                conn.close()

    def test_fixture_factory_outputs(self) -> None:
        identity = build_identity_fixture()
        binding = build_binding_fixture(chat_binding_kind="GROUP_THREAD", thread_id=1)
        self.assertEqual(identity["telegram_access_status"], "ACTIVE")
        self.assertEqual(binding["chat_binding_kind"], "GROUP_THREAD")

    def test_enrollment_not_effective_without_active_binding(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-1", telegram_user_id=1001)
                complete = svc.complete_enrollment(telegram_user_id=1001)
                self.assertFalse(bool(complete["effective"]))
                self.assertEqual(complete["enrollment_state"], "PENDING_BINDING")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
