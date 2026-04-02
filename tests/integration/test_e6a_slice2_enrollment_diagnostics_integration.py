from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_operator import TelegramOperatorRegistry
from services.telegram_operator.errors import E6A_OPERATOR_IDENTITY_MISMATCH
from tests._helpers import temp_env


class TestE6ASlice2EnrollmentDiagnosticsIntegration(unittest.TestCase):
    def test_enroll_and_create_private_binding_and_whoami(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-1", telegram_user_id=2001, max_permission_class="STANDARD_OPERATOR_MUTATE")
                binding = svc.create_binding(
                    product_operator_id="operator-1",
                    telegram_user_id=2001,
                    chat_id=-4001,
                    thread_id=None,
                    chat_binding_kind="PRIVATE_CHAT",
                    binding_status="ACTIVE",
                )
                self.assertEqual(str(binding["binding_status"]), "ACTIVE")

                who = svc.whoami(telegram_user_id=2001, chat_id=-4001, thread_id=None)
                self.assertEqual(who["resolved_product_operator_id"], "operator-1")
                self.assertEqual(who["effective_max_allowed_action_class"], "STANDARD_OPERATOR_MUTATE")
                self.assertIsNone(who["error"])
            finally:
                conn.close()

    def test_group_thread_binding_and_visibility(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-2", telegram_user_id=2002)
                svc.create_binding(
                    product_operator_id="operator-2",
                    telegram_user_id=2002,
                    chat_id=-5001,
                    thread_id=777,
                    chat_binding_kind="GROUP_THREAD",
                    binding_status="ACTIVE",
                )
                bindings = svc.list_bindings_for_operator(product_operator_id="operator-2")
                self.assertEqual(len(bindings), 1)
                self.assertEqual(int(bindings[0]["thread_id"]), 777)
            finally:
                conn.close()

    def test_disabled_binding_and_revoked_identity_visibility(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-3", telegram_user_id=2003)
                binding = svc.create_binding(
                    product_operator_id="operator-3",
                    telegram_user_id=2003,
                    chat_id=-6001,
                    thread_id=None,
                    chat_binding_kind="GROUP_CHAT",
                    binding_status="ACTIVE",
                )
                svc.update_binding_status(binding_id=int(binding["id"]), binding_status="DISABLED")
                who_disabled = svc.whoami(telegram_user_id=2003, chat_id=-6001, thread_id=None)
                self.assertEqual(who_disabled["error"]["code"], "E6A_CHAT_BINDING_DISABLED")

                svc.set_identity_access(telegram_user_id=2003, telegram_access_status="REVOKED")
                who_revoked = svc.whoami(telegram_user_id=2003, chat_id=-6001, thread_id=None)
                self.assertEqual(who_revoked["error"]["code"], "E6A_OPERATOR_REVOKED")
            finally:
                conn.close()

    def test_misbinding_visibility_and_enrollment_effective_rule(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-4", telegram_user_id=2004)
                complete_before = svc.complete_enrollment(telegram_user_id=2004)
                self.assertFalse(bool(complete_before["effective"]))

                svc.create_binding(
                    product_operator_id="operator-4",
                    telegram_user_id=2004,
                    chat_id=-7001,
                    thread_id=None,
                    chat_binding_kind="GROUP_CHAT",
                    binding_status="ACTIVE",
                )
                complete_after = svc.complete_enrollment(telegram_user_id=2004)
                self.assertTrue(bool(complete_after["effective"]))

                who_mismatch = svc.whoami(telegram_user_id=2004, chat_id=-9999, thread_id=None)
                self.assertEqual(who_mismatch["error"]["code"], "E6A_CHAT_BINDING_MISSING")
            finally:
                conn.close()

    def test_operator_identity_mismatch_is_blocked(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                svc = TelegramOperatorRegistry(conn)
                svc.start_enrollment(product_operator_id="operator-5", telegram_user_id=2005)
                with self.assertRaisesRegex(Exception, E6A_OPERATOR_IDENTITY_MISMATCH):
                    svc.create_binding(
                        product_operator_id="operator-6",
                        telegram_user_id=2005,
                        chat_id=-8001,
                        thread_id=None,
                        chat_binding_kind="PRIVATE_CHAT",
                        binding_status="ACTIVE",
                    )
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
