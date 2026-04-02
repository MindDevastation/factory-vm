from __future__ import annotations

import unittest

from services.common import db as dbm
from services.telegram_inbox import (
    DELIVERY_BEHAVIORS,
    INBOX_ACTIONABILITY_CLASSES,
    INBOX_CATEGORIES,
    INBOX_LIFECYCLE_STATES,
    INBOX_MESSAGE_FAMILIES,
    INBOX_SEVERITIES,
    ensure_actionability_class,
    ensure_category,
    ensure_delivery_behavior,
    ensure_lifecycle_state,
    ensure_message_family,
    ensure_severity,
)
from tests._helpers import temp_env


class TestE6AMf2Slice1SchemaFoundation(unittest.TestCase):
    def test_migrate_creates_inbox_tables_and_indexes(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                tables = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertTrue(
                    {
                        "telegram_inbox_messages",
                        "telegram_inbox_deliveries",
                        "telegram_inbox_lifecycle_events",
                        "telegram_inbox_acknowledgments",
                    }.issubset(tables)
                )
                indexes = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
                self.assertTrue(
                    {
                        "idx_telegram_inbox_messages_operator_state",
                        "idx_telegram_inbox_messages_route_state",
                        "idx_telegram_inbox_messages_family_severity",
                        "idx_telegram_inbox_deliveries_status",
                        "idx_telegram_inbox_lifecycle_events_message_time",
                        "idx_telegram_inbox_acknowledgments_user_time",
                    }.issubset(indexes)
                )
            finally:
                conn.close()

    def test_literal_contracts(self) -> None:
        self.assertEqual(INBOX_MESSAGE_FAMILIES, ("CRITICAL_ALERT", "ACTIONABLE_ALERT", "SUMMARY_DIGEST", "UNRESOLVED_FOLLOW_UP", "RESOLUTION_UPDATE"))
        self.assertEqual(INBOX_LIFECYCLE_STATES, ("ACTIVE", "SUPERSEDED", "RESOLVED", "EXPIRED", "INFO_ONLY"))
        self.assertEqual(INBOX_SEVERITIES, ("CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"))
        self.assertEqual(INBOX_CATEGORIES, ("PUBLISH", "READINESS", "RECOVERY", "HEALTH", "FOLLOW_UP", "DIGEST", "SYSTEM"))
        self.assertEqual(INBOX_ACTIONABILITY_CLASSES, ("INFO_ONLY", "ACTIONABLE", "ACK_REQUIRED", "ESCALATE_ONLY"))
        self.assertEqual(DELIVERY_BEHAVIORS, ("IMMEDIATE", "DIGEST", "FOLLOW_UP_ONLY", "SUPPRESSED"))

        self.assertEqual(ensure_message_family("critical_alert"), "CRITICAL_ALERT")
        self.assertEqual(ensure_lifecycle_state("resolved"), "RESOLVED")
        self.assertEqual(ensure_severity("info"), "INFO")
        self.assertEqual(ensure_category("health"), "HEALTH")
        self.assertEqual(ensure_actionability_class("ack_required"), "ACK_REQUIRED")
        self.assertEqual(ensure_delivery_behavior("digest"), "DIGEST")

    def test_smoke_no_domain_mutation_introduced(self) -> None:
        with temp_env() as (_td, env):
            conn = dbm.connect(env)
            try:
                dbm.migrate(conn)
                fields = {str(r["name"]) for r in conn.execute("PRAGMA table_info(telegram_inbox_messages)").fetchall()}
                self.assertFalse({"publish_action", "ops_mutation"}.intersection(fields))
                mf1_tables = {str(r["name"]) for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                self.assertIn("telegram_operator_identities", mf1_tables)
                self.assertIn("telegram_chat_bindings", mf1_tables)
                self.assertIn("telegram_action_gateway_events", mf1_tables)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
