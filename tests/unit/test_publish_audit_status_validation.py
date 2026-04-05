from __future__ import annotations

import unittest

from services.factory_api.publish_audit_status import ALLOWED_AUDIT_STATUSES, validate_audit_status


class TestPublishAuditStatusValidation(unittest.TestCase):
    def test_allowed_status_values(self) -> None:
        for status in ALLOWED_AUDIT_STATUSES:
            self.assertEqual(validate_audit_status(status), status)

    def test_forbidden_status_values(self) -> None:
        for bad in ("", "APPROVED", "blocked", "needs-review", "manual", "approved "):
            with self.assertRaises(ValueError):
                validate_audit_status(bad)


if __name__ == "__main__":
    unittest.main()
