from __future__ import annotations

import unittest

from services.db_viewer.rate_limit import GROUP_POLICY, GROUP_READ, InMemoryRateLimiter, endpoint_group


class TestDbViewerRateLimit(unittest.TestCase):
    def test_boundary_50_allowed_then_51_limited(self):
        now = [100.0]

        def _now() -> float:
            return now[0]

        limiter = InMemoryRateLimiter(now_fn=_now)

        for _ in range(50):
            self.assertFalse(limiter.is_limited("alice", GROUP_READ))

        self.assertTrue(limiter.is_limited("alice", GROUP_READ))

    def test_isolated_by_group_for_same_user(self):
        now = [200.0]

        def _now() -> float:
            return now[0]

        limiter = InMemoryRateLimiter(now_fn=_now)

        for _ in range(50):
            self.assertFalse(limiter.is_limited("alice", GROUP_READ))

        self.assertTrue(limiter.is_limited("alice", GROUP_READ))
        self.assertFalse(limiter.is_limited("alice", GROUP_POLICY))

    def test_endpoint_group_mapping(self):
        self.assertEqual(endpoint_group("/tables"), GROUP_READ)
        self.assertEqual(endpoint_group("/rows"), GROUP_READ)
        self.assertEqual(endpoint_group("/policy"), GROUP_POLICY)
        self.assertEqual(endpoint_group("/policy/update"), GROUP_POLICY)
        self.assertIsNone(endpoint_group("/health"))


if __name__ == "__main__":
    unittest.main()
