from __future__ import annotations

import unittest

from services.factory_api.planner_common import planner_error


class TestPlannerCommon(unittest.TestCase):
    def test_planner_error_without_details(self):
        resp = planner_error("PLANNER_BAD_REQUEST", "bad request", status_code=400)

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.body, b'{"error":{"code":"PLANNER_BAD_REQUEST","message":"bad request"}}')

    def test_planner_error_with_details_and_request_id(self):
        resp = planner_error(
            "PLANNER_CONFLICT",
            "conflict",
            details={"field": "title"},
            status_code=409,
            request_id="req-123",
        )

        self.assertEqual(resp.status_code, 409)
        self.assertEqual(
            resp.body,
            b'{"error":{"code":"PLANNER_CONFLICT","message":"conflict","request_id":"req-123","details":{"field":"title"}}}',
        )


if __name__ == "__main__":
    unittest.main()
