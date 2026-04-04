from __future__ import annotations

import unittest

from services.factory_api.ux_registry import breadcrumb_context, control_center_entry, primary_nav_items, route_ownership_map


class TestE6Mf1S1UxRegistry(unittest.TestCase):
    def test_route_ownership_map_has_control_center_and_family(self) -> None:
        ownership = route_ownership_map()
        self.assertIn("/", ownership)
        self.assertEqual(ownership["/"]["route_key"], "CONTROL_CENTER")
        self.assertEqual(ownership["/"]["owner_group"], "control_center")

    def test_primary_nav_marks_active_route(self) -> None:
        nav = primary_nav_items(current_path="/ui/planner")
        planner = next(item for item in nav if item["key"] == "PLANNER")
        control_center = next(item for item in nav if item["key"] == "CONTROL_CENTER")
        self.assertTrue(planner["active"])
        self.assertFalse(control_center["active"])

    def test_primary_nav_marks_parent_active_for_nested_entity(self) -> None:
        nav = primary_nav_items(current_path="/ui/publish/jobs/123")
        publish_queue = next(item for item in nav if item["key"] == "PUBLISH_QUEUE")
        self.assertTrue(publish_queue["active"])

    def test_breadcrumb_contract_for_nested_publish_job(self) -> None:
        breadcrumb = breadcrumb_context(current_path="/ui/publish/jobs/11")
        self.assertEqual([item["label"] for item in breadcrumb], ["Control Center", "Publish Queue", "Publish Job"])

    def test_breadcrumb_fallback_defaults_to_control_center(self) -> None:
        breadcrumb = breadcrumb_context(current_path="/not-mapped")
        self.assertEqual(breadcrumb, [{"label": "Control Center", "path": "/"}])

    def test_prefix_matching_respects_segment_boundaries(self) -> None:
        nav = primary_nav_items(current_path="/ui/publish/queueing")
        publish_queue = next(item for item in nav if item["key"] == "PUBLISH_QUEUE")
        self.assertFalse(publish_queue["active"])
        breadcrumb = breadcrumb_context(current_path="/ui/publish/queueing")
        self.assertEqual(breadcrumb, [{"label": "Control Center", "path": "/"}])

    def test_control_center_entry_contract(self) -> None:
        entry = control_center_entry()
        self.assertEqual(entry, {"label": "Control Center", "path": "/"})


if __name__ == "__main__":
    unittest.main()
