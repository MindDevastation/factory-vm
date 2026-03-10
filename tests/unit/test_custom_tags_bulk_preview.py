from __future__ import annotations

import json
from pathlib import Path
import unittest


class CustomTagsBulkPreviewHandoffTest(unittest.TestCase):
    def test_handoff_preview_example_has_required_fields(self) -> None:
        doc = Path("qa/FVR-S7_followup_handoff_preview_confirm.md").read_text(encoding="utf-8")
        blocks = [part.strip() for part in doc.split("```json") if "```" in part]
        preview_json = json.loads(blocks[0].split("```", 1)[0].strip())

        self.assertIn("summary", preview_json)
        self.assertIn("can_confirm", preview_json)
        self.assertIn("items", preview_json)
        self.assertIsInstance(preview_json["items"], list)
        self.assertGreaterEqual(len(preview_json["items"]), 1)

        first_item = preview_json["items"][0]
        self.assertIn("normalized", first_item)
        self.assertIn("action", first_item)
        self.assertIn("errors", first_item)
        self.assertIn("warnings", first_item)
