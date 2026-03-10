from __future__ import annotations

import json
from pathlib import Path
import unittest


class CustomTagsBulkConfirmHandoffTest(unittest.TestCase):
    def test_handoff_confirm_example_has_result_counters(self) -> None:
        doc = Path("qa/FVR-S7_followup_handoff_preview_confirm.md").read_text(encoding="utf-8")
        blocks = [part.strip() for part in doc.split("```json") if "```" in part]
        confirm_json = json.loads(blocks[1].split("```", 1)[0].strip())

        self.assertIn("summary", confirm_json)
        self.assertIn("inserted", confirm_json)
        self.assertIn("updated", confirm_json)
        self.assertIn("unchanged", confirm_json)
