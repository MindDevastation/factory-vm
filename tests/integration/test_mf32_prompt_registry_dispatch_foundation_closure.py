from __future__ import annotations

from pathlib import Path
import unittest


class TestMf32PromptRegistryDispatchFoundationClosure(unittest.TestCase):
    def test_closure_doc_exists_and_contains_required_references(self) -> None:
        doc = Path("docs/reviews/prompt_registry_dispatch_foundation_closure.md")
        self.assertTrue(doc.exists(), "Closure handoff doc must exist")

        text = doc.read_text(encoding="utf-8")

        required_snippets = [
            "Prompt Registry Dispatch Foundation Closure",
            "real execution runtime is not implemented",
            "no actual action execution",
            "no audit write",
            "no queue/jobs/workers",
            "no external calls",
            "Next phase",
            "Review checklist",
            "MF31 contract snapshots",
            "read-only dispatch execution foundation",
        ]

        for snippet in required_snippets:
            self.assertIn(snippet, text)


if __name__ == "__main__":
    unittest.main()
