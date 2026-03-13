from __future__ import annotations

import unittest

from services.common.runtime_roles import resolve_required_runtime_roles, worker_roles_for_runtime


class TestRuntimeRoles(unittest.TestCase):
    def test_prod_required_roles_include_conditionals_when_enabled(self) -> None:
        resolved = resolve_required_runtime_roles(
            profile="prod",
            environ={"IMPORTER_ENABLED": "1", "BOT_ENABLED": "1", "TRACK_CATALOG_ENABLED": "1"},
        )
        self.assertEqual(
            resolved.required_roles,
            ["orchestrator", "qa", "uploader", "cleanup", "importer", "bot", "track_jobs"],
        )
        self.assertEqual(resolved.optional_roles, [])

    def test_prod_optional_roles_when_conditionals_disabled(self) -> None:
        resolved = resolve_required_runtime_roles(
            profile="prod",
            environ={"IMPORTER_ENABLED": "0", "BOT_ENABLED": "0", "TRACK_CATALOG_ENABLED": "0"},
        )
        self.assertEqual(resolved.required_roles, ["orchestrator", "qa", "uploader", "cleanup"])
        self.assertEqual(resolved.optional_roles, ["importer", "bot", "track_jobs"])

    def test_worker_roles_for_runtime_matches_run_stack_source(self) -> None:
        roles = worker_roles_for_runtime(
            no_importer_flag=True,
            with_bot_flag=True,
            environ={"TRACK_CATALOG_ENABLED": "1"},
        )
        self.assertEqual(roles, ["orchestrator", "track_jobs", "qa", "uploader", "cleanup", "bot"])


if __name__ == "__main__":
    unittest.main()
