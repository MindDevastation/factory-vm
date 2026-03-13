from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.common.runtime_roles import (
    RuntimeRoleInputs,
    launched_worker_roles_for_runtime,
    persist_runtime_role_inputs,
    resolve_required_runtime_roles,
    runtime_role_inputs_from_runtime,
    worker_roles_for_runtime,
)


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

    def test_runtime_inputs_env_override_flags(self) -> None:
        inputs = runtime_role_inputs_from_runtime(
            profile="prod",
            no_importer_flag=False,
            with_bot_flag=False,
            environ={"FACTORY_RUNTIME_NO_IMPORTER": "1", "FACTORY_RUNTIME_WITH_BOT": "1"},
        )
        self.assertTrue(inputs.no_importer_flag)
        self.assertTrue(inputs.with_bot_flag)

    def test_runtime_inputs_reads_persisted_shared_source_without_process_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_inputs_file = Path(tmpdir) / "runtime-inputs.json"
            persist_runtime_role_inputs(
                RuntimeRoleInputs(profile="prod", no_importer_flag=True, with_bot_flag=True),
                environ={"FACTORY_RUNTIME_INPUTS_FILE": str(runtime_inputs_file)},
            )
            inputs = runtime_role_inputs_from_runtime(
                profile="",
                no_importer_flag=False,
                with_bot_flag=False,
                environ={"FACTORY_RUNTIME_INPUTS_FILE": str(runtime_inputs_file)},
            )

        self.assertEqual(inputs.profile, "prod")
        self.assertTrue(inputs.no_importer_flag)
        self.assertTrue(inputs.with_bot_flag)

    def test_track_jobs_disabled_removes_required_track_jobs(self) -> None:
        resolved = resolve_required_runtime_roles(
            profile="prod",
            no_importer_flag=False,
            with_bot_flag=False,
            environ={"TRACK_CATALOG_ENABLED": "0", "IMPORTER_ENABLED": "1", "BOT_ENABLED": "0"},
        )
        self.assertNotIn("track_jobs", resolved.required_roles)
        self.assertIn("track_jobs", resolved.optional_roles)

    def test_launched_roles_for_prod_excludes_optional_disabled_roles(self) -> None:
        roles = launched_worker_roles_for_runtime(
            profile="prod",
            no_importer_flag=True,
            with_bot_flag=False,
            environ={"TRACK_CATALOG_ENABLED": "0"},
        )
        self.assertEqual(roles, ["orchestrator", "qa", "uploader", "cleanup"])


if __name__ == "__main__":
    unittest.main()
