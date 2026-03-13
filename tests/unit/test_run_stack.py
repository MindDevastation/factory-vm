from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_stack import _resolve_runtime_roles
from services.common.runtime_roles import RuntimeRoleInputs, persist_runtime_role_inputs, resolve_required_runtime_roles, runtime_role_inputs_from_runtime


class TestRunStack(unittest.TestCase):
    def test_worker_roles_includes_track_jobs_by_default(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("IMPORTER_ENABLED", None)
            roles = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=False)
        self.assertIn("track_jobs", roles)

    def test_worker_roles_excludes_importer_when_no_importer_flag(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("IMPORTER_ENABLED", None)
            roles = _resolve_runtime_roles(profile="prod", no_importer_flag=True, with_bot_flag=False)
        self.assertNotIn("importer", roles)

    def test_importer_enabled_env_overrides_default(self) -> None:
        with patch.dict(os.environ, {"IMPORTER_ENABLED": "0"}, clear=True):
            roles = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=False)
        self.assertNotIn("importer", roles)

    def test_bot_enabled_path(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            roles = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=True)
        self.assertIn("bot", roles)

    def test_prod_importer_disabled_alignment_between_launch_and_smoke_resolution(self) -> None:
        with patch.dict(os.environ, {"FACTORY_RUNTIME_NO_IMPORTER": "1", "FACTORY_RUNTIME_WITH_BOT": "0"}, clear=True):
            launched = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=False)
            inputs = runtime_role_inputs_from_runtime(profile="prod")
            resolved = resolve_required_runtime_roles(profile=inputs.profile, no_importer_flag=inputs.no_importer_flag, with_bot_flag=inputs.with_bot_flag)
        self.assertEqual(launched, resolved.required_roles)
        self.assertNotIn("importer", launched)

    def test_prod_bot_enabled_alignment_between_launch_and_smoke_resolution(self) -> None:
        with patch.dict(os.environ, {"FACTORY_RUNTIME_NO_IMPORTER": "0", "FACTORY_RUNTIME_WITH_BOT": "1"}, clear=True):
            launched = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=False)
            inputs = runtime_role_inputs_from_runtime(profile="prod")
            resolved = resolve_required_runtime_roles(profile=inputs.profile, no_importer_flag=inputs.no_importer_flag, with_bot_flag=inputs.with_bot_flag)
        self.assertEqual(launched, resolved.required_roles)
        self.assertIn("bot", launched)

    def test_track_jobs_disabled_alignment_between_launch_and_smoke_resolution(self) -> None:
        with patch.dict(
            os.environ,
            {"FACTORY_RUNTIME_NO_IMPORTER": "0", "FACTORY_RUNTIME_WITH_BOT": "0", "TRACK_CATALOG_ENABLED": "0"},
            clear=True,
        ):
            launched = _resolve_runtime_roles(profile="prod", no_importer_flag=False, with_bot_flag=False)
            inputs = runtime_role_inputs_from_runtime(profile="prod")
            resolved = resolve_required_runtime_roles(profile=inputs.profile, no_importer_flag=inputs.no_importer_flag, with_bot_flag=inputs.with_bot_flag)
        self.assertEqual(launched, resolved.required_roles)
        self.assertNotIn("track_jobs", launched)

    def test_separate_process_smoke_reads_shared_runtime_inputs_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_inputs_file = Path(tmpdir) / "runtime-inputs.json"
            shared_environ = {"FACTORY_RUNTIME_INPUTS_FILE": str(runtime_inputs_file), "TRACK_CATALOG_ENABLED": "1"}
            persist_runtime_role_inputs(
                RuntimeRoleInputs(profile="prod", no_importer_flag=True, with_bot_flag=True),
                environ=shared_environ,
            )
            launched = _resolve_runtime_roles(profile="prod", no_importer_flag=True, with_bot_flag=True)
            inputs = runtime_role_inputs_from_runtime(profile="", environ=shared_environ)
            resolved = resolve_required_runtime_roles(
                profile=inputs.profile,
                no_importer_flag=inputs.no_importer_flag,
                with_bot_flag=inputs.with_bot_flag,
                environ=shared_environ,
            )

        self.assertEqual(inputs, RuntimeRoleInputs(profile="prod", no_importer_flag=True, with_bot_flag=True))
        self.assertEqual(resolved.required_roles, ["orchestrator", "qa", "uploader", "cleanup", "bot", "track_jobs"])
        self.assertEqual(launched, resolved.required_roles)


if __name__ == "__main__":
    unittest.main()
