from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from scripts.run_stack import _worker_roles


class TestRunStack(unittest.TestCase):
    def test_worker_roles_excludes_importer_when_no_importer_flag(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("IMPORTER_ENABLED", None)
            roles = _worker_roles(no_importer_flag=True)
        self.assertNotIn("importer", roles)

    def test_importer_enabled_env_overrides_default(self) -> None:
        with patch.dict(os.environ, {"IMPORTER_ENABLED": "0"}, clear=False):
            roles = _worker_roles(no_importer_flag=False)
        self.assertNotIn("importer", roles)


if __name__ == "__main__":
    unittest.main()
