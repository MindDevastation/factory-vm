from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

_PERSISTENT_PYDEPS_DIR: str | None = None


def make_persistent_pydeps_dir(prefix: str = "pydeps_test_") -> str:
    """Return a stable temp dir that survives until manually cleaned.

    coverage run/report are separate commands in CI, so this directory must
    remain after test process exit for coverage to read imported module source.
    """
    global _PERSISTENT_PYDEPS_DIR
    if _PERSISTENT_PYDEPS_DIR is None:
        root = Path(tempfile.gettempdir()) / f"{prefix}factory_vm"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir(parents=True, exist_ok=True)
        _PERSISTENT_PYDEPS_DIR = str(root)
    return _PERSISTENT_PYDEPS_DIR


def write_dummy_tf_modules(pydeps_dir: str) -> None:
    """Create lightweight dummy tensorflow modules for import checks in tests."""
    root = Path(pydeps_dir)
    (root / "tensorflow").mkdir(parents=True, exist_ok=True)
    (root / "tensorflow_hub").mkdir(parents=True, exist_ok=True)
    (root / "tensorflow" / "__init__.py").write_text('__version__ = "0.0-test"\n', encoding="utf-8")
    (root / "tensorflow_hub" / "__init__.py").write_text("", encoding="utf-8")
