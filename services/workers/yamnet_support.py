from __future__ import annotations

import sys
from pathlib import Path

from services.common.pydeps import ensure_py_deps_on_sys_path


class YamnetDepsUnavailableError(RuntimeError):
    pass


def assert_yamnet_available(env) -> str:
    target_dir = ensure_py_deps_on_sys_path(env)
    try:
        import tensorflow  # noqa: F401
        import tensorflow_hub  # noqa: F401
    except Exception as exc:
        resolved_target = str(Path(target_dir).resolve())
        raise YamnetDepsUnavailableError(
            "YAMNET_NOT_INSTALLED: install via UI button and retry; "
            f"target_dir={resolved_target}; "
            f"cause={exc.__class__.__name__}: {exc}; "
            f"python_executable={sys.executable}; "
            f"python_version={sys.version}; "
            "manual_fix=pip install 'setuptools<71' --upgrade && "
            f"python -m pip install -r requirements-yamnet.txt --upgrade --target {resolved_target}; "
            "then rerun Install Yamnet"
        ) from exc
    return target_dir
