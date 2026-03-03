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
        pkg_resources_hint = ""
        if isinstance(exc, ModuleNotFoundError) and "pkg_resources" in str(exc):
            pkg_resources_hint = (
                " pkg_resources missing from setuptools: pin setuptools<71 "
                "(now included in requirements-yamnet.txt)."
            )
        raise YamnetDepsUnavailableError(
            "YAMNET_NOT_INSTALLED: install via UI button and retry; "
            f"target_dir={resolved_target}; "
            f"cause={exc.__class__.__name__}: {exc};"
            f"{pkg_resources_hint} "
            f"python_executable={sys.executable}; "
            f"python_version={sys.version}; "
            "install_note=requirements-yamnet.txt includes tensorflow-io for TF 2.16.1 resampling; "
            "manual_fix=python -m pip install -r requirements-yamnet.txt --upgrade --target "
            f"{resolved_target}; then rerun Install Yamnet"
        ) from exc
    return target_dir
