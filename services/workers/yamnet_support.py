from __future__ import annotations

from services.common.pydeps import ensure_py_deps_on_sys_path


class YamnetDepsUnavailableError(RuntimeError):
    pass


def assert_yamnet_available(env) -> str:
    target_dir = ensure_py_deps_on_sys_path(env)
    try:
        import tensorflow  # noqa: F401
        import tensorflow_hub  # noqa: F401
    except Exception as exc:
        raise YamnetDepsUnavailableError(
            "YAMNET_NOT_INSTALLED: install via UI button and retry; "
            f"target_dir={target_dir}; "
            f"manual_fix=python -m pip install -r requirements-yamnet.txt --upgrade --target {target_dir}"
        ) from exc
    return target_dir
