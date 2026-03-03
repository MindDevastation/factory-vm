from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Mapping


def _read_env(name: str, env: Mapping[str, str] | object | None = None) -> str:
    if env is None:
        return os.environ.get(name, "")
    if isinstance(env, Mapping):
        value = env.get(name)
    else:
        attr_map = {
            "FACTORY_DB_PATH": "db_path",
        }
        value = getattr(env, attr_map.get(name, name.lower()), "")
    if value:
        return str(value)
    return os.environ.get(name, "")


def get_py_deps_dir(env: Mapping[str, str] | object | None = None) -> str:
    explicit = _read_env("FACTORY_PY_DEPS_DIR", env).strip()
    if explicit:
        return str(Path(explicit).expanduser())

    db_path = Path(_read_env("FACTORY_DB_PATH", env).strip() or "data/factory.sqlite3")
    if db_path.name == "factory.sqlite3" and db_path.parent.name == "data":
        project_data_dir = db_path.parent
    else:
        project_data_dir = Path("data")
    return str(project_data_dir / "pydeps")


def ensure_py_deps_on_sys_path(env: Mapping[str, str] | object | None = None) -> str:
    py_deps_dir = Path(get_py_deps_dir(env)).expanduser()
    py_deps_dir.mkdir(parents=True, exist_ok=True)
    target = str(py_deps_dir)
    if target not in sys.path:
        sys.path.insert(0, target)
    return target
