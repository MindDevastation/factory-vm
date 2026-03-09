#!/usr/bin/env python3
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STDLIB_MODULES = set(sys.stdlib_module_names)

REQUIREMENT_FILES = [
    ROOT / "requirements.txt",
    ROOT / "requirements-dev.txt",
    ROOT / "requirements-yamnet.txt",
    ROOT / "requirements-whisper.txt",
    ROOT / "requirements-all.txt",
]

IMPORT_TO_PACKAGE = {
    "dotenv": "python-dotenv",
    "yaml": "PyYAML",
    "google": "google-auth",
    "googleapiclient": "google-api-python-client",
    "google_auth_oauthlib": "google-auth-oauthlib",
    "faster_whisper": "faster-whisper",
    "tensorflow": "tensorflow-cpu",
    "tensorflow_hub": "tensorflow-hub",
}

EXCLUDE_DIRS = {".git", ".venv", "venv", "__pycache__"}
EXCLUDE_PATH_PREFIXES = ("build/", "dist/")


def normalize_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def iter_requirement_names(path: Path, seen: set[Path] | None = None) -> set[str]:
    seen = seen or set()
    if path in seen or not path.exists():
        return set()
    seen.add(path)

    names: set[str] = set()
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if line.startswith("-r "):
            include = (path.parent / line.split(maxsplit=1)[1]).resolve()
            names |= iter_requirement_names(include, seen)
            continue
        if line.startswith("-"):
            continue
        pkg = re.split(r"[<>=!~;\[]", line, maxsplit=1)[0].strip()
        if pkg:
            names.add(normalize_name(pkg))
    return names


def collect_local_modules() -> set[str]:
    modules = {p.stem for p in ROOT.glob("*.py")}
    for child in ROOT.iterdir():
        if child.is_dir() and child.name not in EXCLUDE_DIRS and not child.name.startswith('.'):
            if any(child.glob("*.py")) or any(child.rglob("__init__.py")):
                modules.add(child.name)
    for init in ROOT.rglob("__init__.py"):
        rel = init.parent.relative_to(ROOT)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        modules.add(rel.parts[0])
    return modules


def iter_python_files() -> list[Path]:
    files: list[Path] = []
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT)
        if any(part in EXCLUDE_DIRS for part in rel.parts):
            continue
        if rel.as_posix().startswith(EXCLUDE_PATH_PREFIXES):
            continue
        files.append(path)
    return files


def collect_direct_imports(local_modules: set[str]) -> set[str]:
    imports: set[str] = set()
    for path in iter_python_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name.split(".", 1)[0]
                    if module and module not in local_modules and module not in STDLIB_MODULES and module != "__future__":
                        imports.add(module)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                module = node.module.split(".", 1)[0]
                if module and module not in local_modules and module not in STDLIB_MODULES and module != "__future__":
                    imports.add(module)
    return imports


def main() -> int:
    missing_files = [str(p.relative_to(ROOT)) for p in REQUIREMENT_FILES if not p.exists()]
    if missing_files:
        print("Missing requirement files:")
        for name in missing_files:
            print(f" - {name}")
        return 1

    declared = set()
    for req_file in REQUIREMENT_FILES:
        declared |= iter_requirement_names(req_file.resolve())

    imports = collect_direct_imports(collect_local_modules())

    unresolved = []
    for imported in sorted(imports):
        mapped = IMPORT_TO_PACKAGE.get(imported, imported)
        if normalize_name(mapped) not in declared:
            unresolved.append((imported, mapped))

    if unresolved:
        print("Uncovered direct imports (import -> expected package):")
        for imp, pkg in unresolved:
            print(f" - {imp} -> {pkg}")
        return 1

    print("Dependency coverage check passed.")
    print(f"Direct imports checked: {len(imports)}")
    print(f"Declared packages found: {len(declared)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
