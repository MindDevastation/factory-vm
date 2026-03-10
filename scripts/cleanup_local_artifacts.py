#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Remove safe local artifacts generated during QA/test/dev workflows. "
            "Database files are never deleted by this utility."
        )
    )
    parser.add_argument(
        "--qa",
        action="store_true",
        help="Delete local QA artifacts only (qa/, data/qa/, and .qa_* files).",
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Delete local coverage artifacts only (.coverage*, coverage.xml, htmlcov/).",
    )
    parser.add_argument(
        "--exports",
        action="store_true",
        help="Delete local export artifacts only (exports/ and data/exports/).",
    )
    parser.add_argument(
        "--pydeps",
        action="store_true",
        help="Delete local optional Python dependency cache only (data/pydeps/).",
    )
    parser.add_argument(
        "--all-safe",
        action="store_true",
        help="Apply all safe cleanup groups: --qa --coverage --exports --pydeps.",
    )
    return parser.parse_args()


def _safe_delete(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


def _paths_for_cleanup(include_qa: bool, include_coverage: bool, include_exports: bool, include_pydeps: bool) -> list[Path]:
    paths: list[Path] = []
    if include_qa:
        paths.extend([
            REPO_ROOT / "qa",
            REPO_ROOT / "data" / "qa",
        ])
        paths.extend(REPO_ROOT.glob(".qa_*"))
    if include_coverage:
        paths.extend([
            REPO_ROOT / "coverage.xml",
            REPO_ROOT / "htmlcov",
            REPO_ROOT / ".coverage",
        ])
        paths.extend(REPO_ROOT.glob(".coverage.*"))
    if include_exports:
        paths.extend([
            REPO_ROOT / "exports",
            REPO_ROOT / "data" / "exports",
        ])
    if include_pydeps:
        paths.append(REPO_ROOT / "data" / "pydeps")
    return paths


def main() -> int:
    args = _parse_args()

    include_qa = args.qa or args.all_safe
    include_coverage = args.coverage or args.all_safe
    include_exports = args.exports or args.all_safe
    include_pydeps = args.pydeps or args.all_safe

    selected = any([include_qa, include_coverage, include_exports, include_pydeps])
    if not selected:
        print("No cleanup flags selected. Use --help to see available safe cleanup flags.")
        return 0

    removed = 0
    for path in _paths_for_cleanup(include_qa, include_coverage, include_exports, include_pydeps):
        if _safe_delete(path):
            removed += 1
            print(f"removed: {path.relative_to(REPO_ROOT)}")

    print(f"cleanup complete (removed={removed})")
    print("note: DB files are intentionally untouched by this utility")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
