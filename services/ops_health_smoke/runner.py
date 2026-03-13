from __future__ import annotations

import socket
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from .models import CheckResult, OverallStatus, SmokeSummary


class SmokeCheck(Protocol):
    check_id: str
    title: str
    category: str
    severity: str

    def run(self, context: "SmokeContext") -> CheckResult:
        ...


@dataclass(frozen=True)
class SmokeContext:
    profile: str


class RunnerBootstrapCheck:
    check_id = "runner_bootstrap"
    title = "Smoke runner bootstrap"
    category = "framework"
    severity = "info"

    def run(self, context: SmokeContext) -> CheckResult:
        return CheckResult(
            check_id=self.check_id,
            title=self.title,
            category=self.category,
            severity=self.severity,
            result="PASS",
            message=f"Smoke runner initialized for profile '{context.profile}'",
            details={"profile": context.profile},
        )


def default_checks() -> list[SmokeCheck]:
    return [RunnerBootstrapCheck()]


def _compute_summary(results: list[CheckResult]) -> SmokeSummary:
    return SmokeSummary(
        total_checks=len(results),
        pass_count=sum(1 for r in results if r.result == "PASS"),
        warn_count=sum(1 for r in results if r.result == "WARN"),
        fail_count=sum(1 for r in results if r.result == "FAIL"),
        skip_count=sum(1 for r in results if r.result == "SKIP"),
    )


def _compute_overall(summary: SmokeSummary) -> tuple[OverallStatus, int]:
    if summary.fail_count > 0:
        return ("FAIL", 2)
    if summary.warn_count > 0:
        return ("WARNING", 1)
    return ("OK", 0)


def run_production_smoke(*, profile: str, selected_check_ids: set[str] | None = None) -> dict[str, Any]:
    started = time.monotonic()
    context = SmokeContext(profile=profile)

    checks = default_checks()
    if selected_check_ids:
        checks = [c for c in checks if c.check_id in selected_check_ids]
        if not checks:
            raise ValueError("No checks matched --checks filter")

    results = [check.run(context) for check in checks]
    summary = _compute_summary(results)
    overall, exit_code = _compute_overall(summary)

    return {
        "schema_version": "factory_production_smoke/1",
        "generated_at": datetime.now(UTC).isoformat(),
        "hostname": socket.gethostname(),
        "profile": profile,
        "overall_status": overall,
        "exit_code": exit_code,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "summary": summary.to_dict(),
        "checks": [r.to_dict() for r in results],
    }


def run_checks_with_error_capture(*, profile: str, selected_check_ids: set[str] | None = None) -> dict[str, Any]:
    try:
        return run_production_smoke(profile=profile, selected_check_ids=selected_check_ids)
    except Exception as exc:
        return {
            "schema_version": "factory_production_smoke/1",
            "generated_at": datetime.now(UTC).isoformat(),
            "hostname": socket.gethostname(),
            "profile": profile,
            "overall_status": "RUNNER_ERROR",
            "exit_code": 3,
            "duration_ms": 0,
            "summary": {
                "total_checks": 0,
                "pass_count": 0,
                "warn_count": 0,
                "fail_count": 0,
                "skip_count": 0,
            },
            "checks": [],
            "error": str(exc),
        }
