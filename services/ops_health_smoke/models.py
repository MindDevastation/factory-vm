from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

Severity = Literal["critical", "warning", "info"]
CheckOutcome = Literal["PASS", "WARN", "FAIL", "SKIP"]
OverallStatus = Literal["OK", "WARNING", "FAIL", "RUNNER_ERROR"]


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    title: str
    category: str
    severity: Severity
    result: CheckOutcome
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SmokeSummary:
    total_checks: int
    pass_count: int
    warn_count: int
    fail_count: int
    skip_count: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)
