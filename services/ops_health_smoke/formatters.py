from __future__ import annotations

from typing import Any


def render_human_report(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"Overall status: {report['overall_status']}")
    lines.append(f"Profile: {report['profile']}")
    summary = report["summary"]
    lines.append(
        "Summary: total={total_checks} pass={pass_count} warn={warn_count} fail={fail_count} skip={skip_count}".format(**summary)
    )

    grouped = {"PASS": [], "WARN": [], "FAIL": [], "SKIP": []}
    for check in report["checks"]:
        grouped.setdefault(check["result"], []).append(check)

    for result in ["PASS", "WARN", "FAIL", "SKIP"]:
        for check in grouped[result]:
            lines.append(f"[{result}] {check['check_id']} - {check['message']}")

    if report["overall_status"] == "OK":
        lines.append("Operator hint: System ready")
    elif report["overall_status"] == "WARNING":
        lines.append("Operator hint: Warnings require attention")
    else:
        lines.append("Operator hint: Not safe for production run")

    return "\n".join(lines)
