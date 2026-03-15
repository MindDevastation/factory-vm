from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from services.common.profile import load_profile_env
from services.ops_health_smoke import render_human_report, run_checks_with_error_capture


SCENARIO_PLAYBOOKS: dict[str, str] = {
    "post-deploy": "docs/ops/runbook/post_deploy_verification.md",
    "post-reboot": "docs/ops/runbook/post_reboot_verification.md",
    "post-restore": "docs/ops/runbook/post_restore_verification.md",
    "pre-batch-run": "docs/ops/runbook/sop/before_batch_run.md",
}


def _parse_checks(raw_checks: str) -> set[str] | None:
    selected = {item.strip() for item in raw_checks.split(",") if item.strip()}
    return selected or None


def _render_operational_verdict(exit_code: int, scenario: str) -> str:
    scenario_doc = SCENARIO_PLAYBOOKS[scenario]
    if exit_code == 0:
        return f"OPERATIONAL PASS: scenario={scenario}; smoke=OK; proceed with {scenario_doc}."
    if exit_code == 1:
        return (
            f"OPERATIONAL WARNING: scenario={scenario}; smoke=WARNING; proceed only with explicit operator review. "
            f"Follow {scenario_doc} and docs/ops/runbook/sop/when_smoke_fails.md."
        )
    if exit_code == 2:
        return (
            f"OPERATIONAL FAIL: scenario={scenario}; smoke=FAIL; stop this operational flow and follow "
            "docs/ops/runbook/sop/when_smoke_fails.md."
        )
    return (
        f"OPERATIONAL FAIL: scenario={scenario}; smoke=RUNNER_ERROR; treat as failed verification and follow "
        "docs/ops/runbook/sop/when_smoke_fails.md."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Operational scenario wrapper over production smoke checks.")
    parser.add_argument(
        "--scenario",
        required=True,
        choices=sorted(SCENARIO_PLAYBOOKS.keys()),
        help="Operational scenario context for smoke gating.",
    )
    parser.add_argument("--profile", default="prod", choices=["local", "prod"])
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument("--json-out", default="")
    parser.add_argument("--checks", default="")
    args = parser.parse_args()

    os.environ["FACTORY_PROFILE"] = args.profile
    load_profile_env()

    selected_checks = _parse_checks(args.checks)
    report = run_checks_with_error_capture(profile=args.profile, selected_check_ids=selected_checks)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    if args.as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(render_human_report(report))
        print(_render_operational_verdict(int(report["exit_code"]), args.scenario))

    raise SystemExit(int(report["exit_code"]))


if __name__ == "__main__":
    main()
