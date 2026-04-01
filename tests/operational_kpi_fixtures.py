from __future__ import annotations

from services.analytics_center.operational_kpi import build_explainability_payload


def make_valid_explainability() -> dict:
    return build_explainability_payload(
        primary_reason_code="TEST_REASON",
        primary_reason_text="test reason",
        supporting_signals_json=[{"signal": "s1", "value": 1}],
        remediation_hint="inspect queue",
        baseline_scope_type="CHANNEL",
        baseline_scope_ref="darkwood-reverie",
        baseline_window_ref="latest",
        evidence_payload_json={"k": 1},
    )
