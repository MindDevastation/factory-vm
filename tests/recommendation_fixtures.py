from __future__ import annotations

from services.analytics_center.helpers import canonicalize_scope_ref
from services.analytics_center.mf4_runtime import recompute_mf4
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


def seed_recommendation_inputs(conn, *, scope_type: str = "CHANNEL", scope_ref: str = "darkwood-reverie") -> None:
    canonical_scope_ref = canonicalize_scope_ref(conn, scope_type=scope_type, scope_ref=scope_ref)
    seed_mf4_mixed_input_snapshots(conn, scope_type=scope_type, scope_ref=scope_ref)
    seed_mf4_operational_kpi_snapshot(conn, scope_type=scope_type, scope_ref=scope_ref)
    conn.execute(
        "UPDATE analytics_operational_kpi_snapshots SET status_class = 'RISK' WHERE scope_type = ? AND scope_ref = ?",
        (scope_type, canonical_scope_ref),
    )
    recompute_mf4(
        conn,
        run_kind="FULL_STACK_RECOMPUTE",
        target_scope_type=scope_type,
        target_scope_ref=canonical_scope_ref,
        recompute_mode="FULL_RECOMPUTE",
    )
