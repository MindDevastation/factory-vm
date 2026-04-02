from __future__ import annotations

from services.analytics_center.mf4_runtime import recompute_mf4
from services.analytics_center.recommendation_core import persist_recommendation_snapshot, synthesize_recommendations
from services.common import db as dbm
from tests.prediction_fixtures import seed_mf4_mixed_input_snapshots, seed_mf4_operational_kpi_snapshot


def seed_mf6_page_data(conn) -> dict[str, str | int]:
    channel_slug = "darkwood-reverie"
    seed_mf4_mixed_input_snapshots(conn, scope_type="CHANNEL", scope_ref=channel_slug)
    seed_mf4_operational_kpi_snapshot(conn, scope_type="CHANNEL", scope_ref=channel_slug)
    recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="CHANNEL", target_scope_ref=channel_slug, recompute_mode="FULL_RECOMPUTE")

    channel_id = int(conn.execute("SELECT id FROM channels WHERE slug = ?", (channel_slug,)).fetchone()["id"])
    release_id = int(
        conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, origin_meta_file_id, created_at) VALUES(?, 'mf6-rel', 'd', '[]', 'meta-mf6-rel', ?)",
            (channel_id, dbm.now_ts()),
        ).lastrowid
    )
    seed_mf4_mixed_input_snapshots(conn, scope_type="RELEASE", scope_ref=str(release_id))
    seed_mf4_operational_kpi_snapshot(conn, scope_type="RELEASE", scope_ref=str(release_id))
    recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="RELEASE", target_scope_ref=str(release_id), recompute_mode="FULL_RECOMPUTE")

    batch_month = "2026-04"
    seed_mf4_mixed_input_snapshots(conn, scope_type="BATCH_MONTH", scope_ref=batch_month)
    seed_mf4_operational_kpi_snapshot(conn, scope_type="BATCH_MONTH", scope_ref=batch_month)
    recompute_mf4(conn, run_kind="FULL_STACK_RECOMPUTE", target_scope_type="BATCH_MONTH", target_scope_ref=batch_month, recompute_mode="FULL_RECOMPUTE")

    for scope_type, scope_ref in (("CHANNEL", channel_slug), ("RELEASE", str(release_id)), ("BATCH_MONTH", batch_month)):
        recs = synthesize_recommendations(conn, scope_type=scope_type, scope_ref=scope_ref)
        for rec in recs[:2]:
            persist_recommendation_snapshot(conn, recommendation=rec)

    return {"channel_slug": channel_slug, "release_id": release_id, "batch_month": batch_month}
