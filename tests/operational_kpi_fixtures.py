from __future__ import annotations

from services.analytics_center.operational_kpi import build_explainability_payload
from services.common import db as dbm


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


def seed_scope_isolation_jobs(conn) -> dict[str, str]:
    now = dbm.now_ts()
    ch_a = conn.execute("SELECT id, slug FROM channels WHERE slug='darkwood-reverie'").fetchone()
    ch_b = conn.execute("SELECT id, slug FROM channels WHERE slug='channel-b'").fetchone()
    assert ch_a is not None and ch_b is not None

    rel_a = int(
        conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_meta_file_id, created_at) VALUES(?, 'rel-a', 'd', '[]', '2026-04-11T00:00:00Z', 'meta-a', ?)",
            (int(ch_a["id"]), now),
        ).lastrowid
    )
    rel_b = int(
        conn.execute(
            "INSERT INTO releases(channel_id, title, description, tags_json, planned_at, origin_meta_file_id, created_at) VALUES(?, 'rel-b', 'd', '[]', '2026-05-11T00:00:00Z', 'meta-b', ?)",
            (int(ch_b["id"]), now),
        ).lastrowid
    )
    job_a = int(dbm.insert_job_with_lineage_defaults(conn, release_id=rel_a, job_type="UI", state="FAILED", stage="QA", priority=0, attempt=0, created_at=now - 7200, updated_at=now))
    job_b = int(dbm.insert_job_with_lineage_defaults(conn, release_id=rel_b, job_type="UI", state="DONE", stage="DONE", priority=0, attempt=0, created_at=now - 7200, updated_at=now))
    conn.execute(
        "INSERT INTO qa_reports(job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual, vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at) VALUES(?, 0, '[]', '{}', 60.0, 58.0, 'h264', 'aac', 24.0, 1920, 1080, 44100, 2, -14.0, -1.0, ?)",
        (job_a, now),
    )
    conn.execute(
        "INSERT INTO qa_reports(job_id, hard_ok, warnings_json, info_json, duration_expected, duration_actual, vcodec, acodec, fps, width, height, sr, ch, mean_volume_db, max_volume_db, created_at) VALUES(?, 1, '[]', '{}', 60.0, 60.0, 'h264', 'aac', 24.0, 1920, 1080, 44100, 2, -14.0, -1.0, ?)",
        (job_b, now),
    )
    conn.execute(
        "INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at) VALUES('darkwood-reverie', 'LONG', 'pa', '2026-04-15T00:00:00Z', NULL, 'FAILED', ?, ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO planned_releases(channel_slug, content_type, title, publish_at, notes, status, created_at, updated_at) VALUES('channel-b', 'LONG', 'pb', '2026-05-15T00:00:00Z', NULL, 'PLANNED', ?, ?)",
        (now, now),
    )
    return {"channel_a_slug": str(ch_a["slug"]), "channel_b_slug": str(ch_b["slug"]), "release_a_id": str(rel_a), "release_b_id": str(rel_b)}
