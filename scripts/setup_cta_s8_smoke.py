from __future__ import annotations

import json

from services.common import db as dbm
from services.common.env import Env


CHANNEL_SLUG = "darkwood-reverie"
TRACK_ID = "cta_s8_smoke_track_001"
GDRIVE_FILE_ID = "cta_s8_smoke_gdrive_file_001"
TAG_CODE = "CTA_S8_SMOKE"


def _upsert_channel(conn) -> None:
    conn.execute(
        """
        INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
            display_name=excluded.display_name,
            kind=excluded.kind,
            weight=excluded.weight,
            render_profile=excluded.render_profile,
            autopublish_enabled=excluded.autopublish_enabled
        """,
        (CHANNEL_SLUG, "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
    )

    conn.execute(
        "INSERT OR IGNORE INTO canon_channels(value) VALUES(?)",
        (CHANNEL_SLUG,),
    )
    conn.execute(
        "INSERT OR IGNORE INTO canon_thresholds(value) VALUES(?)",
        (CHANNEL_SLUG,),
    )


def _upsert_track_report_row(conn, now_ts: float) -> int:
    conn.execute(
        """
        INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(channel_slug, track_id) DO UPDATE SET
            source=excluded.source,
            filename=excluded.filename,
            title=excluded.title,
            artist=excluded.artist,
            duration_sec=excluded.duration_sec,
            analyzed_at=excluded.analyzed_at
        """,
        (
            CHANNEL_SLUG,
            TRACK_ID,
            GDRIVE_FILE_ID,
            "LOCAL_SMOKE",
            "cta_s8_smoke_track_001.wav",
            "CTA S8 Smoke Track",
            "Factory VM",
            8.0,
            now_ts,
            now_ts,
        ),
    )

    row = conn.execute(
        "SELECT id FROM tracks WHERE channel_slug = ? AND track_id = ? LIMIT 1",
        (CHANNEL_SLUG, TRACK_ID),
    ).fetchone()
    track_pk = int(row["id"])

    payload = json.dumps({"smoke": True, "slice": "CTA-S8"}, separators=(",", ":"))
    conn.execute(
        "INSERT OR REPLACE INTO track_features(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
        (track_pk, payload, now_ts),
    )
    conn.execute(
        "INSERT OR REPLACE INTO track_tags(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
        (track_pk, payload, now_ts),
    )
    conn.execute(
        "INSERT OR REPLACE INTO track_scores(track_pk, payload_json, computed_at) VALUES(?, ?, ?)",
        (track_pk, payload, now_ts),
    )
    return track_pk


def _upsert_smoke_tag(conn, now_ts: float) -> int:
    ts = str(now_ts)
    conn.execute(
        """
        INSERT INTO custom_tags(code, label, category, description, is_active, created_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(category, code) DO UPDATE SET
            label=excluded.label,
            description=excluded.description,
            is_active=excluded.is_active,
            updated_at=excluded.updated_at
        """,
        (TAG_CODE, "CTA S8 Smoke", "VISUAL", "Seed tag for CTA-S8 browser smoke", 1, ts, ts),
    )
    row = conn.execute(
        "SELECT id FROM custom_tags WHERE category = 'VISUAL' AND code = ? LIMIT 1",
        (TAG_CODE,),
    ).fetchone()
    return int(row["id"])


def main() -> None:
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        now_ts = dbm.now_ts()
        _upsert_channel(conn)
        track_pk = _upsert_track_report_row(conn, now_ts)
        tag_id = _upsert_smoke_tag(conn, now_ts)
    finally:
        conn.close()

    print("CTA-S8 smoke data ready")
    print(f"channel_slug={CHANNEL_SLUG}")
    print(f"track_pk={track_pk}")
    print(f"tag_code={TAG_CODE}")
    print(f"tag_id={tag_id}")


if __name__ == "__main__":
    main()
