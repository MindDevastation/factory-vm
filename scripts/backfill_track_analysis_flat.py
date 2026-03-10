from __future__ import annotations

from dataclasses import dataclass

from services.common import db as dbm
from services.common.env import Env
from services.common.profile import load_profile_env
from services.track_analyzer.track_analysis_flat import build_track_analysis_flat_row, upsert_track_analysis_flat


@dataclass
class BackfillSummary:
    scanned: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0


def backfill_track_analysis_flat(conn: object) -> BackfillSummary:
    summary = BackfillSummary()
    rows = conn.execute(
        """
        SELECT
            t.*,
            tf.payload_json AS features_payload_json,
            tf.computed_at AS features_computed_at,
            tt.payload_json AS tags_payload_json,
            tt.computed_at AS tags_computed_at,
            ts.payload_json AS scores_payload_json,
            ts.computed_at AS scores_computed_at
        FROM tracks t
        LEFT JOIN track_features tf ON tf.track_pk = t.id
        LEFT JOIN track_tags tt ON tt.track_pk = t.id
        LEFT JOIN track_scores ts ON ts.track_pk = t.id
        ORDER BY t.id ASC
        """
    ).fetchall()

    for row in rows:
        summary.scanned += 1
        try:
            features_payload = dbm.json_loads(row["features_payload_json"] or "")
            tags_payload = dbm.json_loads(row["tags_payload_json"] or "")
            scores_payload = dbm.json_loads(row["scores_payload_json"] or "")

            if not isinstance(features_payload, dict) or not isinstance(tags_payload, dict) or not isinstance(scores_payload, dict):
                summary.skipped += 1
                continue

            computed_values = [row["features_computed_at"], row["tags_computed_at"], row["scores_computed_at"]]
            computed_candidates = [float(v) for v in computed_values if v is not None]
            if not computed_candidates:
                summary.skipped += 1
                continue

            existing = conn.execute("SELECT 1 FROM track_analysis_flat WHERE track_pk = ?", (int(row["id"]),)).fetchone()
            flat_row = build_track_analysis_flat_row(
                track_row=row,
                features_payload=features_payload,
                tags_payload=tags_payload,
                scores_payload=scores_payload,
                analysis_computed_at=max(computed_candidates),
            )
            upsert_track_analysis_flat(conn, flat_row)
            if existing is None:
                summary.inserted += 1
            else:
                summary.updated += 1
        except Exception:
            summary.errors += 1

    return summary


def main() -> None:
    load_profile_env()
    env = Env.load()
    conn = dbm.connect(env)
    try:
        dbm.migrate(conn)
        summary = backfill_track_analysis_flat(conn)
        conn.commit()
    finally:
        conn.close()

    print(
        "scanned={scanned} inserted={inserted} updated={updated} skipped={skipped} errors={errors}".format(
            scanned=summary.scanned,
            inserted=summary.inserted,
            updated=summary.updated,
            skipped=summary.skipped,
            errors=summary.errors,
        )
    )


if __name__ == "__main__":
    main()
