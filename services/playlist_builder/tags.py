from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass


@dataclass(frozen=True)
class BuilderTagOption:
    source: str
    value: str
    label: str
    group: str
    count: int = 0


def normalize_filter_token(raw: str) -> str:
    token = str(raw or "").strip()
    if not token:
        return ""
    if ":" not in token:
        return token.lower()
    source, label = token.split(":", 1)
    source_norm = source.strip().lower()
    label_norm = label.strip().lower()
    if not source_norm or not label_norm:
        return token.lower()
    return f"{source_norm}:{label_norm}"


def candidate_filter_tokens(*, custom_codes: list[str], yamnet_tags: list[str], semantic_tags: list[str]) -> set[str]:
    tokens: set[str] = set()
    for source, values in (("custom", custom_codes), ("yamnet", yamnet_tags), ("semantic", semantic_tags)):
        for value in values:
            plain = str(value or "").strip().lower()
            if not plain:
                continue
            tokens.add(plain)
            tokens.add(f"{source}:{plain}")
    return tokens


def list_builder_tag_options(conn: sqlite3.Connection, *, channel_slug: str | None) -> list[BuilderTagOption]:
    scope_sql = "" if not channel_slug else " AND t.channel_slug = ?"
    scope_args: tuple[object, ...] = () if not channel_slug else (channel_slug,)

    custom_counts: dict[str, int] = {}
    custom_rows = conn.execute(
        f"""
        SELECT LOWER(ct.code) AS code, COUNT(*) AS use_count
        FROM custom_tags ct
        LEFT JOIN track_custom_tag_assignments tcta ON tcta.tag_id = ct.id AND tcta.state IN ('AUTO','MANUAL')
        LEFT JOIN tracks t ON t.id = tcta.track_pk
        WHERE ct.is_active = 1
          AND (? IS NULL OR t.channel_slug = ? OR t.channel_slug IS NULL)
        GROUP BY LOWER(ct.code)
        ORDER BY LOWER(ct.code) ASC
        """,
        (channel_slug, channel_slug),
    ).fetchall()
    for row in custom_rows:
        custom_counts[str(row["code"])] = int(row["use_count"] or 0)

    analyzer_rows = conn.execute(
        f"""
        SELECT taf.yamnet_top_tags_text, tt.payload_json
        FROM track_analysis_flat taf
        JOIN tracks t ON t.id = taf.track_pk
        LEFT JOIN track_tags tt ON tt.track_pk = taf.track_pk
        WHERE taf.analysis_status = 'ok'
        {scope_sql}
        """,
        scope_args,
    ).fetchall()

    yamnet_counts: dict[str, int] = defaultdict(int)
    semantic_counts: dict[str, int] = defaultdict(int)

    for row in analyzer_rows:
        yamnet_raw = str(row.get("yamnet_top_tags_text") or "")
        for part in yamnet_raw.split(","):
            tag = part.strip()
            if tag:
                yamnet_counts[tag] += 1
        payload_text = row.get("payload_json")
        if not payload_text:
            continue
        try:
            payload = json.loads(str(payload_text))
        except json.JSONDecodeError:
            continue
        semantic = ((payload.get("advanced_v1") or {}).get("semantic") or {})
        for key in ("mood_tags", "theme_tags"):
            for raw_tag in semantic.get(key) or []:
                tag = str(raw_tag or "").strip()
                if tag:
                    semantic_counts[tag] += 1

    options: list[BuilderTagOption] = []

    for code in sorted(custom_counts):
        options.append(BuilderTagOption(source="custom", value=f"custom:{code}", label=code, group="Custom tags", count=custom_counts[code]))
    for tag in sorted(yamnet_counts):
        options.append(BuilderTagOption(source="yamnet", value=f"yamnet:{tag}", label=tag, group="YAMNet tags", count=yamnet_counts[tag]))
    for tag in sorted(semantic_counts):
        options.append(BuilderTagOption(source="semantic", value=f"semantic:{tag}", label=tag, group="Semantic tags", count=semantic_counts[tag]))

    return options
