from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from collections import defaultdict
import json
import logging
import sqlite3
from typing import Any, Dict, List, Literal

from services.common import db as dbm
from services.planner.time_normalization import PublishAtValidationError, normalize_publish_at

logger = logging.getLogger(__name__)

DomainStatus = Literal["READY", "NOT_READY", "BLOCKED"]
CheckStatus = Literal["PASS", "NOT_READY", "BLOCKED"]
AggregateStatus = Literal["NOT_READY", "BLOCKED", "READY_FOR_MATERIALIZATION"]

_DOMAIN_ORDER = ["planning_identity", "scheduling", "metadata", "playlist", "visual_assets"]
_REASON_SEVERITY_RANK = {"BLOCKED": 0, "NOT_READY": 1}


class PlannedReleaseReadinessNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class _FieldDefaults:
    source_type: str
    default_field: str
    default_name: str


_METADATA_FIELD_DEFAULTS: dict[str, _FieldDefaults] = {
    "title": _FieldDefaults("title_template", "default_title_template_id", "title"),
    "description": _FieldDefaults("description_template", "default_description_template_id", "description"),
    "tags": _FieldDefaults("video_tag_preset", "default_video_tag_preset_id", "tags"),
}


class PlannedReleaseReadinessService:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    def evaluate_many(self, *, planned_release_ids: list[int]) -> dict[int, dict[str, Any]]:
        ordered_ids = [int(item) for item in planned_release_ids]
        if not ordered_ids:
            return {}

        unique_ids = list(dict.fromkeys(ordered_ids))
        placeholders = ",".join("?" for _ in unique_ids)

        planned_rows = self._conn.execute(
            f"SELECT * FROM planned_releases WHERE id IN ({placeholders})",
            tuple(unique_ids),
        ).fetchall()
        planned_by_id = {int(row["id"]): dict(row) for row in planned_rows}
        missing = [planned_release_id for planned_release_id in unique_ids if planned_release_id not in planned_by_id]
        if missing:
            raise PlannedReleaseReadinessNotFoundError(missing[0])

        linked_rows = self._conn.execute(
            f"""
            SELECT prl.planned_release_id, r.*, c.slug AS release_channel_slug
            FROM planner_release_links prl
            JOIN releases r ON r.id = prl.release_id
            LEFT JOIN channels c ON c.id = r.channel_id
            WHERE prl.planned_release_id IN ({placeholders})
            """,
            tuple(unique_ids),
        ).fetchall()
        linked_by_planned: dict[int, dict[str, Any]] = {}
        for row in linked_rows:
            linked_by_planned[int(row["planned_release_id"])] = dict(row)

        channel_slugs = sorted({str(row.get("channel_slug") or "").strip() for row in planned_by_id.values() if str(row.get("channel_slug") or "").strip()})
        defaults_by_channel: dict[str, dict[str, Any]] = {}
        if channel_slugs:
            channel_placeholders = ",".join("?" for _ in channel_slugs)
            defaults_rows = self._conn.execute(
                f"SELECT * FROM channel_metadata_defaults WHERE channel_slug IN ({channel_placeholders})",
                tuple(channel_slugs),
            ).fetchall()
            defaults_by_channel = {str(row["channel_slug"]): dict(row) for row in defaults_rows}

        channels_existing: set[str] = set()
        if channel_slugs:
            channel_placeholders = ",".join("?" for _ in channel_slugs)
            channel_rows = self._conn.execute(
                f"SELECT slug FROM channels WHERE slug IN ({channel_placeholders})",
                tuple(channel_slugs),
            ).fetchall()
            channels_existing = {str(row["slug"]) for row in channel_rows}

        release_ids = [int(item["id"]) for item in linked_by_planned.values()]
        active_playlist_counts: dict[int, int] = {}
        draft_backgrounds: dict[int, dict[str, Any]] = {}
        if release_ids:
            release_placeholders = ",".join("?" for _ in release_ids)
            active_rows = self._conn.execute(
                f"""
                SELECT j.release_id AS release_id,
                       COALESCE((SELECT COUNT(1) FROM playlist_history_items phi WHERE phi.history_id = ph.id), 0) AS item_count
                FROM playlist_history ph
                JOIN jobs j ON j.id = ph.job_id
                JOIN (
                    SELECT j2.release_id AS release_id, MAX(ph2.created_at) AS created_at
                    FROM playlist_history ph2
                    JOIN jobs j2 ON j2.id = ph2.job_id
                    WHERE ph2.is_active = 1 AND j2.release_id IN ({release_placeholders})
                    GROUP BY j2.release_id
                ) latest ON latest.release_id = j.release_id AND latest.created_at = ph.created_at
                WHERE ph.is_active = 1
                """,
                tuple(release_ids),
            ).fetchall()
            active_playlist_counts = {int(row["release_id"]): int(row["item_count"]) for row in active_rows}

            draft_rows = self._conn.execute(
                f"""
                SELECT j.release_id AS release_id, d.background_name, d.background_ext
                FROM ui_job_drafts d
                JOIN jobs j ON j.id = d.job_id
                JOIN (
                    SELECT j2.release_id AS release_id, MAX(d2.updated_at) AS updated_at
                    FROM ui_job_drafts d2
                    JOIN jobs j2 ON j2.id = d2.job_id
                    WHERE j2.release_id IN ({release_placeholders})
                    GROUP BY j2.release_id
                ) latest ON latest.release_id = j.release_id AND latest.updated_at = d.updated_at
                """,
                tuple(release_ids),
            ).fetchall()
            draft_backgrounds = {
                int(row["release_id"]): {
                    "background_name": row["background_name"],
                    "background_ext": row["background_ext"],
                }
                for row in draft_rows
            }

        settings_by_channel: dict[str, dict[str, Any]] = {}
        tracks_count_by_channel: dict[str, int] = {}
        if channel_slugs:
            channel_placeholders = ",".join("?" for _ in channel_slugs)
            settings_rows = self._conn.execute(
                f"SELECT * FROM playlist_builder_channel_settings WHERE channel_slug IN ({channel_placeholders})",
                tuple(channel_slugs),
            ).fetchall()
            settings_by_channel = {str(row["channel_slug"]): dict(row) for row in settings_rows}

            track_rows = self._conn.execute(
                f"SELECT channel_slug, COUNT(1) AS c FROM tracks WHERE channel_slug IN ({channel_placeholders}) GROUP BY channel_slug",
                tuple(channel_slugs),
            ).fetchall()
            tracks_count_by_channel = {str(row["channel_slug"]): int(row["c"]) for row in track_rows}

        source_ids: dict[str, set[int]] = defaultdict(set)
        for channel_slug, defaults_row in defaults_by_channel.items():
            del channel_slug
            for field, spec in _METADATA_FIELD_DEFAULTS.items():
                source_id = defaults_row.get(spec.default_field)
                if source_id is None:
                    continue
                source_ids[field].add(int(source_id))

        title_sources: dict[int, dict[str, Any]] = {}
        desc_sources: dict[int, dict[str, Any]] = {}
        tag_sources: dict[int, dict[str, Any]] = {}
        if source_ids["title"]:
            ph = ",".join("?" for _ in source_ids["title"])
            rows = self._conn.execute(f"SELECT * FROM title_templates WHERE id IN ({ph})", tuple(sorted(source_ids["title"]))).fetchall()
            title_sources = {int(row["id"]): dict(row) for row in rows}
        if source_ids["description"]:
            ph = ",".join("?" for _ in source_ids["description"])
            rows = self._conn.execute(f"SELECT * FROM description_templates WHERE id IN ({ph})", tuple(sorted(source_ids["description"]))).fetchall()
            desc_sources = {int(row["id"]): dict(row) for row in rows}
        if source_ids["tags"]:
            ph = ",".join("?" for _ in source_ids["tags"])
            rows = self._conn.execute(f"SELECT * FROM video_tag_presets WHERE id IN ({ph})", tuple(sorted(source_ids["tags"]))).fetchall()
            tag_sources = {int(row["id"]): dict(row) for row in rows}

        out: dict[int, dict[str, Any]] = {}
        for planned_release_id in ordered_ids:
            planner_row = planned_by_id[planned_release_id]
            linked_release = linked_by_planned.get(planned_release_id)
            channel_slug = str(planner_row.get("channel_slug") or "").strip()
            defaults_row = defaults_by_channel.get(channel_slug)

            domains = {
                "planning_identity": self._evaluate_planning_identity_batched(planner_row, linked_release, channels_existing),
                "scheduling": self._evaluate_scheduling(planner_row),
                "metadata": self._evaluate_metadata_batched(
                    planner_row=planner_row,
                    linked_release=linked_release,
                    defaults_row=defaults_row,
                    title_sources=title_sources,
                    desc_sources=desc_sources,
                    tag_sources=tag_sources,
                ),
                "playlist": self._evaluate_playlist_batched(
                    planner_row=planner_row,
                    linked_release=linked_release,
                    active_playlist_counts=active_playlist_counts,
                    settings_by_channel=settings_by_channel,
                    tracks_count_by_channel=tracks_count_by_channel,
                ),
                "visual_assets": self._evaluate_visual_assets_batched(
                    linked_release=linked_release,
                    draft_backgrounds=draft_backgrounds,
                ),
            }
            aggregate_status = self._resolve_aggregate_status(domains=domains)
            reasons = self._flatten_reasons(domains=domains)
            summary = self._build_summary(domains=domains)
            primary_reason = self._select_primary_reason(reasons=reasons)
            computed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            out[planned_release_id] = {
                "planned_release_id": int(planned_release_id),
                "aggregate_status": aggregate_status,
                "domains": domains,
                "summary": summary,
                "reasons": reasons,
                "computed_at": computed_at,
                "primary_reason": primary_reason,
                "primary_remediation_hint": (primary_reason or {}).get("remediation_hint"),
            }
        return out

    def evaluate(self, *, planned_release_id: int) -> dict[str, Any]:
        planner_row = self._conn.execute("SELECT * FROM planned_releases WHERE id = ?", (planned_release_id,)).fetchone()
        if not planner_row:
            raise PlannedReleaseReadinessNotFoundError(planned_release_id)

        linked_release = self._fetch_linked_release(planned_release_id=planned_release_id)
        defaults_row = self._conn.execute(
            "SELECT * FROM channel_metadata_defaults WHERE channel_slug = ?",
            (str(planner_row.get("channel_slug") or ""),),
        ).fetchone()

        domains = {
            "planning_identity": self._evaluate_planning_identity(planner_row, linked_release),
            "scheduling": self._evaluate_scheduling(planner_row),
            "metadata": self._evaluate_metadata(planner_row, linked_release, defaults_row),
            "playlist": self._evaluate_playlist(planner_row, linked_release),
            "visual_assets": self._evaluate_visual_assets(linked_release),
        }

        aggregate_status = self._resolve_aggregate_status(domains=domains)
        reasons = self._flatten_reasons(domains=domains)
        summary = self._build_summary(domains=domains)
        primary_reason = self._select_primary_reason(reasons=reasons)

        computed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        logger.info(
            "planned_release_readiness_evaluated planned_release_id=%s aggregate_status=%s ready_domains=%s not_ready_domains=%s blocked_domains=%s",
            planned_release_id,
            aggregate_status,
            summary["ready_domains"],
            summary["not_ready_domains"],
            summary["blocked_domains"],
        )
        return {
            "planned_release_id": int(planned_release_id),
            "aggregate_status": aggregate_status,
            "domains": domains,
            "summary": summary,
            "reasons": reasons,
            "computed_at": computed_at,
            "primary_reason": primary_reason,
            "primary_remediation_hint": (primary_reason or {}).get("remediation_hint"),
        }

    def _fetch_linked_release(self, *, planned_release_id: int) -> dict[str, Any] | None:
        row = self._conn.execute(
            """
            SELECT r.*, c.slug AS release_channel_slug
            FROM planner_release_links prl
            JOIN releases r ON r.id = prl.release_id
            LEFT JOIN channels c ON c.id = r.channel_id
            WHERE prl.planned_release_id = ?
            """,
            (planned_release_id,),
        ).fetchone()
        return dict(row) if row else None

    def _evaluate_planning_identity(self, planner_row: dict[str, Any], linked_release: dict[str, Any] | None) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        channel_slug = str(planner_row.get("channel_slug") or "").strip()
        status_value = str(planner_row.get("status") or "").strip()

        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CHANNEL_MISSING",
                domain="planning_identity",
                passed=bool(channel_slug),
                severity="BLOCKED",
                message_fail="Planned release is missing channel binding.",
                hint_fail="Set a valid channel_slug on the planned release.",
            )
        )

        channel_row = None
        if channel_slug:
            channel_row = self._conn.execute("SELECT slug FROM channels WHERE slug = ?", (channel_slug,)).fetchone()
        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CHANNEL_NOT_FOUND",
                domain="planning_identity",
                passed=(not channel_slug) or bool(channel_row),
                severity="BLOCKED",
                message_fail="Planned release channel does not resolve to an existing channel.",
                hint_fail="Create the missing channel or fix channel_slug to an existing channel.",
            )
        )

        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CORRUPT",
                domain="planning_identity",
                passed=(planner_row.get("id") is not None and status_value in {"PLANNED", "LOCKED", "FAILED"}),
                severity="BLOCKED",
                message_fail="Planned release core identity fields are inconsistent.",
                hint_fail="Repair planner row identity/status integrity.",
            )
        )

        link_ok = True
        if linked_release is not None and channel_slug:
            link_ok = str(linked_release.get("release_channel_slug") or "") == channel_slug
        checks.append(
            self._make_check(
                code="PRR_IDENTITY_TARGET_LINK_CONFLICT",
                domain="planning_identity",
                passed=link_ok,
                severity="BLOCKED",
                message_fail="Linked release channel conflicts with planned release channel binding.",
                hint_fail="Repair planner_release_links or planned release channel_slug so both point to the same channel.",
            )
        )

        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_planning_identity_batched(
        self,
        planner_row: dict[str, Any],
        linked_release: dict[str, Any] | None,
        channels_existing: set[str],
    ) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        channel_slug = str(planner_row.get("channel_slug") or "").strip()
        status_value = str(planner_row.get("status") or "").strip()

        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CHANNEL_MISSING",
                domain="planning_identity",
                passed=bool(channel_slug),
                severity="BLOCKED",
                message_fail="Planned release is missing channel binding.",
                hint_fail="Set a valid channel_slug on the planned release.",
            )
        )
        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CHANNEL_NOT_FOUND",
                domain="planning_identity",
                passed=(not channel_slug) or (channel_slug in channels_existing),
                severity="BLOCKED",
                message_fail="Planned release channel does not resolve to an existing channel.",
                hint_fail="Create the missing channel or fix channel_slug to an existing channel.",
            )
        )
        checks.append(
            self._make_check(
                code="PRR_IDENTITY_CORRUPT",
                domain="planning_identity",
                passed=(planner_row.get("id") is not None and status_value in {"PLANNED", "LOCKED", "FAILED"}),
                severity="BLOCKED",
                message_fail="Planned release core identity fields are inconsistent.",
                hint_fail="Repair planner row identity/status integrity.",
            )
        )
        link_ok = True
        if linked_release is not None and channel_slug:
            link_ok = str(linked_release.get("release_channel_slug") or "") == channel_slug
        checks.append(
            self._make_check(
                code="PRR_IDENTITY_TARGET_LINK_CONFLICT",
                domain="planning_identity",
                passed=link_ok,
                severity="BLOCKED",
                message_fail="Linked release channel conflicts with planned release channel binding.",
                hint_fail="Repair planner_release_links or planned release channel_slug so both point to the same channel.",
            )
        )
        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_scheduling(self, planner_row: dict[str, Any]) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        publish_at = planner_row.get("publish_at")
        publish_at_text = str(publish_at or "").strip()

        checks.append(
            self._make_check(
                code="PRR_SCHEDULING_MISSING",
                domain="scheduling",
                passed=bool(publish_at_text),
                severity="NOT_READY",
                message_fail="Planned release scheduling datetime is missing.",
                hint_fail="Set planned_releases.publish_at to a valid ISO8601 datetime.",
            )
        )

        valid = True
        if publish_at_text:
            try:
                normalize_publish_at(publish_at_text)
            except PublishAtValidationError:
                valid = False
        checks.append(
            self._make_check(
                code="PRR_SCHEDULING_INVALID",
                domain="scheduling",
                passed=(not publish_at_text) or valid,
                severity="BLOCKED",
                message_fail="Planned release scheduling datetime is invalid.",
                hint_fail="Fix planned_releases.publish_at to a parseable ISO8601 datetime value.",
            )
        )
        checks.append(
            self._make_check(
                code="PRR_SCHEDULING_CONTRADICTION",
                domain="scheduling",
                passed=True,
                severity="BLOCKED",
                message_fail="Planned release scheduling fields are contradictory.",
                hint_fail="Fix contradictory scheduling fields so canonical planning datetime is consistent.",
            )
        )

        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_metadata(
        self,
        planner_row: dict[str, Any],
        linked_release: dict[str, Any] | None,
        defaults_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        publish_at_text = str(planner_row.get("publish_at") or "").strip()
        scheduling_valid = bool(publish_at_text)
        if scheduling_valid:
            try:
                normalize_publish_at(publish_at_text)
            except PublishAtValidationError:
                scheduling_valid = False

        checks.extend(self._evaluate_metadata_field("title", linked_release, defaults_row, scheduling_valid))
        checks.extend(self._evaluate_metadata_field("description", linked_release, defaults_row, scheduling_valid))
        checks.extend(self._evaluate_metadata_field("tags", linked_release, defaults_row, scheduling_valid))

        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_metadata_field(
        self,
        field: Literal["title", "description", "tags"],
        linked_release: dict[str, Any] | None,
        defaults_row: dict[str, Any] | None,
        scheduling_valid: bool,
    ) -> list[dict[str, Any]]:
        spec = _METADATA_FIELD_DEFAULTS[field]
        base = f"PRR_METADATA_{field.upper()}"
        checks: list[dict[str, Any]] = []

        if field == "title":
            present_valid = linked_release is not None and bool(str(linked_release.get("title") or "").strip())
            present_invalid = linked_release is not None and not present_valid
        elif field == "description":
            present_valid = linked_release is not None and bool(str(linked_release.get("description") or "").strip())
            present_invalid = linked_release is not None and not present_valid
        else:
            present_valid, present_invalid = self._tags_validity(linked_release)

        checks.append(
            {
                "code": f"{base}_PRESENT",
                "status": "PASS" if present_valid else "NOT_READY",
                "message": (
                    f"{field.title()} is present and structurally valid."
                    if present_valid
                    else f"{field.title()} is not currently present in effective release metadata."
                ),
                "remediation_hint": (
                    "No action required."
                    if present_valid
                    else f"Provide a valid {field} value or configure a deterministic default source."
                ),
            }
        )

        if present_valid:
            return checks

        source_state = self._resolve_default_source(defaults_row=defaults_row, spec=spec, expected_channel=str(linked_release.get("release_channel_slug") if linked_release else ""))
        if source_state == "missing":
            checks.append(
                {
                    "code": f"{base}_SOURCE_MISSING",
                    "status": "NOT_READY",
                    "message": f"No usable default source is configured for {field}.",
                    "remediation_hint": f"Configure an ACTIVE, VALID default {spec.source_type} for this channel.",
                }
            )
            return checks

        if source_state == "invalid":
            checks.append(
                {
                    "code": f"{base}_INVALID",
                    "status": "BLOCKED",
                    "message": f"Configured default source for {field} is invalid or inconsistent.",
                    "remediation_hint": f"Fix default {spec.source_type} linkage/status/validation for this channel.",
                }
            )
            return checks

        if present_invalid:
            checks.append(
                {
                    "code": f"{base}_INVALID",
                    "status": "BLOCKED",
                    "message": f"Existing {field} value is structurally invalid.",
                    "remediation_hint": f"Repair existing {field} value or replace it with a valid deterministic source.",
                }
            )
            return checks

        if not scheduling_valid:
            checks.append(
                {
                    "code": f"{base}_CONTEXT_MISSING",
                    "status": "NOT_READY",
                    "message": f"{field.title()} default source exists, but scheduling context is missing or invalid.",
                    "remediation_hint": "Fix planned_releases.publish_at with a valid ISO8601 datetime.",
                }
            )
            return checks

        checks[0]["status"] = "PASS"
        checks[0]["message"] = f"{field.title()} can be deterministically prepared from current default source and context."
        checks[0]["remediation_hint"] = "No action required."
        return checks

    def _resolve_default_source(self, *, defaults_row: dict[str, Any] | None, spec: _FieldDefaults, expected_channel: str) -> str:
        if not defaults_row:
            return "missing"
        source_id = defaults_row.get(spec.default_field)
        if source_id is None:
            return "missing"

        if spec.source_type == "title_template":
            source = dbm.get_title_template_by_id(self._conn, int(source_id))
        elif spec.source_type == "description_template":
            source = dbm.get_description_template_by_id(self._conn, int(source_id))
        else:
            source = dbm.get_video_tag_preset_by_id(self._conn, int(source_id))

        if not source:
            return "invalid"
        source_channel = str(source.get("channel_slug") or "")
        effective_channel = expected_channel or str(defaults_row.get("channel_slug") or "")
        if source_channel != effective_channel:
            return "invalid"
        if str(source.get("status") or "") != "ACTIVE":
            return "invalid"
        if str(source.get("validation_status") or "") != "VALID":
            return "invalid"
        return "usable"

    def _resolve_default_source_batched(
        self,
        *,
        defaults_row: dict[str, Any] | None,
        spec: _FieldDefaults,
        expected_channel: str,
        title_sources: dict[int, dict[str, Any]],
        desc_sources: dict[int, dict[str, Any]],
        tag_sources: dict[int, dict[str, Any]],
    ) -> str:
        if not defaults_row:
            return "missing"
        source_id = defaults_row.get(spec.default_field)
        if source_id is None:
            return "missing"
        lookup_id = int(source_id)
        if spec.source_type == "title_template":
            source = title_sources.get(lookup_id)
        elif spec.source_type == "description_template":
            source = desc_sources.get(lookup_id)
        else:
            source = tag_sources.get(lookup_id)
        if not source:
            return "invalid"
        source_channel = str(source.get("channel_slug") or "")
        effective_channel = expected_channel or str(defaults_row.get("channel_slug") or "")
        if source_channel != effective_channel:
            return "invalid"
        if str(source.get("status") or "") != "ACTIVE":
            return "invalid"
        if str(source.get("validation_status") or "") != "VALID":
            return "invalid"
        return "usable"

    def _evaluate_metadata_batched(
        self,
        *,
        planner_row: dict[str, Any],
        linked_release: dict[str, Any] | None,
        defaults_row: dict[str, Any] | None,
        title_sources: dict[int, dict[str, Any]],
        desc_sources: dict[int, dict[str, Any]],
        tag_sources: dict[int, dict[str, Any]],
    ) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        publish_at_text = str(planner_row.get("publish_at") or "").strip()
        scheduling_valid = bool(publish_at_text)
        if scheduling_valid:
            try:
                normalize_publish_at(publish_at_text)
            except PublishAtValidationError:
                scheduling_valid = False

        for field in ("title", "description", "tags"):
            checks.extend(
                self._evaluate_metadata_field_batched(
                    field=field,  # type: ignore[arg-type]
                    linked_release=linked_release,
                    defaults_row=defaults_row,
                    scheduling_valid=scheduling_valid,
                    title_sources=title_sources,
                    desc_sources=desc_sources,
                    tag_sources=tag_sources,
                )
            )
        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_metadata_field_batched(
        self,
        *,
        field: Literal["title", "description", "tags"],
        linked_release: dict[str, Any] | None,
        defaults_row: dict[str, Any] | None,
        scheduling_valid: bool,
        title_sources: dict[int, dict[str, Any]],
        desc_sources: dict[int, dict[str, Any]],
        tag_sources: dict[int, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        spec = _METADATA_FIELD_DEFAULTS[field]
        base = f"PRR_METADATA_{field.upper()}"
        checks: list[dict[str, Any]] = []
        if field == "title":
            present_valid = linked_release is not None and bool(str(linked_release.get("title") or "").strip())
            present_invalid = linked_release is not None and not present_valid
        elif field == "description":
            present_valid = linked_release is not None and bool(str(linked_release.get("description") or "").strip())
            present_invalid = linked_release is not None and not present_valid
        else:
            present_valid, present_invalid = self._tags_validity(linked_release)

        checks.append(
            {
                "code": f"{base}_PRESENT",
                "status": "PASS" if present_valid else "NOT_READY",
                "message": (
                    f"{field.title()} is present and structurally valid."
                    if present_valid
                    else f"{field.title()} is not currently present in effective release metadata."
                ),
                "remediation_hint": (
                    "No action required."
                    if present_valid
                    else f"Provide a valid {field} value or configure a deterministic default source."
                ),
            }
        )
        if present_valid:
            return checks
        source_state = self._resolve_default_source_batched(
            defaults_row=defaults_row,
            spec=spec,
            expected_channel=str(linked_release.get("release_channel_slug") if linked_release else ""),
            title_sources=title_sources,
            desc_sources=desc_sources,
            tag_sources=tag_sources,
        )
        if source_state == "missing":
            checks.append(
                {
                    "code": f"{base}_SOURCE_MISSING",
                    "status": "NOT_READY",
                    "message": f"No usable default source is configured for {field}.",
                    "remediation_hint": f"Configure an ACTIVE, VALID default {spec.source_type} for this channel.",
                }
            )
            return checks
        if source_state == "invalid":
            checks.append(
                {
                    "code": f"{base}_INVALID",
                    "status": "BLOCKED",
                    "message": f"Configured default source for {field} is invalid or inconsistent.",
                    "remediation_hint": f"Fix default {spec.source_type} linkage/status/validation for this channel.",
                }
            )
            return checks
        if present_invalid:
            checks.append(
                {
                    "code": f"{base}_INVALID",
                    "status": "BLOCKED",
                    "message": f"Existing {field} value is structurally invalid.",
                    "remediation_hint": f"Repair existing {field} value or replace it with a valid deterministic source.",
                }
            )
            return checks
        if not scheduling_valid:
            checks.append(
                {
                    "code": f"{base}_CONTEXT_MISSING",
                    "status": "NOT_READY",
                    "message": f"{field.title()} default source exists, but scheduling context is missing or invalid.",
                    "remediation_hint": "Fix planned_releases.publish_at with a valid ISO8601 datetime.",
                }
            )
            return checks
        checks[0]["status"] = "PASS"
        checks[0]["message"] = f"{field.title()} can be deterministically prepared from current default source and context."
        checks[0]["remediation_hint"] = "No action required."
        return checks

    def _tags_validity(self, linked_release: dict[str, Any] | None) -> tuple[bool, bool]:
        if linked_release is None:
            return False, False
        raw_tags = linked_release.get("tags_json")
        try:
            parsed = json.loads(str(raw_tags))
        except Exception:
            return False, True
        if not isinstance(parsed, list):
            return False, True
        normalized = [str(item).strip() for item in parsed if isinstance(item, str)]
        if len(normalized) != len(parsed):
            return False, True
        if any(not item for item in normalized):
            return False, True
        return bool(normalized), False

    def _evaluate_playlist(self, planner_row: dict[str, Any], linked_release: dict[str, Any] | None) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        release_id = int(linked_release["id"]) if linked_release is not None else None

        active_history = self._conn.execute(
            """
            SELECT ph.id,
                   COALESCE((SELECT COUNT(1) FROM playlist_history_items phi WHERE phi.history_id = ph.id), 0) AS item_count
            FROM playlist_history ph
            JOIN jobs j ON j.id = ph.job_id
            WHERE j.release_id = ? AND ph.is_active = 1
            ORDER BY ph.created_at DESC
            LIMIT 1
            """,
            (release_id,),
        ).fetchone() if release_id is not None else None

        channel_slug = str(planner_row.get("channel_slug") or "").strip()
        settings = self._conn.execute(
            "SELECT * FROM playlist_builder_channel_settings WHERE channel_slug = ?",
            (channel_slug,),
        ).fetchone()
        tracks_row = self._conn.execute(
            "SELECT COUNT(1) AS c FROM tracks WHERE channel_slug = ?",
            (channel_slug,),
        ).fetchone()
        tracks_count = int((tracks_row or {}).get("c") or 0)

        has_present = bool(active_history and int(active_history.get("item_count") or 0) > 0)
        can_prepare = bool(settings) and int(settings.get("min_duration_min") or 0) <= int(settings.get("max_duration_min") or 0) and tracks_count > 0
        checks.append(
            {
                "code": "PRR_PLAYLIST_PRESENT",
                "status": "PASS" if (has_present or can_prepare) else "NOT_READY",
                "message": (
                    "Active playlist composition is present and non-empty."
                    if has_present
                    else (
                        "Playlist can be deterministically prepared from builder settings and available tracks."
                        if can_prepare
                        else "No active effective playlist composition is currently present."
                    )
                ),
                "remediation_hint": (
                    "No action required." if (has_present or can_prepare) else "Provide playlist composition or ensure deterministic builder path is configured."
                ),
            }
        )

        if has_present:
            return {"status": self._domain_status(checks), "checks": checks}

        if active_history and int(active_history.get("item_count") or 0) == 0:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_INVALID",
                    "status": "BLOCKED",
                    "message": "Active playlist history exists but contains zero playlist items.",
                    "remediation_hint": "Repair playlist history or regenerate playlist so active entry has at least one item.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        if not settings:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_SOURCE_MISSING",
                    "status": "NOT_READY",
                    "message": "No playlist builder settings are configured for this channel.",
                    "remediation_hint": "Configure playlist_builder_channel_settings for the channel.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        if int(settings.get("min_duration_min") or 0) > int(settings.get("max_duration_min") or 0):
            checks.append(
                {
                    "code": "PRR_PLAYLIST_INVALID",
                    "status": "BLOCKED",
                    "message": "Playlist builder settings are contradictory (min_duration_min > max_duration_min).",
                    "remediation_hint": "Fix playlist builder settings so min_duration_min <= max_duration_min.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        if tracks_count <= 0:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_CONTEXT_MISSING",
                    "status": "NOT_READY",
                    "message": "Playlist settings exist, but no candidate tracks are available for the channel.",
                    "remediation_hint": "Import/analyze channel tracks before building playlist composition.",
                }
            )

        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_visual_assets(self, linked_release: dict[str, Any] | None) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        if linked_release is None:
            checks.append(
                {
                    "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                    "status": "NOT_READY",
                    "message": "No linked release/job draft is available for visual assignment checks.",
                    "remediation_hint": "Provide a linked job draft with required background assignment.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        draft = self._conn.execute(
            """
            SELECT d.background_name, d.background_ext
            FROM ui_job_drafts d
            JOIN jobs j ON j.id = d.job_id
            WHERE j.release_id = ?
            ORDER BY d.updated_at DESC
            LIMIT 1
            """,
            (int(linked_release["id"]),),
        ).fetchone()

        if not draft:
            checks.append(
                {
                    "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                    "status": "NOT_READY",
                    "message": "No ui_job_drafts visual assignment exists for linked release jobs.",
                    "remediation_hint": "Create or update ui_job_drafts with required background assignment.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        background_name = str(draft.get("background_name") or "").strip()
        background_ext = str(draft.get("background_ext") or "").strip()
        if bool(background_name) != bool(background_ext):
            checks.append(
                {
                    "code": "PRR_VISUAL_ASSIGNMENT_INVALID",
                    "status": "BLOCKED",
                    "message": "Background visual assignment is partially populated and inconsistent.",
                    "remediation_hint": "Set both background_name and background_ext or clear both before assigning valid values.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        if not background_name:
            checks.append(
                {
                    "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                    "status": "NOT_READY",
                    "message": "Required background visual assignment is missing.",
                    "remediation_hint": "Set background_name and background_ext in the linked ui_job_drafts record.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}

        checks.append(
            {
                "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                "status": "PASS",
                "message": "Required background visual assignment is present.",
                "remediation_hint": "No action required.",
            }
        )
        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_playlist_batched(
        self,
        *,
        planner_row: dict[str, Any],
        linked_release: dict[str, Any] | None,
        active_playlist_counts: dict[int, int],
        settings_by_channel: dict[str, dict[str, Any]],
        tracks_count_by_channel: dict[str, int],
    ) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        release_id = int(linked_release["id"]) if linked_release is not None else None
        item_count = active_playlist_counts.get(int(release_id), -1) if release_id is not None else -1
        has_active_entry = item_count >= 0
        has_present = item_count > 0
        channel_slug = str(planner_row.get("channel_slug") or "").strip()
        settings = settings_by_channel.get(channel_slug)
        tracks_count = tracks_count_by_channel.get(channel_slug, 0)
        can_prepare = bool(settings) and int((settings or {}).get("min_duration_min") or 0) <= int((settings or {}).get("max_duration_min") or 0) and tracks_count > 0
        checks.append(
            {
                "code": "PRR_PLAYLIST_PRESENT",
                "status": "PASS" if (has_present or can_prepare) else "NOT_READY",
                "message": (
                    "Active playlist composition is present and non-empty."
                    if has_present
                    else (
                        "Playlist can be deterministically prepared from builder settings and available tracks."
                        if can_prepare
                        else "No active effective playlist composition is currently present."
                    )
                ),
                "remediation_hint": ("No action required." if (has_present or can_prepare) else "Provide playlist composition or ensure deterministic builder path is configured."),
            }
        )
        if has_present:
            return {"status": self._domain_status(checks), "checks": checks}
        if has_active_entry and item_count == 0:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_INVALID",
                    "status": "BLOCKED",
                    "message": "Active playlist history exists but contains zero playlist items.",
                    "remediation_hint": "Repair playlist history or regenerate playlist so active entry has at least one item.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}
        if not settings:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_SOURCE_MISSING",
                    "status": "NOT_READY",
                    "message": "No playlist builder settings are configured for this channel.",
                    "remediation_hint": "Configure playlist_builder_channel_settings for the channel.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}
        if int(settings.get("min_duration_min") or 0) > int(settings.get("max_duration_min") or 0):
            checks.append(
                {
                    "code": "PRR_PLAYLIST_INVALID",
                    "status": "BLOCKED",
                    "message": "Playlist builder settings are contradictory (min_duration_min > max_duration_min).",
                    "remediation_hint": "Fix playlist builder settings so min_duration_min <= max_duration_min.",
                }
            )
            return {"status": self._domain_status(checks), "checks": checks}
        if tracks_count <= 0:
            checks.append(
                {
                    "code": "PRR_PLAYLIST_CONTEXT_MISSING",
                    "status": "NOT_READY",
                    "message": "Playlist settings exist, but no candidate tracks are available for the channel.",
                    "remediation_hint": "Import/analyze channel tracks before building playlist composition.",
                }
            )
        return {"status": self._domain_status(checks), "checks": checks}

    def _evaluate_visual_assets_batched(
        self,
        *,
        linked_release: dict[str, Any] | None,
        draft_backgrounds: dict[int, dict[str, Any]],
    ) -> dict[str, Any]:
        if linked_release is None:
            return {
                "status": "NOT_READY",
                "checks": [
                    {
                        "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                        "status": "NOT_READY",
                        "message": "No linked release/job draft is available for visual assignment checks.",
                        "remediation_hint": "Provide a linked job draft with required background assignment.",
                    }
                ],
            }
        release_id = int(linked_release["id"])
        draft = draft_backgrounds.get(release_id)
        if not draft:
            return {
                "status": "NOT_READY",
                "checks": [
                    {
                        "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                        "status": "NOT_READY",
                        "message": "No ui_job_drafts visual assignment exists for linked release jobs.",
                        "remediation_hint": "Create or update ui_job_drafts with required background assignment.",
                    }
                ],
            }
        background_name = str(draft.get("background_name") or "").strip()
        background_ext = str(draft.get("background_ext") or "").strip()
        if bool(background_name) != bool(background_ext):
            return {
                "status": "BLOCKED",
                "checks": [
                    {
                        "code": "PRR_VISUAL_ASSIGNMENT_INVALID",
                        "status": "BLOCKED",
                        "message": "Background visual assignment is partially populated and inconsistent.",
                        "remediation_hint": "Set both background_name and background_ext or clear both before assigning valid values.",
                    }
                ],
            }
        if not background_name:
            return {
                "status": "NOT_READY",
                "checks": [
                    {
                        "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                        "status": "NOT_READY",
                        "message": "Required background visual assignment is missing.",
                        "remediation_hint": "Set background_name and background_ext in the linked ui_job_drafts record.",
                    }
                ],
            }
        return {
            "status": "READY",
            "checks": [
                {
                    "code": "PRR_VISUAL_ASSIGNMENT_MISSING",
                    "status": "PASS",
                    "message": "Required background visual assignment is present.",
                    "remediation_hint": "No action required.",
                }
            ],
        }

    def _make_check(
        self,
        *,
        code: str,
        domain: str,
        passed: bool,
        severity: Literal["NOT_READY", "BLOCKED"],
        message_fail: str,
        hint_fail: str,
    ) -> dict[str, Any]:
        if passed:
            return {
                "code": code,
                "status": "PASS",
                "message": f"{domain} check passed.",
                "remediation_hint": "No action required.",
            }
        return {
            "code": code,
            "status": severity,
            "message": message_fail,
            "remediation_hint": hint_fail,
        }

    def _domain_status(self, checks: list[dict[str, Any]]) -> DomainStatus:
        if any(check["status"] == "BLOCKED" for check in checks):
            return "BLOCKED"
        if checks and all(check["status"] == "PASS" for check in checks):
            return "READY"
        return "NOT_READY"

    def _resolve_aggregate_status(self, *, domains: dict[str, dict[str, Any]]) -> AggregateStatus:
        statuses = [domains[name]["status"] for name in _DOMAIN_ORDER]
        if any(status == "BLOCKED" for status in statuses):
            return "BLOCKED"
        if all(status == "READY" for status in statuses):
            return "READY_FOR_MATERIALIZATION"
        return "NOT_READY"

    def _flatten_reasons(self, *, domains: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
        reasons: list[dict[str, Any]] = []
        for domain in _DOMAIN_ORDER:
            for check in domains[domain]["checks"]:
                status = str(check.get("status") or "")
                if status not in {"NOT_READY", "BLOCKED"}:
                    continue
                reasons.append(
                    {
                        "code": check["code"],
                        "domain": domain,
                        "severity": status,
                        "message": check["message"],
                        "remediation_hint": check["remediation_hint"],
                    }
                )
        return reasons

    def _build_summary(self, *, domains: dict[str, dict[str, Any]]) -> dict[str, int]:
        counts = {"ready_domains": 0, "not_ready_domains": 0, "blocked_domains": 0}
        for domain in _DOMAIN_ORDER:
            status = domains[domain]["status"]
            if status == "READY":
                counts["ready_domains"] += 1
            elif status == "BLOCKED":
                counts["blocked_domains"] += 1
            else:
                counts["not_ready_domains"] += 1
        return counts

    def _select_primary_reason(self, *, reasons: list[dict[str, Any]]) -> dict[str, Any] | None:
        if not reasons:
            return None

        domain_rank = {name: idx for idx, name in enumerate(_DOMAIN_ORDER)}

        def _reason_key(reason: dict[str, Any]) -> tuple[int, int, int]:
            severity = str(reason.get("severity") or "")
            return (
                _REASON_SEVERITY_RANK.get(severity, 99),
                domain_rank.get(str(reason.get("domain") or ""), 99),
                reasons.index(reason),
            )

        primary = sorted(reasons, key=_reason_key)[0]
        return dict(primary)
