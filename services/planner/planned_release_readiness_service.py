from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
