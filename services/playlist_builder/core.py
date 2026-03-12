from __future__ import annotations

from typing import Any

from services.common import db as dbm
from services.playlist_builder.api_adapter import channel_settings_row_to_patch, parse_override_json, resolve_playlist_brief
from services.playlist_builder.composition import compose_safe, compose_smart, list_safe_candidates
from services.playlist_builder.explain import build_preview_result
from services.playlist_builder.history import list_effective_history
from services.playlist_builder.models import PlaylistBrief, PlaylistPreviewResult
from services.playlist_builder.sequencing import sequence_safe, sequence_smart


class PlaylistBuilder:
    def generate_preview(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        if brief.generation_mode == "safe":
            return self._generate_safe(conn, brief)
        if brief.generation_mode == "smart":
            return self._generate_smart(conn, brief)
        return PlaylistPreviewResult(
            mode=brief.generation_mode,
            status="not_implemented",
            warnings=[f"generation_mode={brief.generation_mode} is not implemented in this slice"],
            ordering_rationale="No sequencing executed because mode logic is a placeholder.",
        )

    def _generate_safe(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        history = list_effective_history(conn, channel_slug=brief.channel_slug, window=brief.position_memory_window)
        candidates = list_safe_candidates(conn, brief)
        warnings: list[str] = []
        if not candidates:
            warnings.append("No eligible analyzed candidates found for safe mode.")
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=[],
                ordering_rationale="No candidates passed P0 safe filters.",
            )

        selected, scores, relaxations = compose_safe(brief, candidates, history)
        if not selected:
            warnings.append("No composition could satisfy hard eligibility constraints.")
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=relaxations,
                ordering_rationale="No candidates could be selected after greedy composition.",
            )
        ordered, rationale = sequence_safe(brief, selected, history)
        return build_preview_result(
            brief=brief,
            selected=selected,
            ordered=ordered,
            scores=scores,
            history=history,
            warnings=warnings,
            relaxations=relaxations,
            ordering_rationale=rationale,
        )

    def _generate_smart(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        history = list_effective_history(conn, channel_slug=brief.channel_slug, window=brief.position_memory_window)
        candidates = list_safe_candidates(conn, brief)
        warnings: list[str] = []
        if not candidates:
            warnings.append("No eligible analyzed candidates found for smart mode.")
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=[],
                ordering_rationale="No candidates passed Smart mode candidate filtering.",
            )

        selected, scores, relaxations, composition_summary = compose_smart(brief, candidates, history)
        if not selected:
            warnings.append("No composition could satisfy hard eligibility constraints.")
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=relaxations,
                ordering_rationale="No candidates could be selected after Smart composition passes.",
            )

        ordered, rationale = sequence_smart(brief, selected, history)
        warnings.append(composition_summary)
        return build_preview_result(
            brief=brief,
            selected=selected,
            ordered=ordered,
            scores=scores,
            history=history,
            warnings=warnings,
            relaxations=relaxations,
            ordering_rationale=f"{rationale} Smart mode applied post-selection and post-ordering local refinement beyond Safe.",
        )


def resolve_effective_brief_for_job(conn: object, *, job_id: int, request_override: dict[str, Any] | None = None) -> PlaylistBrief:
    draft = dbm.get_ui_job_draft(conn, job_id)
    if not draft:
        raise ValueError(f"ui_job_draft not found for job_id={job_id}")
    job = dbm.get_job(conn, job_id)
    if not job:
        raise ValueError(f"job not found for job_id={job_id}")
    channel_slug = str(job["channel_slug"])
    settings_row = dbm.get_playlist_builder_channel_settings(conn, channel_slug)
    job_override = parse_override_json(draft.get("playlist_builder_override_json"))
    return resolve_playlist_brief(
        channel_slug=channel_slug,
        job_id=job_id,
        channel_settings=channel_settings_row_to_patch(settings_row),
        job_override=job_override,
        request_override=request_override or {},
    )


def prepare_generation_inputs(brief: PlaylistBrief) -> None:
    _ = brief
    return None
