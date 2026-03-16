from __future__ import annotations

from typing import Any

from services.common import db as dbm
from services.playlist_builder.api_adapter import channel_settings_row_to_patch, parse_override_json, resolve_playlist_brief
from services.playlist_builder.composition import CuratedOptimizationLimitExceeded, build_candidate_diagnostics, compose_curated, compose_safe, compose_smart
from services.playlist_builder.explain import build_preview_result
from services.playlist_builder.history import list_effective_history
from services.playlist_builder.models import PlaylistBrief, PlaylistPreviewResult
from services.playlist_builder.sequencing import CuratedSequencingLimitExceeded, sequence_curated, sequence_safe, sequence_smart


class CuratedModeLimitExceeded(RuntimeError):
    pass


class PlaylistBuilder:
    @staticmethod
    def _reason_for_empty(brief: PlaylistBrief, diagnostics: dict[str, int], warnings: list[str]) -> str:
        if diagnostics.get("after_analyzed_eligible", 0) == 0:
            return "No analyzed eligible tracks found for this channel"
        if diagnostics.get("after_required_tags", 0) == 0:
            return "No candidates remained after required tags filtering"
        if diagnostics.get("after_excluded_tags", 0) == 0:
            return "No candidates remained after excluded tags filtering"
        if diagnostics.get("after_vocal_policy", 0) == 0:
            return "No candidates remained after vocal policy filtering"
        if diagnostics.get("final_candidates", 0) == 0:
            return "No candidates remained after candidate filtering"
        if warnings and any("composition could satisfy" in item.lower() for item in warnings):
            return "No valid playlist could satisfy duration constraints"
        return "No valid playlist could be composed"

    @staticmethod
    def _preview_diagnostics(brief: PlaylistBrief, diagnostics: dict[str, int], reason: str) -> dict[str, Any]:
        return {
            **diagnostics,
            "resolved_channel_slug": brief.channel_slug,
            "resolved_month_batch": brief.preferred_month_batch,
            "resolved_generation_mode": brief.generation_mode,
            "resolved_strictness_mode": brief.strictness_mode,
            "resolved_min_duration_min": brief.min_duration_min,
            "resolved_max_duration_min": brief.max_duration_min,
            "resolved_tolerance_min": brief.tolerance_min,
            "reason": reason,
        }

    def generate_preview(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        if brief.generation_mode == "safe":
            return self._generate_safe(conn, brief)
        if brief.generation_mode == "smart":
            return self._generate_smart(conn, brief)
        if brief.generation_mode == "curated":
            return self._generate_curated(conn, brief)
        return PlaylistPreviewResult(
            mode=brief.generation_mode,
            status="not_implemented",
            warnings=[f"generation_mode={brief.generation_mode} is not implemented in this slice"],
            ordering_rationale="No sequencing executed because mode logic is a placeholder.",
        )

    def _generate_safe(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        history = list_effective_history(conn, channel_slug=brief.channel_slug, window=brief.position_memory_window)
        candidates, diagnostics = build_candidate_diagnostics(conn, brief)
        candidate_pool_size = len(candidates)
        warnings: list[str] = []
        if not candidates:
            warnings.append("No eligible analyzed candidates found for safe mode.")
            reason = self._reason_for_empty(brief, diagnostics, warnings)
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=[],
                ordering_rationale="No candidates passed P0 safe filters.",
                candidate_pool_size=candidate_pool_size,
                diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
            )

        selected, scores, relaxations = compose_safe(brief, candidates, history)
        if not selected:
            warnings.append("No composition could satisfy hard eligibility constraints.")
            reason = self._reason_for_empty(brief, diagnostics, warnings)
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=relaxations,
                ordering_rationale="No candidates could be selected after greedy composition.",
                candidate_pool_size=candidate_pool_size,
                diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
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
            candidate_pool_size=candidate_pool_size,
            diagnostics=self._preview_diagnostics(brief, diagnostics, "ok"),
        )

    def _generate_smart(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        history = list_effective_history(conn, channel_slug=brief.channel_slug, window=brief.position_memory_window)
        candidates, diagnostics = build_candidate_diagnostics(conn, brief)
        candidate_pool_size = len(candidates)
        warnings: list[str] = []
        if not candidates:
            warnings.append("No eligible analyzed candidates found for smart mode.")
            reason = self._reason_for_empty(brief, diagnostics, warnings)
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=[],
                ordering_rationale="No candidates passed Smart mode candidate filtering.",
                candidate_pool_size=candidate_pool_size,
                diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
            )

        selected, scores, relaxations, composition_summary = compose_smart(brief, candidates, history)
        if not selected:
            warnings.append("No composition could satisfy hard eligibility constraints.")
            reason = self._reason_for_empty(brief, diagnostics, warnings)
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=relaxations,
                ordering_rationale="No candidates could be selected after Smart composition passes.",
                candidate_pool_size=candidate_pool_size,
                diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
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
            candidate_pool_size=candidate_pool_size,
            diagnostics=self._preview_diagnostics(brief, diagnostics, "ok"),
        )

    def _generate_curated(self, conn: object, brief: PlaylistBrief) -> PlaylistPreviewResult:
        history = list_effective_history(conn, channel_slug=brief.channel_slug, window=brief.position_memory_window)
        candidates, diagnostics = build_candidate_diagnostics(conn, brief)
        candidate_pool_size = len(candidates)
        warnings: list[str] = []
        if not candidates:
            warnings.append("No eligible analyzed candidates found for curated mode.")
            reason = self._reason_for_empty(brief, diagnostics, warnings)
            return build_preview_result(
                brief=brief,
                selected=[],
                ordered=[],
                scores=[],
                history=history,
                warnings=warnings,
                relaxations=[],
                ordering_rationale="No candidates passed Curated mode candidate filtering.",
                candidate_pool_size=candidate_pool_size,
                diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
            )
        try:
            selected, scores, relaxations, composition_summary = compose_curated(brief, candidates, history)
            if not selected:
                warnings.append("No composition could satisfy hard eligibility constraints.")
                reason = self._reason_for_empty(brief, diagnostics, warnings)
                return build_preview_result(
                    brief=brief,
                    selected=[],
                    ordered=[],
                    scores=[],
                    history=history,
                    warnings=warnings,
                    relaxations=relaxations,
                    ordering_rationale="No candidates could be selected after Curated composition passes.",
                    candidate_pool_size=candidate_pool_size,
                    diagnostics=self._preview_diagnostics(brief, diagnostics, reason),
                )
            ordered, rationale = sequence_curated(brief, selected, history)
        except (CuratedOptimizationLimitExceeded, CuratedSequencingLimitExceeded) as exc:
            raise CuratedModeLimitExceeded(str(exc)) from exc

        warnings.append(composition_summary)
        return build_preview_result(
            brief=brief,
            selected=selected,
            ordered=ordered,
            scores=scores,
            history=history,
            warnings=warnings,
            relaxations=relaxations,
            ordering_rationale=(
                f"{rationale} Curated mode performed iterative composition/sequencing refinement with bounded best-of-N and beam-like search."
            ),
            candidate_pool_size=candidate_pool_size,
            diagnostics=self._preview_diagnostics(brief, diagnostics, "ok"),
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
