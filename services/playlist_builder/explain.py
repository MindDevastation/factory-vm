from __future__ import annotations

from services.playlist_builder.composition import achieved_batch_ratio, achieved_novelty, annotate_fit_notes
from services.playlist_builder.models import CandidateScore, PlaylistBrief, PlaylistHistoryEntry, PlaylistPreviewResult, RelaxationItem, TrackCandidate


def build_preview_result(
    *,
    brief: PlaylistBrief,
    selected: list[TrackCandidate],
    ordered: list[TrackCandidate],
    scores: list[CandidateScore],
    history: list[PlaylistHistoryEntry],
    warnings: list[str],
    relaxations: list[RelaxationItem],
    ordering_rationale: str,
    candidate_pool_size: int,
    diagnostics: dict | None = None,
) -> PlaylistPreviewResult:
    duration_sec = sum(c.duration_sec for c in ordered)
    return PlaylistPreviewResult(
        mode=brief.generation_mode,
        status="ok" if ordered else "empty",
        warnings=warnings,
        relaxations=[item.relaxation_applied for item in relaxations],
        relaxations_structured=relaxations,
        selected_track_pks=[c.track_pk for c in selected],
        ordered_track_pks=[c.track_pk for c in ordered],
        achieved_duration_sec=round(duration_sec, 3),
        achieved_duration_min=round(duration_sec / 60.0, 3),
        achieved_novelty=round(achieved_novelty(selected, history), 4),
        achieved_batch_ratio=round(achieved_batch_ratio(selected, brief.preferred_month_batch), 4),
        per_track_fit_notes=annotate_fit_notes(selected, scores, history),
        ordering_rationale=ordering_rationale,
        candidate_pool_size=candidate_pool_size,
        diagnostics=diagnostics,
    )
