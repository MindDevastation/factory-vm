from __future__ import annotations

from services.playlist_builder.models import PlaylistBrief


def relaxed_brief_variants(brief: PlaylistBrief) -> list[tuple[PlaylistBrief, str]]:
    variants: list[tuple[PlaylistBrief, str]] = [(brief, "none")]
    if brief.strictness_mode == "strict":
        return variants

    if brief.preferred_month_batch:
        variants.append((brief.model_copy(update={"preferred_month_batch": None}), "drop_preferred_month_batch"))

    lowered_novelty = max(0.0, brief.novelty_target_min - (0.10 if brief.strictness_mode == "balanced" else 0.20))
    variants.append((brief.model_copy(update={"novelty_target_min": lowered_novelty}), "lower_novelty_target_min"))

    if brief.strictness_mode == "flexible":
        variants.append((brief.model_copy(update={"vocal_policy": "allow_any"}), "relax_vocal_policy_allow_any"))
    return variants


def duration_band_sec(brief: PlaylistBrief) -> tuple[float, float, float]:
    min_sec = max(0.0, (brief.min_duration_min - brief.tolerance_min) * 60.0)
    max_sec = (brief.max_duration_min + brief.tolerance_min) * 60.0
    target_sec = brief.target_duration_min * 60.0
    return min_sec, target_sec, max_sec
