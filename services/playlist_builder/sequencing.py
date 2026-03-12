from __future__ import annotations

from services.playlist_builder.history import position_memory_risk
from services.playlist_builder.models import PlaylistBrief, PlaylistHistoryEntry, TrackCandidate


def _transition_compatibility(a: TrackCandidate, b: TrackCandidate) -> float:
    if a.dsp_score is None or b.dsp_score is None:
        return 0.5
    return max(0.0, 1.0 - abs(a.dsp_score - b.dsp_score))


def _energy_progression_score(a: TrackCandidate, b: TrackCandidate) -> float:
    if a.dsp_score is None or b.dsp_score is None:
        return 0.5
    if b.dsp_score >= a.dsp_score:
        return 1.0
    return max(0.0, 1.0 - (a.dsp_score - b.dsp_score))


def _tonal_or_texture_continuity(a: TrackCandidate, b: TrackCandidate) -> float:
    if not a.dominant_texture or not b.dominant_texture:
        return 0.5
    return 1.0 if a.dominant_texture == b.dominant_texture else 0.4


def _position_fit_for_target_slot(candidate: TrackCandidate, slot: int, total: int) -> float:
    if total <= 1:
        return 1.0
    if slot == 0:
        return 1.0 if not candidate.speech_flag else 0.4
    if slot >= total - 1:
        return 1.0 if (candidate.dsp_score or 0.5) >= 0.4 else 0.6
    return 0.7


def _diversity_bonus(a: TrackCandidate, b: TrackCandidate) -> float:
    score = 0.0
    if a.month_batch and b.month_batch and a.month_batch != b.month_batch:
        score += 0.5
    if a.dominant_texture and b.dominant_texture and a.dominant_texture != b.dominant_texture:
        score += 0.5
    return min(1.0, score)


def sequence_safe(brief: PlaylistBrief, selected: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> tuple[list[TrackCandidate], str]:
    if len(selected) <= 1:
        return selected, "Ordering trivial due to single-track or empty selection."

    remaining = list(selected)
    total = len(remaining)
    first = max(
        remaining,
        key=lambda c: (
            _position_fit_for_target_slot(c, 0, total),
            1.0 - position_memory_risk(c.track_pk, 0, history),
            -(c.dsp_score or 0.5),
            -c.track_pk,
        ),
    )
    sequence = [first]
    remaining.remove(first)

    end_candidate = max(remaining, key=lambda c: (_position_fit_for_target_slot(c, total - 1, total), c.dsp_score or 0.0, -c.track_pk))

    while remaining:
        slot = len(sequence)
        current = sequence[-1]
        pool = [c for c in remaining if not (len(remaining) > 1 and c.track_pk == end_candidate.track_pk)]
        if not pool:
            pool = remaining
        nxt = max(
            pool,
            key=lambda b: (
                0.28 * _transition_compatibility(current, b)
                + 0.20 * _energy_progression_score(current, b)
                + 0.18 * _tonal_or_texture_continuity(current, b)
                + 0.16 * _position_fit_for_target_slot(b, slot, total)
                + 0.10 * _diversity_bonus(current, b)
                + 0.08 * (1.0 - position_memory_risk(b.track_pk, slot, history)),
                -b.track_pk,
            ),
        )
        sequence.append(nxt)
        remaining.remove(nxt)

    rationale = "Greedy pair_score sequencing with low position-memory risk; strongest ending-fit track reserved near the end when possible."
    return sequence, rationale


def _abrupt_jump_penalty(a: TrackCandidate, b: TrackCandidate) -> float:
    if a.dsp_score is None or b.dsp_score is None:
        return 0.2
    return 1.0 if abs(a.dsp_score - b.dsp_score) > 0.35 else 0.0


def _sequence_objective(brief: PlaylistBrief, ordered: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> float:
    if len(ordered) <= 1:
        return 1.0
    smooth = 0.0
    progression = 0.0
    jump_pen = 0.0
    diversity = 0.0
    for idx in range(1, len(ordered)):
        a = ordered[idx - 1]
        b = ordered[idx]
        smooth += _transition_compatibility(a, b)
        progression += _energy_progression_score(a, b)
        jump_pen += _abrupt_jump_penalty(a, b)
        diversity += _diversity_bonus(a, b)
    denom = max(len(ordered) - 1, 1)
    smooth /= denom
    progression /= denom
    jump_pen /= denom
    diversity /= denom

    start_end = (_position_fit_for_target_slot(ordered[0], 0, len(ordered)) + _position_fit_for_target_slot(ordered[-1], len(ordered) - 1, len(ordered))) / 2.0
    pos_memory = sum(1.0 - position_memory_risk(c.track_pk, i, history) for i, c in enumerate(ordered)) / len(ordered)
    return (0.30 * smooth) + (0.20 * progression) + (0.16 * start_end) + (0.14 * pos_memory) + (0.12 * diversity) - (0.20 * jump_pen)


def sequence_smart(brief: PlaylistBrief, selected: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> tuple[list[TrackCandidate], str]:
    if len(selected) <= 1:
        return selected, "Smart ordering is trivial due to single-track or empty selection."

    remaining = sorted(selected, key=lambda c: c.track_pk)
    total = len(remaining)
    seed = max(
        remaining,
        key=lambda c: (
            _position_fit_for_target_slot(c, 0, total),
            1.0 - position_memory_risk(c.track_pk, 0, history),
            -(c.dsp_score or 0.5),
            -c.track_pk,
        ),
    )
    ordered = [seed]
    remaining.remove(seed)

    while remaining:
        slot = len(ordered)
        current = ordered[-1]
        nxt = max(
            remaining,
            key=lambda cand: (
                0.30 * _transition_compatibility(current, cand)
                + 0.20 * _energy_progression_score(current, cand)
                + 0.18 * _position_fit_for_target_slot(cand, slot, total)
                + 0.12 * _diversity_bonus(current, cand)
                + 0.10 * (1.0 - position_memory_risk(cand.track_pk, slot, history))
                - 0.10 * _abrupt_jump_penalty(current, cand),
                -cand.track_pk,
            ),
        )
        ordered.append(nxt)
        remaining.remove(nxt)

    base_obj = _sequence_objective(brief, ordered, history)
    accepted = 0
    for _ in range(2):
        changed = False
        for idx in range(1, len(ordered) - 1):
            upper = min(len(ordered) - 1, idx + 3)
            for jdx in range(idx + 1, upper + 1):
                proposal = list(ordered)
                proposal[idx], proposal[jdx] = proposal[jdx], proposal[idx]
                score = _sequence_objective(brief, proposal, history)
                if score > base_obj + 1e-6:
                    ordered = proposal
                    base_obj = score
                    accepted += 1
                    changed = True
        if not changed:
            break

    rationale = (
        "Smart greedy seed sequencing with bounded local reorder refinement, "
        f"abrupt-jump penalties, and {accepted} accepted local reorder swap(s)."
    )
    return ordered, rationale
