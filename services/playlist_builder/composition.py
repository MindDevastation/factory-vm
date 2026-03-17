from __future__ import annotations

from collections import defaultdict
from time import monotonic
import json
from dataclasses import dataclass

from services.playlist_builder.constraints import duration_band_sec, relaxed_brief_variants
from services.playlist_builder.history import novelty_against_previous, position_memory_risk
from services.playlist_builder.models import CandidateScore, PlaylistBrief, PlaylistHistoryEntry, RelaxationItem, TrackCandidate
from services.playlist_builder.tags import candidate_filter_tokens, normalize_filter_token


class CuratedOptimizationLimitExceeded(RuntimeError):
    pass


@dataclass
class CandidateExtractionMetrics:
    custom_tag_extraction_ms: float = 0.0
    yamnet_tag_extraction_ms: float = 0.0
    semantic_tag_extraction_ms: float = 0.0


DEFAULT_PREVIEW_CANDIDATE_LIMIT = 2000


def _norm_tag_set(tags: set[str]) -> frozenset[str]:
    return frozenset(t.strip().lower() for t in tags if str(t).strip())


def _voice_policy_fit(brief: PlaylistBrief, candidate: TrackCandidate) -> tuple[bool, float]:
    vp = brief.vocal_policy
    if vp == "allow_any":
        return True, 1.0
    voice = candidate.voice_flag
    speech = candidate.speech_flag
    if vp == "exclude_speech":
        if speech is None:
            return False, 0.0
        return (not speech), (1.0 if not speech else 0.0)
    if vp == "require_instrumental":
        if voice is None:
            return False, 0.0
        return (not voice), (1.0 if not voice else 0.0)
    if vp == "require_lyrical":
        if voice is None:
            return False, 0.0
        return bool(voice), (1.0 if voice else 0.0)
    if vp == "prefer_instrumental":
        return True, 1.0 if voice is False else 0.5
    if vp == "prefer_lyrical":
        return True, 1.0 if voice is True else 0.5
    return True, 0.5


def _base_candidate_rows(conn: object, brief: PlaylistBrief) -> list[dict]:
    return conn.execute(
        """
        SELECT
            t.id AS track_pk,
            t.track_id,
            t.channel_slug,
            COALESCE(taf.duration_sec, t.duration_sec) AS duration_sec,
            t.month_batch,
            taf.analysis_status,
            taf.voice_flag,
            taf.speech_flag,
            taf.dominant_texture,
            taf.dsp_score,
            taf.yamnet_top_tags_text,
            tt.payload_json AS tags_payload_json,
            GROUP_CONCAT(CASE WHEN tcta.state IN ('AUTO','MANUAL') THEN LOWER(ct.code) END) AS custom_tag_codes
        FROM tracks t
        LEFT JOIN track_analysis_flat taf ON taf.track_pk = t.id
        LEFT JOIN track_tags tt ON tt.track_pk = t.id
        LEFT JOIN track_custom_tag_assignments tcta ON tcta.track_pk = t.id
        LEFT JOIN custom_tags ct ON ct.id = tcta.tag_id
        WHERE (? = 1 OR t.channel_slug = ?)
        GROUP BY t.id
        ORDER BY t.id ASC
        """,
        (1 if brief.allow_cross_channel else 0, brief.channel_slug),
    ).fetchall()


def _extract_normalized_tags(row: dict, metrics: CandidateExtractionMetrics | None = None) -> frozenset[str]:
    tags = set()
    yamnet_started = monotonic()
    yamnet = str(row["yamnet_top_tags_text"] or "")
    yamnet_tags = [v.strip() for v in yamnet.split(",") if v.strip()]
    if metrics is not None:
        metrics.yamnet_tag_extraction_ms += (monotonic() - yamnet_started) * 1000.0

    semantic_tags: list[str] = []
    payload_text = row["tags_payload_json"]
    if payload_text:
        semantic_started = monotonic()
        try:
            payload = json.loads(str(payload_text))
        except json.JSONDecodeError:
            payload = {}
        semantic = ((payload.get("advanced_v1") or {}).get("semantic") or {})
        for key in ("mood_tags", "theme_tags"):
            semantic_tags.extend(str(v).strip() for v in (semantic.get(key) or []) if str(v).strip())
        if metrics is not None:
            metrics.semantic_tag_extraction_ms += (monotonic() - semantic_started) * 1000.0

    custom_started = monotonic()
    custom_csv = str(row["custom_tag_codes"] or "")
    custom_codes = [v.strip() for v in custom_csv.split(",") if v.strip()]
    if metrics is not None:
        metrics.custom_tag_extraction_ms += (monotonic() - custom_started) * 1000.0
    tags.update(candidate_filter_tokens(custom_codes=custom_codes, yamnet_tags=yamnet_tags, semantic_tags=semantic_tags))
    if row["dominant_texture"]:
        tags.add(str(row["dominant_texture"]).strip().lower())
    return _norm_tag_set(tags)


def build_candidate_diagnostics(conn: object, brief: PlaylistBrief) -> tuple[list[TrackCandidate], dict[str, int | float | bool]]:
    rows = _base_candidate_rows(conn, brief)
    required = {normalize_filter_token(t) for t in brief.required_tags if normalize_filter_token(t)}
    excluded = {normalize_filter_token(t) for t in brief.excluded_tags if normalize_filter_token(t)}

    initial_tracks = len(rows)
    after_channel_scope = initial_tracks

    analyzed_statuses = {"ok", "complete"}
    analyzed_rows = [r for r in rows if str(r.get("analysis_status") or "").strip().lower() in analyzed_statuses]
    after_analyzed_eligible = len(analyzed_rows)

    month_rows = analyzed_rows
    after_month_batch = len(month_rows)

    with_required: list[dict] = []
    with_excluded: list[dict] = []
    candidates: list[TrackCandidate] = []
    extraction_metrics = CandidateExtractionMetrics()

    for row in month_rows:
        dur = row["duration_sec"]
        if dur is None or float(dur) <= 0:
            continue
        norm_tags = _extract_normalized_tags(row, extraction_metrics)
        if required and not required.issubset(norm_tags):
            continue
        with_required.append(row)
        if excluded and excluded & norm_tags:
            continue
        with_excluded.append(row)
        candidates.append(
            TrackCandidate(
                track_pk=int(row["track_pk"]),
                track_id=str(row["track_id"]),
                channel_slug=str(row["channel_slug"]),
                duration_sec=float(dur),
                month_batch=str(row["month_batch"]) if row["month_batch"] else None,
                tags=norm_tags,
                voice_flag=None if row["voice_flag"] is None else bool(row["voice_flag"]),
                speech_flag=None if row["speech_flag"] is None else bool(row["speech_flag"]),
                dominant_texture=str(row["dominant_texture"]) if row["dominant_texture"] else None,
                dsp_score=float(row["dsp_score"]) if row["dsp_score"] is not None else None,
            )
        )

    effective_limit = brief.candidate_limit if brief.candidate_limit is not None else DEFAULT_PREVIEW_CANDIDATE_LIMIT
    trimmed = 0
    if effective_limit and effective_limit > 0:
        limit = max(0, int(effective_limit))
        if len(candidates) > limit:
            trimmed = len(candidates) - limit
            candidates = candidates[:limit]

    voice_eligible = sum(1 for c in candidates if _voice_policy_fit(brief, c)[0])

    diagnostics = {
        "initial_tracks": initial_tracks,
        "after_channel_scope": after_channel_scope,
        "after_analyzed_eligible": after_analyzed_eligible,
        "after_month_batch_preference_or_filter": after_month_batch,
        "after_required_tags": len(with_required),
        "after_excluded_tags": len(with_excluded),
        "after_vocal_policy": voice_eligible,
        "after_history_reuse": voice_eligible,
        "after_position_memory": voice_eligible,
        "final_candidates": len(candidates),
        "candidate_limit_applied": bool(effective_limit and effective_limit > 0),
        "candidate_limit_value": int(effective_limit) if effective_limit else 0,
        "candidate_limit_trimmed": trimmed,
        "custom_tag_extraction_ms": round(extraction_metrics.custom_tag_extraction_ms, 3),
        "yamnet_tag_extraction_ms": round(extraction_metrics.yamnet_tag_extraction_ms, 3),
        "semantic_tag_extraction_ms": round(extraction_metrics.semantic_tag_extraction_ms, 3),
    }
    return candidates, diagnostics


def list_safe_candidates(conn: object, brief: PlaylistBrief) -> list[TrackCandidate]:
    candidates, _ = build_candidate_diagnostics(conn, brief)
    return candidates


def _reuse_penalty(track_pk: int, history: list[PlaylistHistoryEntry]) -> float:
    if not history:
        return 0.0
    used = sum(1 for h in history if track_pk in h.tracks)
    return used / len(history)


def _target_batch_ratio(brief: PlaylistBrief) -> float:
    return max(0.0, min(1.0, brief.preferred_batch_ratio / 100.0))


def _batch_ratio_target_fit(brief: PlaylistBrief, achieved_ratio: float) -> float:
    if not brief.preferred_month_batch:
        return 0.5
    return max(0.0, 1.0 - abs(achieved_ratio - _target_batch_ratio(brief)))


def score_candidates(brief: PlaylistBrief, candidates: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> list[CandidateScore]:
    prev_tracks = history[0].tracks if history else ()
    target_sec = brief.target_duration_min * 60.0
    scores: list[CandidateScore] = []
    for c in candidates:
        ok_voice, voice_fit = _voice_policy_fit(brief, c)
        context_fit = 1.0 if c.channel_slug == brief.channel_slug else 0.6
        novelty = novelty_against_previous([c.track_pk], prev_tracks)
        batch_fit = _batch_ratio_target_fit(brief, 1.0 if brief.preferred_month_batch and c.month_batch == brief.preferred_month_batch else 0.0)
        req_fit = 1.0 if not brief.required_tags else len(set(brief.required_tags) & c.tags) / max(len(brief.required_tags), 1)
        reuse_pen = _reuse_penalty(c.track_pk, history)
        duration_fit = max(0.0, 1.0 - abs(c.duration_sec - (target_sec / 8.0)) / max(target_sec / 8.0, 1.0))
        base_fit = (
            (0.24 * context_fit)
            + (0.18 * novelty)
            + (0.16 * batch_fit)
            + (0.14 * voice_fit)
            + (0.12 * req_fit)
            + (0.10 * (1.0 - reuse_pen))
            + (0.06 * duration_fit)
        )
        scores.append(
            CandidateScore(
                track_pk=c.track_pk,
                hard_eligible=ok_voice,
                context_fit=context_fit,
                novelty_contribution=novelty,
                batch_ratio_contribution=batch_fit,
                voice_policy_fit=voice_fit,
                required_tags_fit=req_fit,
                low_reuse_penalty_inverse=(1.0 - reuse_pen),
                duration_fit_micro=duration_fit,
                base_fit=base_fit,
            )
        )
    return scores


def _relaxation_item(brief: PlaylistBrief, variant: PlaylistBrief, relaxation: str) -> RelaxationItem:
    if relaxation == "drop_preferred_month_batch":
        return RelaxationItem(constraint_name="preferred_month_batch", target_value=brief.preferred_month_batch, achieved_value=variant.preferred_month_batch, relaxation_applied=relaxation, reason="Preferred batch filter prevented valid composition within duration constraints.")
    if relaxation == "lower_novelty_target_min":
        return RelaxationItem(constraint_name="novelty_target_min", target_value=brief.novelty_target_min, achieved_value=variant.novelty_target_min, relaxation_applied=relaxation, reason="Novelty floor was softened to recover feasible candidates.")
    if relaxation == "relax_vocal_policy_allow_any":
        return RelaxationItem(constraint_name="vocal_policy", target_value=brief.vocal_policy, achieved_value=variant.vocal_policy, relaxation_applied=relaxation, reason="Vocal policy was softened in flexible mode to avoid an empty composition.")
    return RelaxationItem(constraint_name="unknown", target_value=None, achieved_value=None, relaxation_applied=relaxation, reason="Generic relaxation applied.")


def compose_safe(brief: PlaylistBrief, candidates: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> tuple[list[TrackCandidate], list[CandidateScore], list[RelaxationItem]]:
    min_sec, target_sec, max_sec = duration_band_sec(brief)
    by_pk = {c.track_pk: c for c in candidates}
    relaxations: list[RelaxationItem] = []
    best_selected: list[TrackCandidate] = []
    best_scores: list[CandidateScore] = []

    for variant, relaxation in relaxed_brief_variants(brief):
        scores = score_candidates(variant, candidates, history)
        score_map = {s.track_pk: s for s in scores}
        ranked = sorted(
            candidates,
            key=lambda c: (
                1 if score_map[c.track_pk].hard_eligible else 0,
                score_map[c.track_pk].novelty_contribution,
                score_map[c.track_pk].required_tags_fit,
                score_map[c.track_pk].voice_policy_fit,
                score_map[c.track_pk].batch_ratio_contribution,
                score_map[c.track_pk].low_reuse_penalty_inverse,
                score_map[c.track_pk].duration_fit_micro,
                score_map[c.track_pk].base_fit,
                -c.track_pk,
            ),
            reverse=True,
        )
        remaining = list(ranked)
        selected: list[TrackCandidate] = []
        total = 0.0
        while remaining:
            preferred_selected = sum(1 for c in selected if c.month_batch == variant.preferred_month_batch)
            def _pick_score(cand: TrackCandidate) -> tuple[float, float, float, float, float, float, float, float, int]:
                sc = score_map[cand.track_pk]
                if not sc.hard_eligible:
                    return (-1.0,) * 8 + (-cand.track_pk,)
                if total + cand.duration_sec > max_sec and total >= min_sec:
                    return (-1.0,) * 8 + (-cand.track_pk,)
                next_count = len(selected) + 1
                next_pref = preferred_selected + (1 if variant.preferred_month_batch and cand.month_batch == variant.preferred_month_batch else 0)
                next_ratio = next_pref / max(next_count, 1)
                ratio_fit = _batch_ratio_target_fit(variant, next_ratio)
                return (
                    ratio_fit,
                    sc.novelty_contribution,
                    sc.required_tags_fit,
                    sc.voice_policy_fit,
                    sc.batch_ratio_contribution,
                    sc.low_reuse_penalty_inverse,
                    sc.duration_fit_micro,
                    sc.base_fit,
                    -cand.track_pk,
                )
            cand = max(remaining, key=_pick_score)
            sc = score_map[cand.track_pk]
            if not sc.hard_eligible:
                break
            if total + cand.duration_sec > max_sec and total >= min_sec:
                remaining.remove(cand)
                continue
            selected.append(cand)
            remaining.remove(cand)
            total += cand.duration_sec
            if total >= target_sec:
                break

        if selected and (total < min_sec or total > max_sec):
            selected = _attempt_swaps(selected, ranked, target_sec)
            total = sum(c.duration_sec for c in selected)

        if selected and (not best_selected or abs(sum(c.duration_sec for c in selected) - target_sec) < abs(sum(c.duration_sec for c in best_selected) - target_sec)):
            best_selected = selected
            best_scores = [score_map[c.track_pk] for c in selected]
            if relaxation != "none":
                relaxations.append(_relaxation_item(brief, variant, relaxation))

        if selected and min_sec <= total <= max_sec:
            if relaxation != "none":
                relaxations.append(_relaxation_item(brief, variant, relaxation))
            return selected, [score_map[c.track_pk] for c in selected], relaxations

    return best_selected, best_scores, relaxations


def _selection_diversity(selected: list[TrackCandidate]) -> float:
    if len(selected) <= 1:
        return 0.5
    textures = {c.dominant_texture for c in selected if c.dominant_texture}
    batches = {c.month_batch for c in selected if c.month_batch}
    tags = {tag for c in selected for tag in c.tags}
    texture_ratio = len(textures) / max(len(selected), 1)
    batch_ratio = len(batches) / max(len(selected), 1)
    tag_ratio = len(tags) / max(len(selected) * 2, 1)
    return min(1.0, 0.45 * texture_ratio + 0.30 * batch_ratio + 0.25 * tag_ratio)


def _selection_objective(brief: PlaylistBrief, selected: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> float:
    if not selected:
        return -1.0
    target_sec = brief.target_duration_min * 60.0
    duration_sec = sum(c.duration_sec for c in selected)
    duration_fit = max(0.0, 1.0 - (abs(duration_sec - target_sec) / max(target_sec, 1.0)))
    novelty = achieved_novelty(selected, history)
    achieved_ratio = achieved_batch_ratio(selected, brief.preferred_month_batch)
    batch_fit = _batch_ratio_target_fit(brief, achieved_ratio)
    diversity = _selection_diversity(selected)
    context_fit = sum(1.0 if c.channel_slug == brief.channel_slug else 0.6 for c in selected) / len(selected)
    return (0.33 * duration_fit) + (0.23 * novelty) + (0.16 * batch_fit) + (0.16 * diversity) + (0.12 * context_fit)


def compose_smart(
    brief: PlaylistBrief,
    candidates: list[TrackCandidate],
    history: list[PlaylistHistoryEntry],
) -> tuple[list[TrackCandidate], list[CandidateScore], list[str], str]:
    initial_selected, initial_scores, relaxations = compose_safe(brief, candidates, history)
    if not initial_selected:
        return initial_selected, initial_scores, relaxations, "Smart mode did not run refinement because Safe composition yielded no seed set."

    scored = score_candidates(brief, candidates, history)
    score_map = {s.track_pk: s for s in scored}
    ranked_pool = sorted(
        candidates,
        key=lambda c: (
            score_map[c.track_pk].base_fit,
            score_map[c.track_pk].novelty_contribution,
            score_map[c.track_pk].context_fit,
            -c.track_pk,
        ),
        reverse=True,
    )
    top_k = min(len(ranked_pool), max(len(initial_selected) * 4, 12))
    pool = ranked_pool[:top_k]
    pool_by_pk = {c.track_pk: c for c in pool}
    current = [pool_by_pk.get(c.track_pk, c) for c in initial_selected]
    current_ids = {c.track_pk for c in current}

    min_sec, _, max_sec = duration_band_sec(brief)
    passes = 0
    improvements = 0
    improved = True
    while improved and passes < 3:
        improved = False
        passes += 1
        current_obj = _selection_objective(brief, current, history)
        for idx, out in enumerate(list(current)):
            for cand in pool:
                if cand.track_pk in current_ids:
                    continue
                proposal = list(current)
                proposal[idx] = cand
                proposal_duration = sum(c.duration_sec for c in proposal)
                if proposal_duration > max_sec + 1e-6:
                    continue
                if proposal_duration < min_sec and sum(c.duration_sec for c in current) >= min_sec:
                    continue
                proposal_obj = _selection_objective(brief, proposal, history)
                if proposal_obj > current_obj + 1e-6:
                    current = proposal
                    current_ids.remove(out.track_pk)
                    current_ids.add(cand.track_pk)
                    current_obj = proposal_obj
                    improvements += 1
                    improved = True

    final_scores = [score_map.get(c.track_pk) for c in current if score_map.get(c.track_pk)]
    summary = f"Smart composition used top-{top_k} pool with {passes} pass(es) and {improvements} accepted swap refinement(s)."
    return current, final_scores, relaxations, summary


def _curated_sequence_hint(ordered: list[TrackCandidate]) -> float:
    if len(ordered) <= 1:
        return 0.0
    smooth = 0.0
    for idx in range(1, len(ordered)):
        prev = ordered[idx - 1].dsp_score
        nxt = ordered[idx].dsp_score
        if prev is None or nxt is None:
            smooth += 0.5
        else:
            smooth += max(0.0, 1.0 - abs(prev - nxt))
    return smooth / max(len(ordered) - 1, 1)


def _curated_set_objective(
    brief: PlaylistBrief,
    selected: list[TrackCandidate],
    history: list[PlaylistHistoryEntry],
    ordered: list[TrackCandidate],
) -> float:
    return (0.88 * _selection_objective(brief, selected, history)) + (0.12 * _curated_sequence_hint(ordered))


def compose_curated(
    brief: PlaylistBrief,
    candidates: list[TrackCandidate],
    history: list[PlaylistHistoryEntry],
    *,
    max_wall_seconds: float = 1.0,
    max_iterations: int = 360,
) -> tuple[list[TrackCandidate], list[CandidateScore], list[str], str]:
    started = monotonic()
    if max_iterations < 1 or max_wall_seconds <= 0.0:
        raise CuratedOptimizationLimitExceeded("Curated optimization guardrail invalid; max_iterations and max_wall_seconds must be positive.")

    seed_selected, _, relaxations, smart_summary = compose_smart(brief, candidates, history)
    if not seed_selected:
        return seed_selected, [], relaxations, "Curated mode could not start optimization because Smart seed composition yielded no set."

    scored = score_candidates(brief, candidates, history)
    score_map = {s.track_pk: s for s in scored}
    ranked_pool = sorted(
        candidates,
        key=lambda c: (score_map[c.track_pk].base_fit, score_map[c.track_pk].novelty_contribution, -c.track_pk),
        reverse=True,
    )
    top_k = min(len(ranked_pool), max(len(seed_selected) * 6, 18))
    pool = ranked_pool[:top_k]
    by_pk = {c.track_pk: c for c in pool}
    min_sec, _, max_sec = duration_band_sec(brief)

    seeds = [list(seed_selected)]
    for start in (1, 2):
        alt = [by_pk.get(c.track_pk, c) for c in seed_selected]
        for idx in range(start, len(alt), 2):
            for cand in pool:
                if cand.track_pk in {t.track_pk for t in alt}:
                    continue
                trial = list(alt)
                trial[idx] = cand
                dur = sum(c.duration_sec for c in trial)
                if min_sec <= dur <= max_sec:
                    alt = trial
                    break
        seeds.append(alt)

    best = list(seed_selected)
    best_obj = _curated_set_objective(brief, best, history, best)
    improvements = 0
    iterations = 0
    for current in seeds:
        current_ids = {c.track_pk for c in current}
        improved = True
        while improved:
            if monotonic() - started > max_wall_seconds:
                raise CuratedOptimizationLimitExceeded(
                    f"Curated composition exceeded guardrail: max_wall_seconds={max_wall_seconds:.2f}, max_iterations={max_iterations}, iterations={iterations}."
                )
            if iterations >= max_iterations:
                raise CuratedOptimizationLimitExceeded(
                    f"Curated composition exceeded guardrail: max_iterations={max_iterations}, elapsed={monotonic() - started:.3f}s."
                )
            improved = False
            iterations += 1
            current_obj = _curated_set_objective(brief, current, history, current)
            for idx, out in enumerate(list(current)):
                for cand in pool:
                    if cand.track_pk in current_ids:
                        continue
                    proposal = list(current)
                    proposal[idx] = cand
                    proposal_duration = sum(c.duration_sec for c in proposal)
                    if proposal_duration < min_sec - 1e-6 or proposal_duration > max_sec + 1e-6:
                        continue
                    proposal_obj = _curated_set_objective(brief, proposal, history, proposal)
                    if proposal_obj > current_obj + 1e-6:
                        current_ids.discard(out.track_pk)
                        current_ids.add(cand.track_pk)
                        current = proposal
                        current_obj = proposal_obj
                        improvements += 1
                        improved = True
            if current_obj > best_obj + 1e-6:
                best = list(current)
                best_obj = current_obj

    final_scores = [score_map[c.track_pk] for c in best if c.track_pk in score_map]
    summary = (
        f"Curated composition ran seeded best-of-{len(seeds)} search over top-{top_k} candidates; "
        f"iterations={iterations}, accepted replacements={improvements}. {smart_summary}"
    )
    return best, final_scores, relaxations, summary


def _attempt_swaps(selected: list[TrackCandidate], ranked: list[TrackCandidate], target_sec: float) -> list[TrackCandidate]:
    current = list(selected)
    curr_err = abs(sum(c.duration_sec for c in current) - target_sec)
    for out_idx, out in enumerate(list(current)):
        current_ids = {c.track_pk for c in current}
        available = [c for c in ranked if c.track_pk not in current_ids]
        for inc in available[:12]:
            proposal = list(current)
            proposal[out_idx] = inc
            if len({c.track_pk for c in proposal}) != len(proposal):
                continue
            err = abs(sum(c.duration_sec for c in proposal) - target_sec)
            if err + 0.001 < curr_err:
                current = proposal
                curr_err = err
    return current


def achieved_batch_ratio(selected: list[TrackCandidate], preferred_month_batch: str | None) -> float:
    if not selected or not preferred_month_batch:
        return 0.0
    count = sum(1 for s in selected if s.month_batch == preferred_month_batch)
    return count / len(selected)


def achieved_novelty(selected: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> float:
    if not selected:
        return 0.0
    prev = history[0].tracks if history else ()
    return novelty_against_previous([s.track_pk for s in selected], prev)


def annotate_fit_notes(candidates: list[TrackCandidate], scores: list[CandidateScore], history: list[PlaylistHistoryEntry]) -> list[dict]:
    score_map = {s.track_pk: s for s in scores}
    out: list[dict] = []
    for c in candidates:
        s = score_map.get(c.track_pk)
        if not s:
            continue
        out.append(
            {
                "track_pk": c.track_pk,
                "base_fit": round(s.base_fit, 4),
                "note": f"ctx={s.context_fit:.2f} novelty={s.novelty_contribution:.2f} reuse_safe={s.low_reuse_penalty_inverse:.2f} pos_mem={1.0-position_memory_risk(c.track_pk, 0, history):.2f}",
            }
        )
    return out
