from __future__ import annotations

from collections import defaultdict

from services.playlist_builder.constraints import duration_band_sec, relaxed_brief_variants
from services.playlist_builder.history import novelty_against_previous, position_memory_risk
from services.playlist_builder.models import CandidateScore, PlaylistBrief, PlaylistHistoryEntry, TrackCandidate


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


def list_safe_candidates(conn: object, brief: PlaylistBrief) -> list[TrackCandidate]:
    rows = conn.execute(
        """
        SELECT
            t.id AS track_pk,
            t.track_id,
            t.channel_slug,
            COALESCE(taf.duration_sec, t.duration_sec) AS duration_sec,
            t.month_batch,
            taf.voice_flag,
            taf.speech_flag,
            taf.dominant_texture,
            taf.dsp_score,
            taf.yamnet_top_tags_text,
            GROUP_CONCAT(CASE WHEN tcta.state IN ('AUTO','MANUAL') THEN LOWER(ct.code) END) AS custom_tag_codes
        FROM tracks t
        JOIN track_analysis_flat taf ON taf.track_pk = t.id
        LEFT JOIN track_custom_tag_assignments tcta ON tcta.track_pk = t.id
        LEFT JOIN custom_tags ct ON ct.id = tcta.tag_id
        WHERE t.analyzed_at IS NOT NULL
          AND taf.analysis_status = 'ok'
          AND (? = 1 OR t.channel_slug = ?)
        GROUP BY t.id
        ORDER BY t.id ASC
        """,
        (1 if brief.allow_cross_channel else 0, brief.channel_slug),
    ).fetchall()

    required = {t.lower() for t in brief.required_tags}
    excluded = {t.lower() for t in brief.excluded_tags}
    result: list[TrackCandidate] = []
    for row in rows:
        dur = row["duration_sec"]
        if dur is None or float(dur) <= 0:
            continue
        tags = set()
        yamnet = str(row["yamnet_top_tags_text"] or "")
        tags.update(v.strip().lower() for v in yamnet.split(",") if v.strip())
        if row["dominant_texture"]:
            tags.add(str(row["dominant_texture"]).strip().lower())
        custom_csv = str(row["custom_tag_codes"] or "")
        tags.update(v.strip().lower() for v in custom_csv.split(",") if v.strip())
        norm_tags = _norm_tag_set(tags)
        if required and not required.issubset(norm_tags):
            continue
        if excluded and excluded & norm_tags:
            continue
        candidate = TrackCandidate(
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
        result.append(candidate)

    if brief.candidate_limit:
        return result[: max(0, int(brief.candidate_limit))]
    return result


def _reuse_penalty(track_pk: int, history: list[PlaylistHistoryEntry]) -> float:
    if not history:
        return 0.0
    used = sum(1 for h in history if track_pk in h.tracks)
    return used / len(history)


def score_candidates(brief: PlaylistBrief, candidates: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> list[CandidateScore]:
    prev_tracks = history[0].tracks if history else ()
    target_sec = brief.target_duration_min * 60.0
    scores: list[CandidateScore] = []
    for c in candidates:
        ok_voice, voice_fit = _voice_policy_fit(brief, c)
        context_fit = 1.0 if c.channel_slug == brief.channel_slug else 0.6
        novelty = novelty_against_previous([c.track_pk], prev_tracks)
        batch_fit = 1.0 if brief.preferred_month_batch and c.month_batch == brief.preferred_month_batch else 0.5
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


def compose_safe(brief: PlaylistBrief, candidates: list[TrackCandidate], history: list[PlaylistHistoryEntry]) -> tuple[list[TrackCandidate], list[CandidateScore], list[str]]:
    min_sec, target_sec, max_sec = duration_band_sec(brief)
    by_pk = {c.track_pk: c for c in candidates}
    relaxations: list[str] = []
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
        selected: list[TrackCandidate] = []
        total = 0.0
        for cand in ranked:
            sc = score_map[cand.track_pk]
            if not sc.hard_eligible:
                continue
            if total + cand.duration_sec > max_sec and total >= min_sec:
                continue
            selected.append(cand)
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
                relaxations.append(relaxation)

        if selected and min_sec <= total <= max_sec:
            if relaxation != "none":
                relaxations.append(relaxation)
            return selected, [score_map[c.track_pk] for c in selected], relaxations

    return best_selected, best_scores, relaxations


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
