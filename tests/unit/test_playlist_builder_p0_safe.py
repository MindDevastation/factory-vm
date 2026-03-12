from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from services.common import db as dbm
from services.playlist_builder.composition import CuratedOptimizationLimitExceeded, _attempt_swaps, compose_curated, compose_safe, compose_smart, list_safe_candidates
from services.playlist_builder.constraints import relaxed_brief_variants
from services.playlist_builder.history import (
    batch_distribution_overlap,
    list_effective_history,
    novelty_against_previous,
    ordered_sequence_overlap,
    position_memory_risk,
    prefix_overlap,
    track_set_overlap,
)
from services.playlist_builder.models import PlaylistBrief, TrackCandidate
from services.playlist_builder.sequencing import CuratedSequencingLimitExceeded, sequence_curated, sequence_safe, sequence_smart
from tests._helpers import temp_env, seed_minimal_db


class PlaylistBuilderP0SafeTest(unittest.TestCase):
    def setUp(self) -> None:
        self._ctx = temp_env()
        _, self.env = self._ctx.__enter__()
        seed_minimal_db(self.env)
        self.conn = dbm.connect(self.env)
        dbm.migrate(self.conn)

    def tearDown(self) -> None:
        self.conn.close()
        self._ctx.__exit__(None, None, None)

    def _insert_track(self, *, pk: int, track_id: str, channel_slug: str = "darkwood-reverie", duration: float = 180.0, batch: str = "2024-01", voice: int = 0, speech: int = 0, dsp: float = 0.5, tags: str = "calm,ambient", texture: str = "smooth") -> None:
        ts = dbm.now_ts()
        self.conn.execute(
            "INSERT INTO tracks(id, channel_slug, track_id, gdrive_file_id, duration_sec, month_batch, discovered_at, analyzed_at) VALUES(?,?,?,?,?,?,?,?)",
            (pk, channel_slug, track_id, f"g{pk}", duration, batch, ts, ts),
        )
        self.conn.execute(
            """
            INSERT INTO track_analysis_flat(
                track_pk, channel_slug, track_id, analysis_computed_at, analysis_status,
                duration_sec, yamnet_top_tags_text, voice_flag, speech_flag, dominant_texture,
                dsp_score, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (pk, channel_slug, track_id, ts, "ok", duration, tags, voice, speech, texture, dsp, datetime.utcnow().isoformat()),
        )

    def test_overlap_formulas(self) -> None:
        self.assertAlmostEqual(track_set_overlap([1, 2], [2, 3]), 1 / 3)
        self.assertAlmostEqual(novelty_against_previous([1, 2], [2, 3]), 0.5)
        self.assertAlmostEqual(ordered_sequence_overlap([1, 2, 3], [1, 4, 3]), 2 / 3)
        self.assertAlmostEqual(prefix_overlap([1, 2, 3], [1, 2, 4], 3), 2 / 3)
        self.assertAlmostEqual(batch_distribution_overlap(["a", "a", "b"], ["a", "c"]), 0.5)

    def test_history_precedence_and_position_memory(self) -> None:
        now = datetime.utcnow()
        self.conn.execute(
            "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (1, "darkwood-reverie", 99, "DRAFT", None, "safe", "balanced", 100, 2, "a", "b", "c", "d", 0.5, 0.5, 1, (now - timedelta(minutes=1)).isoformat()),
        )
        self.conn.execute(
            "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (2, "darkwood-reverie", 99, "COMMITTED", None, "safe", "balanced", 100, 2, "a", "b", "c", "d", 0.5, 0.5, 1, now.isoformat()),
        )
        self.conn.execute("INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?,?)", (1, 1, 0, 123, "2024-01", 10, "darkwood-reverie"))
        self.conn.execute("INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?,?)", (2, 2, 0, 456, "2024-01", 10, "darkwood-reverie"))
        hist = list_effective_history(self.conn, channel_slug="darkwood-reverie", window=20)
        self.assertEqual(len(hist), 1)
        self.assertEqual(hist[0].tracks[0], 456)
        self.assertEqual(position_memory_risk(456, 0, hist), 1.0)

    def test_swap_improvement_keeps_unique_track_pks(self) -> None:
        selected = [
            TrackCandidate(1, "t1", "darkwood-reverie", 200.0, "2024-01", frozenset(), False, False, "smooth", 0.1),
            TrackCandidate(2, "t2", "darkwood-reverie", 200.0, "2024-01", frozenset(), False, False, "smooth", 0.1),
        ]
        ranked = [
            selected[0],
            selected[1],
            TrackCandidate(3, "t3", "darkwood-reverie", 150.0, "2024-01", frozenset(), False, False, "smooth", 0.1),
        ]

        swapped = _attempt_swaps(selected, ranked, target_sec=300.0)

        self.assertEqual([t.track_pk for t in swapped], [3, 2])
        self.assertEqual(len({t.track_pk for t in swapped}), len(swapped))

    def test_history_precedence_applies_before_window_limit(self) -> None:
        now = datetime.utcnow()

        # Newer rows flood the old prefetch window (window=2 -> old prefetch=6).
        for idx in range(1, 6):
            self.conn.execute(
                "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (idx, "darkwood-reverie", 200, "DRAFT", None, "safe", "balanced", 100, 1, "a", "b", "c", "d", 0.5, 0.5, 1, (now - timedelta(seconds=idx)).isoformat()),
            )

        self.conn.execute(
            "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (6, "darkwood-reverie", 300, "DRAFT", None, "safe", "balanced", 100, 1, "a", "b", "c", "d", 0.5, 0.5, 1, (now - timedelta(seconds=6)).isoformat()),
        )
        self.conn.execute(
            "INSERT INTO playlist_history(id, channel_slug, job_id, history_stage, source_preview_id, generation_mode, strictness_mode, playlist_duration_sec, tracks_count, set_fingerprint, ordered_fingerprint, prefix_fingerprint_n3, prefix_fingerprint_n5, novelty_against_prev, batch_overlap_score, is_active, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (7, "darkwood-reverie", 300, "COMMITTED", None, "safe", "balanced", 100, 1, "a", "b", "c", "d", 0.5, 0.5, 1, (now - timedelta(seconds=7)).isoformat()),
        )

        self.conn.execute("INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?,?)", (1, 6, 0, 111, "2024-01", 10, "darkwood-reverie"))
        self.conn.execute("INSERT INTO playlist_history_items(id, history_id, position_index, track_pk, month_batch, duration_sec, channel_slug) VALUES(?,?,?,?,?,?,?)", (2, 7, 0, 999, "2024-01", 10, "darkwood-reverie"))

        hist = list_effective_history(self.conn, channel_slug="darkwood-reverie", window=2)

        target = next(h for h in hist if h.job_id == 300)
        self.assertEqual(target.history_stage, "COMMITTED")
        self.assertEqual(target.tracks, (999,))

    def test_relaxation_engine_variants(self) -> None:
        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="safe", strictness_mode="flexible", preferred_month_batch="2024-02", required_tags=["calm"])
        labels = [label for _, label in relaxed_brief_variants(brief)]
        self.assertIn("drop_preferred_month_batch", labels)
        self.assertIn("lower_novelty_target_min", labels)
        self.assertIn("relax_vocal_policy_allow_any", labels)
        self.assertIn("drop_required_tags", labels)

    def test_safe_composition_and_sequencing(self) -> None:
        self._insert_track(pk=1, track_id="t1", duration=210, batch="2024-01", voice=0, tags="ambient,calm", dsp=0.2)
        self._insert_track(pk=2, track_id="t2", duration=220, batch="2024-01", voice=0, tags="ambient,calm", dsp=0.4)
        self._insert_track(pk=3, track_id="t3", duration=230, batch="2024-02", voice=1, tags="ambient,tense", dsp=0.7)
        self._insert_track(pk=4, track_id="t4", channel_slug="channel-b", duration=240, batch="2024-02", voice=0, tags="ambient,calm", dsp=0.6)
        brief = PlaylistBrief(
            channel_slug="darkwood-reverie",
            generation_mode="safe",
            strictness_mode="balanced",
            min_duration_min=10,
            max_duration_min=14,
            tolerance_min=1,
            required_tags=["calm"],
            preferred_month_batch="2024-01",
            vocal_policy="require_instrumental",
        )
        candidates = list_safe_candidates(self.conn, brief)
        self.assertEqual({c.track_pk for c in candidates}, {1, 2})
        selected, _, _ = compose_safe(brief, candidates, history=[])
        self.assertGreaterEqual(len(selected), 2)
        ordered, rationale = sequence_safe(brief, selected, history=[])
        self.assertEqual(len(ordered), len(selected))
        self.assertIn("Greedy pair_score sequencing", rationale)

    def test_smart_composition_refines_with_top_k_passes(self) -> None:
        for spec in (
            (1, 260.0, "2024-01", "smooth", 0.20),
            (2, 260.0, "2024-01", "smooth", 0.25),
            (3, 260.0, "2024-01", "smooth", 0.30),
            (4, 160.0, "2024-02", "rough", 0.80),
            (5, 180.0, "2024-02", "grain", 0.76),
            (6, 170.0, "2024-03", "airy", 0.72),
        ):
            self._insert_track(pk=spec[0], track_id=f"t{spec[0]}", duration=spec[1], batch=spec[2], texture=spec[3], dsp=spec[4], tags="ambient,calm")

        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="smart", min_duration_min=10, max_duration_min=10, preferred_month_batch="2024-02")
        candidates = list_safe_candidates(self.conn, brief)
        safe_selected, _, _ = compose_safe(brief, candidates, history=[])
        smart_selected, _, _, smart_summary = compose_smart(brief, candidates, history=[])

        safe_err = abs(sum(c.duration_sec for c in safe_selected) - 600.0)
        smart_err = abs(sum(c.duration_sec for c in smart_selected) - 600.0)
        self.assertLessEqual(smart_err, safe_err)
        self.assertIn("top-", smart_summary)
        self.assertIn("accepted swap refinement", smart_summary)

    def test_smart_sequence_runs_local_reorder_refinement(self) -> None:
        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="smart", min_duration_min=8, max_duration_min=10)
        selected = [
            TrackCandidate(1, "t1", "darkwood-reverie", 180.0, "2024-01", frozenset({"a"}), False, False, "smooth", 0.10),
            TrackCandidate(2, "t2", "darkwood-reverie", 180.0, "2024-02", frozenset({"b"}), False, False, "smooth", 0.95),
            TrackCandidate(3, "t3", "darkwood-reverie", 180.0, "2024-03", frozenset({"c"}), False, False, "rough", 0.20),
            TrackCandidate(4, "t4", "darkwood-reverie", 180.0, "2024-04", frozenset({"d"}), False, False, "rough", 0.82),
        ]
        ordered, rationale = sequence_smart(brief, selected, history=[])
        self.assertEqual(len(ordered), len(selected))
        self.assertIn("bounded local reorder refinement", rationale)
        self.assertIn("accepted local reorder swap", rationale)

    def test_curated_composition_runs_seeded_deeper_refinement(self) -> None:
        for spec in (
            (1, 260.0, "2024-01", "smooth", 0.20),
            (2, 260.0, "2024-01", "smooth", 0.25),
            (3, 260.0, "2024-01", "smooth", 0.30),
            (4, 160.0, "2024-02", "rough", 0.82),
            (5, 180.0, "2024-02", "grain", 0.78),
            (6, 170.0, "2024-03", "airy", 0.72),
            (7, 190.0, "2024-03", "dense", 0.68),
        ):
            self._insert_track(pk=spec[0], track_id=f"t{spec[0]}", duration=spec[1], batch=spec[2], texture=spec[3], dsp=spec[4], tags="ambient,calm")

        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="curated", min_duration_min=10, max_duration_min=10, preferred_month_batch="2024-02")
        candidates = list_safe_candidates(self.conn, brief)
        curated_selected, _, _, curated_summary = compose_curated(brief, candidates, history=[])
        self.assertGreaterEqual(len(curated_selected), 3)
        self.assertIn("best-of-", curated_summary)
        self.assertIn("accepted replacements", curated_summary)

    def test_curated_sequencing_uses_beam_like_search_summary(self) -> None:
        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="curated", min_duration_min=8, max_duration_min=10)
        selected = [
            TrackCandidate(1, "t1", "darkwood-reverie", 180.0, "2024-01", frozenset({"a"}), False, False, "smooth", 0.10),
            TrackCandidate(2, "t2", "darkwood-reverie", 180.0, "2024-02", frozenset({"b"}), False, False, "smooth", 0.95),
            TrackCandidate(3, "t3", "darkwood-reverie", 180.0, "2024-03", frozenset({"c"}), False, False, "rough", 0.20),
            TrackCandidate(4, "t4", "darkwood-reverie", 180.0, "2024-04", frozenset({"d"}), False, False, "rough", 0.82),
        ]
        ordered, rationale = sequence_curated(brief, selected, history=[])
        self.assertEqual(len(ordered), len(selected))
        self.assertIn("beam-like bounded local search", rationale)

    def test_curated_guardrails_raise_explicit_failure(self) -> None:
        brief = PlaylistBrief(channel_slug="darkwood-reverie", generation_mode="curated", min_duration_min=8, max_duration_min=10)
        selected = [
            TrackCandidate(1, "t1", "darkwood-reverie", 180.0, "2024-01", frozenset(), False, False, "smooth", 0.10),
            TrackCandidate(2, "t2", "darkwood-reverie", 180.0, "2024-02", frozenset(), False, False, "smooth", 0.95),
            TrackCandidate(3, "t3", "darkwood-reverie", 180.0, "2024-03", frozenset(), False, False, "rough", 0.20),
        ]
        with self.assertRaises(CuratedOptimizationLimitExceeded):
            compose_curated(brief, selected, history=[], max_iterations=0)
        with self.assertRaises(CuratedSequencingLimitExceeded):
            sequence_curated(brief, selected, history=[], max_iterations=0)


if __name__ == "__main__":
    unittest.main()
