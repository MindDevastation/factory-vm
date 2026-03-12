from __future__ import annotations

import unittest

from services.playlist_builder.api_adapter import PlaylistBuilderValidationError, resolve_playlist_brief
from services.playlist_builder.models import PlaylistBrief


class TestPlaylistBuilderBrief(unittest.TestCase):
    def test_model_validations(self) -> None:
        with self.assertRaises(Exception):
            PlaylistBrief(channel_slug="alpha", min_duration_min=0)
        with self.assertRaises(Exception):
            PlaylistBrief(channel_slug="alpha", min_duration_min=20, max_duration_min=10)
        with self.assertRaises(Exception):
            PlaylistBrief(channel_slug="alpha", novelty_target_min=0.9, novelty_target_max=0.4)
        with self.assertRaises(Exception):
            PlaylistBrief(channel_slug="alpha", preferred_batch_ratio=101)
        with self.assertRaises(Exception):
            PlaylistBrief(channel_slug="alpha", position_memory_window=0)

    def test_resolution_precedence(self) -> None:
        brief = resolve_playlist_brief(
            channel_slug="alpha",
            job_id=123,
            channel_settings={"min_duration_min": 40, "max_duration_min": 80, "generation_mode": "safe"},
            job_override={"generation_mode": "curated", "preferred_batch_ratio": 66},
            request_override={"preferred_batch_ratio": 22},
        )
        self.assertEqual(brief.channel_slug, "alpha")
        self.assertEqual(brief.job_id, 123)
        self.assertEqual(brief.generation_mode, "curated")
        self.assertEqual(brief.preferred_batch_ratio, 22)
        self.assertEqual(brief.target_duration_min, 60)

    def test_invalid_merged_brief_raises_domain_error(self) -> None:
        with self.assertRaises(PlaylistBuilderValidationError):
            resolve_playlist_brief(
                channel_slug="alpha",
                job_id=None,
                channel_settings={"min_duration_min": 40},
                job_override={"max_duration_min": 10},
                request_override=None,
            )


if __name__ == "__main__":
    unittest.main()
