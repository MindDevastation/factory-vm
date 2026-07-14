from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest import mock

import numpy as np

from services.common import db as dbm
from services.track_analyzer import track_jobs_db as tjdb
from services.track_analyzer.analyze import AnalyzeError, analyze_tracks
from services.track_analyzer.yamnet_resample import TrackAnalyzeMemoryLimitError
from tests._helpers import seed_minimal_db, temp_env


class _WritingDrive:
    def download_to_path(self, _file_id: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"not-real-audio")


class TestTrackAnalyzeMemoryCleanup(unittest.TestCase):
    def test_memory_limit_failure_still_cleans_track_temp_dir(self) -> None:
        with temp_env() as (_, env):
            seed_minimal_db(env)
            conn = dbm.connect(env)
            try:
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("darkwood-reverie", "001", "fid-1", "GDRIVE", "001_A.wav", "A", None, None, dbm.now_ts(), None),
                )
                job_id = tjdb.enqueue_job(conn, job_type="TRACK_ANALYZE", channel_slug="darkwood-reverie", payload={})

                with mock.patch("services.track_analyzer.analyze._extract_duration_sec", return_value=12.5), mock.patch(
                    "services.track_analyzer.analyze._load_wav_pcm",
                    return_value=(np.zeros(64, dtype=np.float32), 16000, 1, None),
                ), mock.patch("services.track_analyzer.analyze._extract_true_peak_dbfs", return_value=-1.0), mock.patch(
                    "services.track_analyzer.analyze.yamnet.analyze_with_yamnet",
                    side_effect=TrackAnalyzeMemoryLimitError(),
                ):
                    with self.assertRaisesRegex(AnalyzeError, "TRACK_ANALYZE_MEMORY_LIMIT"):
                        analyze_tracks(
                            conn,
                            _WritingDrive(),
                            channel_slug="darkwood-reverie",
                            storage_root=env.storage_root,
                            job_id=job_id,
                            max_tracks=1,
                        )

                temp_dir = Path(env.storage_root) / "tmp" / "track_analyzer" / str(job_id) / "1"
                self.assertFalse(temp_dir.exists())
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
