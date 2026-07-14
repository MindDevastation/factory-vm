from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass

from services.common import db as dbm
from services.track_analyzer.discover import DiscoverError, discover_channel_tracks

_FOLDER = "application/vnd.google-apps.folder"
_FILE = "audio/wav"


@dataclass
class FakeItem:
    id: str
    name: str
    mime_type: str


class FakeDrive:
    def __init__(self, *, fail_file_id: str | None = None) -> None:
        self._children: dict[str, list[FakeItem]] = {}
        self.rename_calls: list[tuple[str, str]] = []
        self.fail_file_id = fail_file_id

    def add_child(self, parent_id: str, item: FakeItem) -> None:
        self._children.setdefault(parent_id, []).append(item)

    def list_children(self, parent_id: str):
        return list(self._children.get(parent_id, []))

    def update_name(self, file_id: str, new_name: str) -> None:
        if file_id == self.fail_file_id:
            raise RuntimeError("simulated rename failure")
        self.rename_calls.append((file_id, new_name))
        for items in self._children.values():
            for item in items:
                if item.id == file_id:
                    item.name = new_name
                    return
        raise AssertionError(f"file not found: {file_id}")

    def names(self, parent_id: str) -> list[str]:
        return [i.name for i in self._children[parent_id]]


def _conn(td: str):
    conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
    dbm.migrate(conn)
    conn.execute(
        "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
        ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
    )
    conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
    conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
    return conn


def _base_drive() -> FakeDrive:
    drive = FakeDrive()
    drive.add_child("lib", FakeItem("ch", "Darkwood Reverie", _FOLDER))
    drive.add_child("ch", FakeItem("audio", "Audio", _FOLDER))
    return drive


class TestTrackDiscoverReconciliation(unittest.TestCase):
    def test_production_regression_chronological_global_numbering(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = _conn(td)
            try:
                drive = _base_drive()
                drive.add_child("audio", FakeItem("apr", "April 2026", _FOLDER))
                drive.add_child("audio", FakeItem("mar", "March 2026", _FOLDER))
                drive.add_child("audio", FakeItem("feb", "February 2026", _FOLDER))
                for idx in range(1, 85):
                    drive.add_child("feb", FakeItem(f"feb-{idx}", f"{idx:03d}_Feb {idx}.wav", _FILE))
                for idx in range(1, 101):
                    drive.add_child("mar", FakeItem(f"mar-{idx}", f"March {idx}.wav", _FILE))
                    drive.add_child("apr", FakeItem(f"apr-{idx}", f"April {idx}.wav", _FILE))
                stats = discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertEqual(stats.seen_wav, 284)
                self.assertEqual(drive.names("feb")[0], "0001_Feb 1.wav")
                self.assertEqual(drive.names("feb")[-1], "0084_Feb 84.wav")
                self.assertEqual(drive.names("mar")[0], "0085_March 1.wav")
                self.assertEqual(drive.names("mar")[-1], "0184_March 100.wav")
                self.assertEqual(drive.names("apr")[0], "0185_April 1.wav")
                self.assertEqual(drive.names("apr")[-1], "0284_April 100.wav")
            finally:
                conn.close()

    def test_legacy_stacked_and_duplicate_titles_are_repaired_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = _conn(td)
            try:
                drive = _base_drive()
                drive.add_child("audio", FakeItem("m", "2026-02", _FOLDER))
                names = ["001_Title.wav", "0001_001_Title (1).wav", "085_001_Title (2).wav", "0001_0001_Title (3).wav"]
                for idx, name in enumerate(names, start=1):
                    drive.add_child("m", FakeItem(f"fid-{idx}", name, _FILE))
                stats = discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertEqual(drive.names("m"), ["0001_Title.wav", "0002_Title 1.wav", "0003_Title 2.wav", "0004_Title 3.wav"])
                rows = conn.execute("SELECT track_id, filename, title FROM tracks ORDER BY track_id").fetchall()
                self.assertEqual([r["title"] for r in rows], ["Title", "Title 1", "Title 2", "Title 3"])
                self.assertTrue(all(len(r["track_id"]) == 4 for r in rows))
                self.assertGreaterEqual(stats.stacked_prefixes_repaired, 3)
                before = len(drive.rename_calls)
                stats2 = discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertEqual(stats2.renamed, 0)
                self.assertEqual(len(drive.rename_calls), before)
                self.assertEqual(conn.execute("SELECT COUNT(*) AS n FROM tracks").fetchone()["n"], 4)
            finally:
                conn.close()

    def test_preserves_existing_analyzed_track_pk_and_child_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = _conn(td)
            try:
                conn.execute(
                    "INSERT INTO tracks(id, channel_slug, track_id, gdrive_file_id, source, filename, title, discovered_at, analyzed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (42, "darkwood-reverie", "001", "fid-1", "GDRIVE", "001_Old.wav", "Old", 1.0, 2.0),
                )
                conn.execute("INSERT INTO track_features(track_pk, payload_json, computed_at) VALUES(?,?,?)", (42, "{}", 3.0))
                drive = _base_drive()
                drive.add_child("audio", FakeItem("m", "2026-02", _FOLDER))
                drive.add_child("m", FakeItem("fid-1", "001_New.wav", _FILE))
                discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                row = conn.execute("SELECT id, track_id, analyzed_at FROM tracks WHERE gdrive_file_id='fid-1'").fetchone()
                self.assertEqual(dict(row), {"id": 42, "track_id": "0001", "analyzed_at": 2.0})
                self.assertIsNotNone(conn.execute("SELECT 1 FROM track_features WHERE track_pk=42").fetchone())
            finally:
                conn.close()

    def test_rename_failure_fails_without_deleting_audio(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = _conn(td)
            try:
                drive = FakeDrive(fail_file_id="fid-1")
                drive.add_child("lib", FakeItem("ch", "Darkwood Reverie", _FOLDER))
                drive.add_child("ch", FakeItem("audio", "Audio", _FOLDER))
                drive.add_child("audio", FakeItem("m", "2026-02", _FOLDER))
                drive.add_child("m", FakeItem("fid-1", "001_Title.wav", _FILE))
                with self.assertRaises(DiscoverError) as ctx:
                    discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertIn("TRACK_DISCOVER_RENAME_FAILED", str(ctx.exception))
                self.assertEqual(len(drive.names("m")), 1)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
