from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass

from services.common import db as dbm
from services.track_analyzer.canon import deterministic_hash_suffix
from services.track_analyzer.discover import DiscoverError, discover_channel_tracks

_FOLDER = "application/vnd.google-apps.folder"
_FILE = "audio/wav"


@dataclass
class FakeItem:
    id: str
    name: str
    mime_type: str


class FakeDrive:
    def __init__(self) -> None:
        self._children: dict[str, list[FakeItem]] = {}
        self.rename_calls: list[tuple[str, str]] = []

    def add_child(self, parent_id: str, item: FakeItem) -> None:
        self._children.setdefault(parent_id, []).append(item)

    def list_children(self, parent_id: str):
        return list(self._children.get(parent_id, []))

    def update_name(self, file_id: str, new_name: str) -> None:
        self.rename_calls.append((file_id, new_name))
        for items in self._children.values():
            for item in items:
                if item.id == file_id:
                    item.name = new_name
                    return
        raise AssertionError(f"file not found: {file_id}")


class TestTrackDiscover(unittest.TestCase):
    def test_discover_renames_canonical_with_trailing_numeric_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))

                drive = FakeDrive()
                drive.add_child("lib", FakeItem("ch", "Darkwood Reverie", _FOLDER))
                drive.add_child("ch", FakeItem("audio", "Audio", _FOLDER))
                drive.add_child("audio", FakeItem("m202501", "202501", _FOLDER))
                drive.add_child("m202501", FakeItem("fid-1", "001_Title (1).wav", _FILE))

                stats = discover_channel_tracks(
                    conn,
                    drive,
                    gdrive_library_root_id="lib",
                    channel_slug="darkwood-reverie",
                )

                self.assertEqual(stats.seen_wav, 1)
                self.assertEqual(stats.renamed, 1)
                self.assertIn(("fid-1", "001_Title.wav"), drive.rename_calls)
            finally:
                conn.close()

    def test_discover_wav_rename_upsert_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute(
                    """
                    INSERT INTO tracks(channel_slug, track_id, gdrive_file_id, source, filename, title, artist, duration_sec, discovered_at, analyzed_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    """,
                    ("darkwood-reverie", "099", "fid-upd", "GDRIVE", "old.wav", "Old", None, None, 1.0, None),
                )

                drive = FakeDrive()
                drive.add_child("lib", FakeItem("ch", "Darkwood Reverie", _FOLDER))
                drive.add_child("ch", FakeItem("audio", "Audio", _FOLDER))
                drive.add_child("audio", FakeItem("m202501", "202501", _FOLDER))
                drive.add_child("m202501", FakeItem("fid-existing", "001_Title.wav", _FILE))
                drive.add_child("m202501", FakeItem("fid-rename", "001 Title.wav", _FILE))
                drive.add_child("m202501", FakeItem("fid-upd", "003 New Name.wav", _FILE))
                drive.add_child("m202501", FakeItem("fid-noid", "Ambient mix.wav", _FILE))
                drive.add_child("m202501", FakeItem("fid-skip", "ignore.mp3", "audio/mpeg"))

                stats = discover_channel_tracks(
                    conn,
                    drive,
                    gdrive_library_root_id="lib",
                    channel_slug="darkwood-reverie",
                )

                self.assertEqual(stats.seen_wav, 4)
                self.assertEqual(stats.inserted, 2)
                self.assertEqual(stats.updated, 1)

                collision_suffix = deterministic_hash_suffix(
                    "darkwood-reverie", "202501", "fid-rename", "001 Title.wav", "001_Title.wav"
                )
                self.assertIn(("fid-rename", f"001_Title_{collision_suffix}.wav"), drive.rename_calls)
                self.assertIn(("fid-noid", "002_Ambient mix.wav"), drive.rename_calls)
                self.assertIn(("fid-upd", "003_New Name.wav"), drive.rename_calls)

                rows = conn.execute(
                    "SELECT channel_slug, track_id, gdrive_file_id, filename FROM tracks WHERE channel_slug=? ORDER BY track_id ASC",
                    ("darkwood-reverie",),
                ).fetchall()
                self.assertEqual(len(rows), 3)
                by_id = {r["gdrive_file_id"]: r for r in rows}
                self.assertEqual(by_id["fid-upd"]["track_id"], "003")
                self.assertEqual(by_id["fid-noid"]["track_id"], "002")

                stats_second = discover_channel_tracks(
                    conn,
                    drive,
                    gdrive_library_root_id="lib",
                    channel_slug="darkwood-reverie",
                )
                self.assertEqual(stats_second.inserted, 0)
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) AS n FROM tracks WHERE channel_slug=?", ("darkwood-reverie",)).fetchone()["n"],
                    3,
                )
            finally:
                conn.close()


    def test_discover_fails_when_channel_display_name_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))
                conn.execute("INSERT INTO canon_thresholds(value) VALUES(?)", ("darkwood-reverie",))

                drive = FakeDrive()
                with self.assertRaises(DiscoverError) as ctx:
                    discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertEqual(str(ctx.exception), "channel display_name is empty: darkwood-reverie")
            finally:
                conn.close()

    def test_discover_requires_channel_in_both_canon_tables(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            conn = dbm.connect(type("E", (), {"db_path": f"{td}/db.sqlite3"})())
            try:
                dbm.migrate(conn)
                conn.execute(
                    "INSERT INTO channels(slug, display_name, kind, weight, render_profile, autopublish_enabled) VALUES(?,?,?,?,?,?)",
                    ("darkwood-reverie", "Darkwood Reverie", "LONG", 1.0, "long_1080p24", 0),
                )
                conn.execute("INSERT INTO canon_channels(value) VALUES(?)", ("darkwood-reverie",))

                drive = FakeDrive()
                with self.assertRaises(DiscoverError) as ctx:
                    discover_channel_tracks(conn, drive, gdrive_library_root_id="lib", channel_slug="darkwood-reverie")
                self.assertEqual(str(ctx.exception), "CHANNEL_NOT_IN_CANON")
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
