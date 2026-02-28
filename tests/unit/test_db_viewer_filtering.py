from __future__ import annotations

import sqlite3
import unittest

from services.db_viewer.filtering import (
    detect_text_columns,
    filter_allowed_tables,
    filter_visible_columns,
    is_secret_name,
    make_human_table_name,
)
from services.db_viewer.meta import (
    SAFE_IDENTIFIER_RE,
    is_safe_identifier,
    is_text_declared_type,
    list_existing_tables,
    list_table_columns,
)


class TestDbViewerFiltering(unittest.TestCase):
    def test_secret_name_filtering_is_case_insensitive(self):
        self.assertTrue(is_secret_name("api_token"))
        self.assertTrue(is_secret_name("OAuthState"))
        self.assertTrue(is_secret_name("CREDENTIAL_ID"))
        self.assertFalse(is_secret_name("track_jobs"))

    def test_allowed_tables_apply_secret_and_denylist(self):
        existing = ["track_jobs", "oauth_sessions", "jobs", "internal"]
        self.assertEqual(filter_allowed_tables(existing, ["jobs"]), ["track_jobs", "internal"])

    def test_visible_columns_exclude_secret_names(self):
        columns = ["id", "job_type", "api_key", "password_hash", "payload_json"]
        self.assertEqual(filter_visible_columns(columns), ["id", "job_type", "payload_json"])

    def test_safe_identifier_regex(self):
        self.assertTrue(SAFE_IDENTIFIER_RE.match("track_jobs"))
        self.assertTrue(is_safe_identifier("_ok2"))
        self.assertFalse(is_safe_identifier("2bad"))
        self.assertFalse(is_safe_identifier("bad-name"))
        self.assertFalse(is_safe_identifier("jobs;drop"))

    def test_text_detection_for_declared_type(self):
        self.assertTrue(is_text_declared_type("TEXT"))
        self.assertTrue(is_text_declared_type("varchar(255)"))
        self.assertTrue(is_text_declared_type("nchar(10)"))
        self.assertFalse(is_text_declared_type("INTEGER"))
        self.assertFalse(is_text_declared_type(""))
        self.assertFalse(is_text_declared_type(None))

    def test_human_name_generation(self):
        self.assertEqual(make_human_table_name("track_jobs"), "Track Jobs")
        self.assertEqual(
            make_human_table_name("track_jobs", overrides={"track_jobs": "Track Queue"}),
            "Track Queue",
        )

    def test_sqlite_metadata_tables_and_columns_and_text_column_detection(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE track_jobs (id INTEGER, job_type TEXT, api_key TEXT, tries INT)")
        conn.execute("CREATE TABLE oauth_tokens (id INTEGER)")

        tables = list_existing_tables(conn)
        self.assertEqual(tables, ["oauth_tokens", "track_jobs"])

        cols = list_table_columns(conn, "track_jobs")
        self.assertEqual([c["name"] for c in cols], ["id", "job_type", "api_key", "tries"])

        text_cols = detect_text_columns(cols)
        self.assertEqual(text_cols, {"job_type", "api_key"})

        with self.assertRaisesRegex(ValueError, "invalid table identifier"):
            list_table_columns(conn, "track_jobs;DROP")


if __name__ == "__main__":
    unittest.main()
