import unittest

from services.track_analyzer.canon import (
    canonicalize_track_filename,
    deterministic_hash_suffix,
    sanitize_title,
)


class TestSanitizeTitle(unittest.TestCase):
    def test_replaces_forbidden_chars_and_collapses_spaces(self):
        self.assertEqual(
            sanitize_title('A<bad>:"/\\|?*   name'),
            'A bad name',
        )

    def test_removes_track_id_when_present(self):
        self.assertEqual(sanitize_title('Song 001 Mix', track_id='001'), 'Song Mix')

    def test_caps_length_to_90(self):
        title = 'A' * 120
        self.assertEqual(len(sanitize_title(title)), 90)


class TestCanonicalizeTrackFilename(unittest.TestCase):
    def test_keeps_second_id_for_double_prefix_pattern(self):
        self.assertEqual(
            canonicalize_track_filename('081_001_Title.ext'),
            '001_Title.ext',
        )

    def test_repairs_space_dash_dot_variants(self):
        self.assertEqual(canonicalize_track_filename('001 Title.ext'), '001_Title.ext')
        self.assertEqual(canonicalize_track_filename('001-Title.ext'), '001_Title.ext')
        self.assertEqual(canonicalize_track_filename('001.Title.ext'), '001_Title.ext')

    def test_preserves_already_canonical_form(self):
        self.assertEqual(canonicalize_track_filename('001_Title.ext'), '001_Title.ext')

    def test_non_matching_filename_is_unchanged(self):
        self.assertEqual(canonicalize_track_filename('Title.ext'), 'Title.ext')


class TestDeterministicHashSuffix(unittest.TestCase):
    def test_deterministic_for_same_parts(self):
        self.assertEqual(
            deterministic_hash_suffix('001', 'Title', '.ext'),
            deterministic_hash_suffix('001', 'Title', '.ext'),
        )

    def test_changes_when_input_changes(self):
        self.assertNotEqual(
            deterministic_hash_suffix('001', 'Title', '.ext'),
            deterministic_hash_suffix('002', 'Title', '.ext'),
        )

    def test_supports_4_to_6_chars(self):
        self.assertEqual(len(deterministic_hash_suffix('a', length=4)), 4)
        self.assertEqual(len(deterministic_hash_suffix('a', length=6)), 6)

    def test_rejects_invalid_length(self):
        with self.assertRaises(ValueError):
            deterministic_hash_suffix('a', length=3)
        with self.assertRaises(ValueError):
            deterministic_hash_suffix('a', length=7)


if __name__ == '__main__':
    unittest.main()
