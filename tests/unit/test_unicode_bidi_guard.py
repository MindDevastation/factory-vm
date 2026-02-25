import subprocess
import unicodedata
from pathlib import Path
import unittest


class TestNoBidiOrFormatUnicode(unittest.TestCase):
    def test_tracked_python_and_html_files_have_no_format_controls(self):
        tracked_files = subprocess.check_output(["git", "ls-files"], text=True).splitlines()
        target_files = [f for f in tracked_files if f.endswith((".py", ".html"))]

        forbidden_codepoints = {
            0x00A0,
            *range(0x2000, 0x200C),
            0x202F,
            0x205F,
            0x3000,
            0xFEFF,
            *range(0x202A, 0x202F),
            *range(0x2066, 0x206A),
        }

        issues = []
        for rel_path in target_files:
            path = Path(rel_path)
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError as exc:
                issues.append(f"{rel_path}: decode_error={exc}")
                continue

            for line_no, line in enumerate(text.splitlines(), 1):
                cps = sorted(
                    {
                        f"U+{ord(ch):04X}({unicodedata.name(ch, 'UNKNOWN')})"
                        for ch in line
                        if unicodedata.category(ch) == "Cf" or ord(ch) in forbidden_codepoints
                    }
                )
                if cps:
                    issues.append(f"{rel_path}:{line_no}: {', '.join(cps)}")

        self.assertEqual([], issues, "Found forbidden format-control unicode chars:\n" + "\n".join(issues))


if __name__ == "__main__":
    unittest.main()
