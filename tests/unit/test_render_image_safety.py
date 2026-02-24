import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from render_worker.main import validate_image_decodable, validate_or_reencode_image


@unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required")
class TestRenderImageSafety(unittest.TestCase):
    def test_validate_image_decodable_valid_png(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            valid_png = d / "valid.png"
            cmd = [
                "ffmpeg",
                "-y",
                "-hide_banner",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "testsrc=duration=0.1:size=64x64:rate=1",
                "-frames:v",
                "1",
                str(valid_png),
            ]
            p = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
            self.assertEqual(p.returncode, 0, msg=p.stderr)

            ok, err_tail = validate_image_decodable(valid_png)
            self.assertTrue(ok)
            self.assertEqual(err_tail, "")

    def test_validate_image_decodable_invalid_png(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            bad_png = d / "invalid.png"
            bad_png.write_bytes(b"\x89PNG\r\n\x1a\n" + (b"\x00" * 10))

            ok, err_tail = validate_image_decodable(bad_png)
            self.assertFalse(ok)
            self.assertTrue(err_tail)

    def test_validate_or_reencode_image_invalid_does_not_hang(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            d = Path(td)
            bad_png = d / "invalid.png"
            bad_png.write_bytes(b"\x89PNG\r\n\x1a\n" + (b"\x00" * 10))
            tmp_dir = d / "tmp"

            with mock.patch("render_worker.main.reencode_image_to_safe", return_value=False):
                with self.assertRaisesRegex(RuntimeError, "invalid/corrupted image"):
                    validate_or_reencode_image(bad_png, tmp_dir)


if __name__ == "__main__":
    unittest.main()
