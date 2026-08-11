import io
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from webqq_tui_app.config import load_tui_preferences, save_tui_preferences
from webqq_tui_app.management import render_image_cells


class TuiManagementTests(unittest.TestCase):
    def test_terminal_image_preview_uses_color_cells_and_limits_size(self):
        image = Image.new("RGB", (20, 10), (12, 34, 56))
        body = io.BytesIO()
        image.save(body, format="PNG")
        rendered = render_image_cells(body.getvalue(), max_width=8, max_height=3)
        lines = rendered.plain.splitlines()
        self.assertLessEqual(max(map(len, lines)), 8)
        self.assertLessEqual(len(lines), 3)
        self.assertIn("▀", rendered.plain)
        self.assertTrue(any(span.style.color for span in rendered.spans))

    def test_tui_theme_preferences_round_trip_and_ignore_invalid_json(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "nested" / "tui.json"
            save_tui_preferences({"theme": "light"}, path)
            self.assertEqual(load_tui_preferences(path)["theme"], "light")
            path.write_text("not json", encoding="utf-8")
            self.assertEqual(load_tui_preferences(path), {})


if __name__ == "__main__":
    unittest.main()
