import tempfile
import unittest
from pathlib import Path

from webqq_tui_app.client import collision_safe_path, safe_filename
from webqq_tui_app.config import TuiConfig, normalize_server_url
from webqq_tui_app.models import (
    Chat,
    Message,
    deduplicate_messages,
    display_content,
    format_chat,
    format_message,
    human_size,
    message_matches,
)


class TuiConfigTests(unittest.TestCase):
    def test_normalizes_server_urls(self):
        self.assertEqual(normalize_server_url("localhost:8080/"), "http://localhost:8080")
        self.assertEqual(normalize_server_url("https://example.test/webqq/"), "https://example.test/webqq")
        with self.assertRaises(ValueError):
            normalize_server_url("ftp://example.test")

    def test_cli_values_take_precedence_over_environment(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = TuiConfig.from_args(
                ["--url", "https://cli.test", "--token", "cli", "--download-dir", tmp],
                {"WEBQQ_URL": "https://env.test", "WEBQQ_TOKEN": "env"},
            )
            self.assertEqual(config.server_url, "https://cli.test")
            self.assertEqual(config.token, "cli")
            self.assertEqual(config.download_dir, Path(tmp).resolve())

    def test_environment_is_used_when_flags_are_absent(self):
        config = TuiConfig.from_args([], {"WEBQQ_URL": "server.test/base", "WEBQQ_TOKEN": "secret"})
        self.assertEqual(config.server_url, "http://server.test/base")
        self.assertEqual(config.token, "secret")


class TuiModelTests(unittest.TestCase):
    def test_rich_message_normalization_and_summary(self):
        message = Message.from_json({
            "chat_id": "group_1",
            "message_id": 10,
            "time": 100,
            "sender_id": 2,
            "sender_name": "Alice",
            "content": "hello @[3] [image] [file]",
            "mentions": {"3": "Bob"},
            "images": [{"summary": "photo", "url": "https://example.test/a.png"}],
            "files": [{"name": "notes.txt", "size": 1536, "id": "f1"}],
            "extra_segments": [{"type": "music", "label": "[music]", "text": "Track"}],
            "reactions": [{"emoji_id": "14", "count": 2}],
        })
        self.assertEqual(display_content(message), "hello @Bob")
        rendered = format_message(message).plain
        self.assertIn("[image: a.png]", rendered)
        self.assertIn("[file: notes.txt (1.5 KB)]", rendered)
        self.assertIn("[music] Track", rendered)
        self.assertIn("[face:14] x2", rendered)
        self.assertEqual(len(message.downloadable_attachments), 2)

    def test_compact_rendering_keeps_attachment_information(self):
        message = Message.from_json({
            "chat_id": "private_1",
            "sender_name": "A",
            "files": [{"name": "archive.zip", "size": 100, "id": "x"}],
            "forwards": [{"title": "Thread", "nodes": [{"sender_name": "B", "content": "inside"}]}],
        })
        rendered = format_message(message, compact=True).plain
        self.assertIn("archive.zip", rendered)
        self.assertIn("forward: Thread", rendered)

    def test_search_and_deduplication_reconcile_local_message(self):
        pending = Message.from_json({
            "chat_id": "private_2", "local_id": "local-a", "time": 10, "content": "Draft", "pending": True,
        })
        confirmed = Message.from_json({
            "chat_id": "private_2", "local_id": "local-a", "message_id": 99, "time": 10, "content": "Draft",
        })
        messages = deduplicate_messages([pending, confirmed])
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].message_id, "99")
        self.assertTrue(message_matches(messages[0], "draft"))

    def test_chat_and_size_formatting(self):
        chat = Chat("group_1", "Group", "group", 100, "preview")
        self.assertIn("Group", format_chat(chat).plain)
        self.assertEqual(human_size(1024), "1.0 KB")


class DownloadPathTests(unittest.TestCase):
    def test_sanitizes_names_and_avoids_overwrite(self):
        self.assertNotIn("/", safe_filename("../../secret.txt"))
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "report.txt").write_text("existing", encoding="utf-8")
            self.assertEqual(collision_safe_path(directory, "report.txt").name, "report (1).txt")


if __name__ == "__main__":
    unittest.main()
