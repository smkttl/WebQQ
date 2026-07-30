import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from webqq_tui_app.client import collision_safe_path, safe_filename
from webqq_tui_app.config import DEFAULT_SERVER_URL, TuiConfig, local_server_url, normalize_server_url
from webqq_tui_app.emoji import explain_emoji, load_emoji_names
from webqq_tui_app.models import (
    Chat,
    Message,
    deduplicate_messages,
    display_content,
    format_chat,
    format_message,
    forward_status_label,
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

    def test_local_config_port_is_default_when_url_is_unspecified(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text('{"web_port": 14232}', encoding="utf-8")
            with patch("webqq_tui_app.config.local_server_url", return_value=local_server_url(config_path)):
                config = TuiConfig.from_args([], {})
            self.assertEqual(config.server_url, "http://localhost:14232")

    def test_invalid_local_config_falls_back_to_standard_port(self):
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            config_path.write_text('{"web_port": 99999}', encoding="utf-8")
            self.assertEqual(local_server_url(config_path), DEFAULT_SERVER_URL)


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
        self.assertIn("[face:14 微笑] x2", rendered)
        self.assertEqual(len(message.downloadable_attachments), 2)

    def test_named_mention_uses_inline_name_without_duplicate_suffix(self):
        message = Message.from_json({
            "chat_id": "group_1",
            "content": "hello @[3](Bob)",
        })
        self.assertEqual(display_content(message), "hello @Bob")

    def test_compact_rendering_keeps_attachment_information(self):
        message = Message.from_json({
            "chat_id": "private_1",
            "sender_name": "A",
            "files": [{"name": "archive.zip", "size": 100, "id": "x"}],
            "forwards": [{"title": "Thread", "nodes": [{"sender_name": "B", "content": "inside"}]}],
        })
        rendered = format_message(message, compact=True).plain
        self.assertIn("archive.zip", rendered)
        self.assertIn("forward: Thread - 1 message", rendered)

    def test_structured_cards_and_additional_media_are_rendered_and_searchable(self):
        message = Message.from_json({
            "content": "[json card] [onlinefile] [video] [voice]",
            "files": [{"kind": "onlinefile", "name": "cloud.zip", "url": "https://example.test/cloud.zip"}],
            "videos": [{"name": "clip.mp4", "url": "https://example.test/clip.mp4"}],
            "records": [{"name": "memo.ogg", "url": "https://example.test/memo.ogg"}],
            "extra_segments": [{
                "type": "json", "label": "[json card]", "title": "Release notes",
                "description": "Important changes", "source": "Docs", "url": "https://example.test/read",
            }],
        })
        rendered = format_message(message, compact=True).plain

        self.assertEqual(display_content(message), "")
        self.assertIn("[json card] Release notes | from Docs | link - Important changes", rendered)
        self.assertEqual([item.kind for item in message.attachments], ["onlinefile", "video", "voice"])
        self.assertEqual(len(message.downloadable_attachments), 3)
        self.assertTrue(message_matches(message, "release notes"))
        self.assertTrue(message_matches(message, "cloud.zip"))

    def test_unavailable_forward_does_not_claim_zero_messages(self):
        forward = {"title": "Thread", "status": "unavailable", "error": "expired", "nodes": []}
        message = Message.from_json({"sender_name": "A", "content": "[forward]", "forwards": [forward]})
        rendered = format_message(message, compact=True).plain

        self.assertEqual(forward_status_label(forward), "unavailable")
        self.assertIn("forward: Thread - unavailable", rendered)
        self.assertNotIn("0 messages", rendered)

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
        self.assertFalse(messages[0].pending)
        self.assertTrue(message_matches(messages[0], "draft"))

    def test_confirmed_message_wins_over_late_pending_duplicate(self):
        confirmed = Message.from_json({
            "chat_id": "group_1", "message_id": 99, "time": 10, "sender_id": 42,
            "sender_name": "Me", "content": "sent", "self": True,
        })
        stale = Message.from_json({
            "chat_id": "group_1", "message_id": 99, "local_id": "late", "time": 10,
            "sender_id": "self", "sender_name": "You", "content": "sent", "self": True, "pending": True,
        })
        messages = deduplicate_messages([confirmed, stale])
        self.assertEqual(len(messages), 1)
        self.assertFalse(messages[0].pending)
        self.assertEqual(messages[0].sender_id, "42")

    def test_server_confirmation_clears_merged_pending_state(self):
        pending = Message.from_json({
            "chat_id": "group_1", "local_id": "local-a", "content": "sent", "pending": True,
        })
        confirmed = pending.merged({"message_id": 99, "sender_id": 1})
        self.assertEqual(confirmed.message_id, "99")
        self.assertFalse(confirmed.pending)

    def test_known_emoji_has_explanation_and_unknown_id_is_preserved(self):
        self.assertEqual(explain_emoji("14"), "[face:14 微笑]")
        self.assertEqual(explain_emoji("999999999"), "[face:999999999]")
        message = Message.from_json({"content": "hello [face:14] [face:999999999]"})
        self.assertEqual(display_content(message), "hello [face:14 微笑] [face:999999999]")

    def test_invalid_emoji_map_loads_as_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "invalid.json"
            path.write_text("not json", encoding="utf-8")
            self.assertEqual(load_emoji_names(path), {})

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
