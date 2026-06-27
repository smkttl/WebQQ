import tempfile
import time
import unittest
from unittest.mock import patch

from webqq_app.app import configured_web_port
from webqq_app.auth import BanTracker
import webqq_app.api as api
from webqq_app.common import (
    canonical_chat_id,
    normalize_emoji_like_response,
    normalize_emoji_likes,
    notice_text,
    parse_chat_id,
    recall_notice_text,
)
from webqq_app.napcat import NapCatConnection
from webqq_app.store import MessageStore


class ChatIdTests(unittest.TestCase):
    def test_parse_supported_chat_ids(self):
        self.assertEqual(parse_chat_id("group_123"), {"type": "group", "group_id": 123})
        self.assertEqual(parse_chat_id("private_456"), {"type": "private", "private_id": 456})
        self.assertEqual(parse_chat_id("temp_123_456"), {"type": "temp", "group_id": 123, "user_id": 456})

    def test_reject_invalid_chat_ids(self):
        for chat_id in ("", "group_x", "private_", "temp_1_x", "other_1", 123):
            self.assertIsNone(parse_chat_id(chat_id))

    def test_canonical_temp_chat_is_private(self):
        self.assertEqual(canonical_chat_id("temp_123_456"), "private_456")
        self.assertEqual(canonical_chat_id("group_123"), "group_123")
        self.assertEqual(canonical_chat_id("bad"), "bad")


class NapCatParsingTests(unittest.TestCase):
    def test_plain_text_stays_plain_without_reply(self):
        self.assertEqual(NapCatConnection._parse_message("hello"), "hello")

    def test_tokens_convert_to_segments(self):
        self.assertEqual(
            NapCatConnection._parse_message("hi @[10001] [face:14]", reply_to="99"),
            [
                {"type": "reply", "data": {"id": "99"}},
                {"type": "text", "data": {"text": "hi "}},
                {"type": "at", "data": {"qq": "10001"}},
                {"type": "text", "data": {"text": " "}},
                {"type": "face", "data": {"id": "14"}},
            ],
        )


class NoticeAndReactionTests(unittest.TestCase):
    def test_notice_text_group_events(self):
        self.assertEqual(
            notice_text({"notice_type": "group_increase", "sub_type": "invite", "user_id": 1, "operator_id": 2}),
            "1 joined the group by invitation from 2.",
        )
        self.assertEqual(
            notice_text({"notice_type": "group_ban", "sub_type": "ban", "user_id": 1, "operator_id": 2, "duration": 3600}),
            "1 was muted by 2 for 1h.",
        )

    def test_recall_notice_text(self):
        self.assertEqual(
            recall_notice_text({"notice_type": "group_recall", "operator_id": 2, "user_id": 1}),
            "Message recalled by 2 for 1.",
        )
        self.assertEqual(
            recall_notice_text({"notice_type": "friend_recall", "operator_id": 2}),
            "Message recalled by 2.",
        )

    def test_normalize_emoji_likes(self):
        self.assertEqual(
            normalize_emoji_like_response("14", {"count": "2", "users": [{"user_id": 1, "nickname": "A"}]}),
            {"emoji_id": "14", "count": 2, "users": [{"user_id": 1, "name": "A"}]},
        )
        self.assertEqual(
            normalize_emoji_likes([{"message_id": 1, "emoji_id": 14, "count": 1}, {"message_id": 2, "emoji_id": 5}], message_id=1),
            [{"emoji_id": "14", "count": 1, "users": []}],
        )


class BanTrackerTests(unittest.TestCase):
    def test_ban_window_and_expiry(self):
        tracker = BanTracker(max_failures=2, window_seconds=10, ban_seconds=5)
        self.assertFalse(tracker.record_failure("1.2.3.4", now=100))
        self.assertTrue(tracker.record_failure("1.2.3.4", now=101))
        self.assertTrue(tracker.is_banned("1.2.3.4", now=104))
        self.assertFalse(tracker.is_banned("1.2.3.4", now=107))


class ConfiguredWebPortTests(unittest.TestCase):
    def test_config_port_is_default(self):
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(configured_web_port({"web_port": 14232}), 14232)

    def test_environment_port_takes_precedence(self):
        with patch.dict("os.environ", {"WEBQQ_PORT": "19145", "PORT": "22222", "WEB_PORT": "33333"}, clear=True):
            self.assertEqual(configured_web_port({"web_port": 14232}), 19145)
        with patch.dict("os.environ", {"PORT": "22222", "WEB_PORT": "33333"}, clear=True):
            self.assertEqual(configured_web_port({"web_port": 14232}), 22222)
        with patch.dict("os.environ", {"WEB_PORT": "33333"}, clear=True):
            self.assertEqual(configured_web_port({"web_port": 14232}), 33333)

    def test_invalid_environment_port_fails_fast(self):
        with patch.dict("os.environ", {"WEBQQ_PORT": "not-a-port"}, clear=True):
            with self.assertRaises(ValueError):
                configured_web_port({"web_port": 14232})


class ApiExtractionTests(unittest.TestCase):
    def test_json_body_helper_is_available_to_api_handlers(self):
        self.assertTrue(callable(api.read_json_body))


class MessageStoreTests(unittest.TestCase):
    def test_temp_history_loads_as_private_chat(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = f"{tmp}/temp_10_20.json"
            with open(path, "w", encoding="utf-8") as f:
                f.write('[{"message_id":1,"time":100,"content":"hi","group_name":"G"}]')

            store = MessageStore(maxlen=10, data_dir=tmp)
            store.load_all()

            messages = store.get_messages("private_20", limit=10)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["chat_id"], "private_20")
            self.assertEqual(messages[0]["type"], "private")
            self.assertEqual(messages[0]["temp_group_id"], 10)

    def test_add_deduplicates_history_messages(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            message = {
                "post_type": "message",
                "message_type": "private",
                "user_id": 1,
                "message_id": 10,
                "time": int(time.time()),
                "message": "hello",
                "sender": {"user_id": 1, "nickname": "A"},
            }

            store.add_history_messages([message, dict(message)])

            messages = store.get_messages("private_1", limit=10)
            self.assertEqual(len(messages), 1)
            self.assertEqual(messages[0]["message_id"], 10)


if __name__ == "__main__":
    unittest.main()
