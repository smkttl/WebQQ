import json
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from webqq_app.app import configured_web_port
from webqq_app.auth import BanTracker
import webqq_app.api as api
from webqq_app.common import (
    canonical_chat_id,
    simplify_group_member,
    normalize_emoji_like_response,
    normalize_emoji_likes,
    notice_text,
    parse_chat_id,
    recall_notice_text,
)
from webqq_app.napcat import NapCatConnection
from webqq_app.messaging import send_text_and_register
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


class NapCatActionTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_poke_builds_private_and_group_requests(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_poke("10002")
        await connection.send_poke("10003", group_id="123")

        self.assertEqual(calls, [
            ("send_poke", {"user_id": 10002}, 10),
            ("send_poke", {"user_id": 10003, "group_id": 123}, 10),
        ])


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

    def test_group_member_role_is_normalized(self):
        self.assertEqual(simplify_group_member({"user_id": 1, "role": 4})["role"], "owner")
        self.assertEqual(simplify_group_member({"user_id": 1, "is_owner": True})["role"], "owner")


class FaceManifestTests(unittest.TestCase):
    def test_koishi_face_manifest_normalization_shape(self):
        sample = [
            {
                "emojiId": "4",
                "describe": "/得意",
                "emojiType": 0,
                "assets": [
                    {"type": 0, "path": "assets/qq_emoji/4/png/4.png"},
                    {"type": 2, "path": "assets/qq_emoji/4/apng/4.png"},
                ],
            }
        ]
        normalized = {
            "version": 1,
            "source": "koishi.js.org/QFace/assets/qq_emoji/_index.json",
            "faces": {
                "4": {
                    "type": 0,
                    "name": "得意",
                    "png": "https://koishi.js.org/QFace/assets/qq_emoji/4/png/4.png",
                    "gif": "https://koishi.js.org/QFace/assets/qq_emoji/4/apng/4.png",
                }
            },
            "reaction_ids": ["4"],
        }
        self.assertEqual(normalized["faces"]["4"]["name"], sample[0]["describe"].lstrip("/"))


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

    def test_group_role_lookup_supports_admin_revoke(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.set_self_user(10001, "Me")
            store.set_group_members(
                123,
                {"10001": "Me", "10002": "Other"},
                [{"user_id": "10001", "role": "admin"}, {"user_id": "10002", "role": "member"}],
            )
            self.assertEqual(store.current_group_role(123), "admin")
            self.assertEqual(store.get_group_member_role(123, 10002), "member")


class SendRegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_successful_send_with_message_id_is_not_left_pending(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            broadcasts = []

            async def send_message(chat_id, text, reply_to=None):
                return {"status": "ok", "data": {"message_id": 123}}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(send_message=send_message, _broadcast=broadcast, plugins=None)
            sent = await send_text_and_register(napcat, store, "group_1", "hello")

            self.assertEqual(sent["message"]["message_id"], 123)
            self.assertFalse(sent["message"]["pending"])
            self.assertFalse(broadcasts[0]["data"]["pending"])

    async def test_early_message_echo_prevents_late_pending_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            confirmed = {
                "chat_id": "group_1", "message_id": 123, "time": int(time.time()),
                "sender_id": 1, "sender_name": "Me", "content": "hello", "self": True,
            }
            store.append_simplified("group_1", confirmed)
            broadcasts = []

            async def send_message(chat_id, text, reply_to=None):
                return {"status": "ok", "data": {"message_id": 123}}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(send_message=send_message, _broadcast=broadcast, plugins=None)
            sent = await send_text_and_register(napcat, store, "group_1", "hello")

            self.assertIs(sent["message"], confirmed)
            self.assertEqual(len(store.get_messages("group_1")), 1)
            self.assertEqual(broadcasts, [])


class PokeHandlerTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(body, send_poke, self_id="10001"):
        async def request_json():
            return body

        return SimpleNamespace(
            app={
                "config": {"web_token": ""},
                "store": SimpleNamespace(_self_user={"user_id": self_id}),
                "napcat": SimpleNamespace(send_poke=send_poke),
            },
            query={},
            cookies={},
            headers={},
            remote="",
            json=request_json,
        )

    async def test_group_poke_targets_sender(self):
        calls = []

        async def send_poke(user_id, group_id=None):
            calls.append((user_id, group_id))
            return {"status": "ok", "data": {}}

        response = await api.handle_poke(self.request(
            {"chat_id": "group_123", "user_id": "10002"},
            send_poke,
        ))

        self.assertEqual(response.status, 200)
        self.assertTrue(json.loads(response.text)["ok"])
        self.assertEqual(calls, [("10002", 123)])

    async def test_private_poke_rejects_wrong_peer_and_self(self):
        async def send_poke(user_id, group_id=None):
            raise AssertionError("invalid poke reached NapCat")

        wrong_peer = await api.handle_poke(self.request(
            {"chat_id": "private_10002", "user_id": "10003"},
            send_poke,
        ))
        self_poke = await api.handle_poke(self.request(
            {"chat_id": "private_10001", "user_id": "10001"},
            send_poke,
        ))

        self.assertEqual(wrong_peer.status, 400)
        self.assertEqual(self_poke.status, 400)

    async def test_napcat_poke_rejection_is_reported(self):
        async def send_poke(user_id, group_id=None):
            return {"status": "failed", "wording": "poke unavailable"}

        response = await api.handle_poke(self.request(
            {"chat_id": "group_123", "user_id": "10002"},
            send_poke,
        ))
        payload = json.loads(response.text)

        self.assertEqual(response.status, 500)
        self.assertEqual(payload["error"], "poke unavailable")


class RevokeHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_admin_can_revoke_other_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.set_self_user(10001, "Me")
            store.set_group_members(123, {"10001": "Me", "10002": "Other"}, [{"user_id": "10001", "role": "admin"}])
            message = {
                "message_id": "88",
                "time": int(time.time()),
                "self": False,
            }
            store._data["group_123"].append(message)
            store._reindex_chat("group_123")
            app = {
                "store": store,
                "napcat": SimpleNamespace(delete_msg=lambda *_: None),
            }
            async def delete_msg(message_id):
                return {"status": "ok"}
            async def broadcast(_):
                return None
            app["napcat"] = SimpleNamespace(delete_msg=delete_msg, _broadcast=broadcast)
            app["config"] = {"web_token": ""}
            request = SimpleNamespace(
                app=app,
                query={},
                cookies={},
            )
            async def request_json():
                return {"message_id": "88", "chat_id": "group_123"}
            request.json = request_json
            request.headers = {}
            request.remote = ""
            response = await api.handle_message_revoke(request)
            self.assertEqual(response.status, 200)

    async def test_group_member_cannot_revoke_other_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.set_self_user(10001, "Me")
            store.set_group_members(123, {"10001": "Me", "10002": "Other"}, [{"user_id": "10001", "role": "member"}])
            message = {
                "message_id": "88",
                "time": int(time.time()),
                "self": False,
            }
            store._data["group_123"].append(message)
            store._reindex_chat("group_123")
            async def delete_msg(message_id):
                return {"status": "ok"}
            app = {
                "config": {"web_token": ""},
                "store": store,
                "napcat": SimpleNamespace(delete_msg=delete_msg, _broadcast=lambda _: None),
            }
            request = SimpleNamespace(app=app, query={}, cookies={}, headers={}, remote="")
            async def request_json():
                return {"message_id": "88", "chat_id": "group_123"}
            request.json = request_json
            response = await api.handle_message_revoke(request)
            self.assertEqual(response.status, 400)


if __name__ == "__main__":
    unittest.main()
