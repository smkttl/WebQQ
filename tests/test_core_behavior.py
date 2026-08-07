import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from webqq_app.app import configured_web_port
from webqq_app.auth import BanTracker
import webqq_app.api as api
from webqq_app.common import (
    PublicAddressResolver,
    canonical_chat_id,
    file_url_allowed,
    simplify_group_member,
    normalize_emoji_like_response,
    normalize_emoji_likes,
    notice_text,
    parse_chat_id,
    recall_notice_text,
)
from webqq_app.napcat import NapCatConnection
from webqq_app.messaging import normalize_forward_nodes, send_forward_and_register, send_text_and_register
from webqq_app.mentions import format_mentions_for_agent
from webqq_app.plugins import PluginContext, PluginManager
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

    def test_named_and_short_mentions_convert_to_the_same_segments(self):
        expected = [
            {"type": "at", "data": {"qq": "10001"}},
            {"type": "text", "data": {"text": " and "}},
            {"type": "at", "data": {"qq": "10002"}},
        ]
        self.assertEqual(NapCatConnection._parse_message("@[10001](Alice) and @[10002]"), expected)

    def test_agent_mentions_include_known_names(self):
        self.assertEqual(
            format_mentions_for_agent("Hi @[10001] and @[10002](old)", {"10001": "Alice", 10002: "Bob"}),
            "Hi @[10001](Alice) and @[10002](Bob)",
        )
        self.assertEqual(format_mentions_for_agent("Hi @[10003]", {}), "Hi @[10003]")

    def test_empty_forward_container_stays_empty(self):
        self.assertIsNone(NapCatConnection._extract_forward_response_payload({"messages": []}))


class NapCatActionTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_image_builds_group_private_and_temp_requests(self):
        calls = []
        store = SimpleNamespace(private_send_context=lambda user_id: {"group_id": 8})
        connection = NapCatConnection("", "", store)
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        image_path = Path("/tmp/webqq image.png")
        image = [{"type": "image", "data": {"file": image_path.resolve().as_uri()}}]
        await connection.send_image("group_7", image_path)
        await connection.send_image("private_9", image_path)
        await connection.send_image("temp_10_11", image_path)

        self.assertEqual(calls, [
            ("send_group_msg", {"group_id": 7, "message": image}, 10),
            ("send_private_msg", {"user_id": 9, "message": image, "group_id": 8}, 10),
            ("send_private_msg", {"user_id": 11, "group_id": 10, "message": image}, 10),
        ])

    async def test_rich_media_segments_use_4182_onebot_schema(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace(private_send_context=lambda user_id: {}))
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_video("group_7", Path("/tmp/movie clip.mp4"))
        await connection.send_voice("private_8", Path("/tmp/note.ogg"))
        await connection.send_contact("group_7", "qq", "123")
        await connection.send_music("group_7", {"type": "163", "id": "456"})

        self.assertEqual(calls[0], ("send_group_msg", {
            "group_id": 7,
            "message": [{"type": "video", "data": {"file": Path("/tmp/movie clip.mp4").resolve().as_uri()}}],
        }, 60))
        self.assertEqual(calls[1][1]["message"][0]["type"], "record")
        self.assertEqual(calls[2][1]["message"], [{"type": "contact", "data": {"type": "qq", "id": "123"}}])
        self.assertEqual(calls[3][1]["message"], [{"type": "music", "data": {"type": "163", "id": "456"}}])

    async def test_private_messages_keep_mentions_as_plain_text(self):
        calls = []
        store = SimpleNamespace(private_send_context=lambda user_id: {})
        connection = NapCatConnection("", "", store)
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_message(
            "private_7", "Hi @[42](Alice) [face:14]", reply_to="9",
        )

        self.assertEqual(calls[0][0], "send_private_msg")
        message = calls[0][1]["message"]
        self.assertFalse(any(segment.get("type") == "at" for segment in message))
        self.assertEqual(message[0], {"type": "reply", "data": {"id": "9"}})
        self.assertTrue(any(
            segment.get("type") == "text" and segment.get("data", {}).get("text") == "@[42](Alice)"
            for segment in message
        ))
        self.assertTrue(any(segment.get("type") == "face" for segment in message))

    async def test_group_messages_still_send_mentions(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_message("group_7", "Hi @[42](Alice)")

        self.assertEqual(calls[0][0], "send_group_msg")
        self.assertTrue(any(
            segment.get("type") == "at" and segment.get("data", {}).get("qq") == "42"
            for segment in calls[0][1]["message"]
        ))

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

    async def test_qzone_actions_use_native_actions(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            if action == "send_qzone_msg":
                return {"status": "ok", "data": {"tid": "abc123"}}
            return {"status": "ok"}

        connection._request = request
        result = await connection.send_qzone_post("hello", ["file:///tmp/a.png"], 16, [123, "456"])
        await connection.delete_qzone_post("abc123")

        self.assertEqual(result["data"]["tid"], "abc123")
        self.assertEqual(calls, [
            ("send_qzone_msg", {
                "content": "hello",
                "images": ["file:///tmp/a.png"],
                "ugc_right": 16,
                "target_uins": ["123", "456"],
            }, 180),
            ("delete_qzone_msg", {"tid": "abc123"}, 30),
        ])

    async def test_fetch_forward_retries_napcat_parameter_alias(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            if "id" in params:
                return {"status": "failed", "wording": "invalid parameter"}
            return {"status": "ok", "data": {"messages": [{"content": "inside"}]}}

        connection._request = request
        response = await connection.fetch_forward("forward-1")

        self.assertEqual(response, {"status": "ok", "data": [{"content": "inside"}]})
        self.assertEqual(calls, [
            ("get_forward_msg", {"id": "forward-1"}, 10),
            ("get_forward_msg", {"message_id": "forward-1"}, 10),
        ])

    async def test_send_forward_builds_reference_and_custom_group_nodes(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok", "data": {"message_id": 123}}

        connection._request = request
        await connection.send_forward("group_9", [
            {"message_id": "10"},
            {"sender_id": "10001", "sender_name": "Alice", "content": "Hi @[2] [face:14]"},
        ])

        self.assertEqual(calls[0], ("nc_get_packet_status", {}, 10))
        self.assertEqual(calls[1][0], "send_group_forward_msg")
        self.assertEqual(calls[1][1]["group_id"], 9)
        self.assertEqual(calls[1][1]["messages"][0], {"type": "node", "data": {"id": "10"}})
        custom = calls[1][1]["messages"][1]["data"]
        self.assertEqual(custom["user_id"], "10001")
        self.assertEqual(custom["nickname"], "Alice")
        self.assertIsInstance(custom["content"], list)
        self.assertEqual(calls[1][2], 60)

    async def test_send_reference_forward_uses_private_temp_context_without_packet_check(self):
        calls = []
        store = SimpleNamespace(private_send_context=lambda user_id: {"group_id": 8})
        connection = NapCatConnection("", "", store)
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_forward("private_7", [{"message_id": "10"}])

        self.assertEqual(calls, [(
            "send_private_forward_msg",
            {"user_id": 7, "group_id": 8, "messages": [{"type": "node", "data": {"id": "10"}}]},
            60,
        )])

    async def test_send_reference_forward_supports_legacy_temp_chat_id(self):
        calls = []
        connection = NapCatConnection("", "", SimpleNamespace())
        connection.ws = object()

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.send_forward("temp_8_7", [{"message_id": "10"}])

        self.assertEqual(calls, [(
            "send_private_forward_msg",
            {"user_id": 7, "group_id": 8, "messages": [{"type": "node", "data": {"id": "10"}}]},
            60,
        )])

    async def test_custom_forward_fails_when_packet_backend_is_unavailable(self):
        connection = NapCatConnection("", "", SimpleNamespace())
        connection.ws = object()

        async def request(action, params, timeout=10):
            return {"status": "failed", "wording": "packet disabled"}

        connection._request = request
        with self.assertRaisesRegex(RuntimeError, "packet backend"):
            await connection.send_forward("group_9", [
                {"sender_id": "10001", "sender_name": "Alice", "content": "hello"},
            ])


class ImageSendHandlerTests(unittest.IsolatedAsyncioTestCase):
    class Part:
        def __init__(self, name, value):
            self.name = name
            self.value = value
            self.sent = False

        async def text(self):
            return str(self.value)

        async def read_chunk(self, size=0):
            if self.sent:
                return b""
            self.sent = True
            return self.value

        async def release(self):
            return None

    class Reader:
        def __init__(self, parts):
            self.parts = list(parts)

        async def next(self):
            return self.parts.pop(0) if self.parts else None

    async def test_send_image_passes_temporary_file_to_napcat_and_cleans_it(self):
        captured = {}

        async def send_image(chat_id, path):
            captured.update(chat_id=chat_id, path=path, body=Path(path).read_bytes())
            return {"status": "ok", "data": {"message_id": 12}}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace(send_image=send_image)},
            query={}, cookies={}, headers={}, remote="",
        )
        request.multipart = lambda: self._multipart([
            self.Part("chat_id", "private_42"),
            self.Part("file", b"image bytes"),
        ])
        response = await api.handle_send_image(request)

        self.assertEqual(response.status, 200)
        self.assertEqual(captured["chat_id"], "private_42")
        self.assertEqual(captured["body"], b"image bytes")
        self.assertFalse(Path(captured["path"]).exists())

    async def test_send_image_rejects_invalid_chat(self):
        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace()},
            query={}, cookies={}, headers={}, remote="",
        )
        request.multipart = lambda: self._multipart([
            self.Part("chat_id", "invalid"),
            self.Part("file", b"image bytes"),
        ])
        response = await api.handle_send_image(request)
        self.assertEqual(response.status, 400)
        self.assertIn("invalid chat_id", response.text)

    async def _multipart(self, parts):
        return self.Reader(parts)

    async def test_send_video_upload_is_bounded_and_cleaned_up(self):
        captured = {}

        async def send_video(chat_id, path):
            captured.update(chat_id=chat_id, path=path, body=Path(path).read_bytes())
            return {"status": "ok", "data": {"message_id": 14}}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace(send_video=send_video)},
            query={}, cookies={}, headers={}, remote="",
        )
        request.multipart = lambda: self._multipart([
            self.Part("chat_id", "group_42"), self.Part("file", b"video bytes"),
        ])
        response = await api.handle_send_video(request)
        self.assertEqual(response.status, 200)
        self.assertEqual(captured["body"], b"video bytes")
        self.assertFalse(Path(captured["path"]).exists())


class RichMediaHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_contact_and_custom_music_are_validated_and_sent(self):
        sent = []

        async def send_contact(chat_id, contact_type, contact_id):
            sent.append(("contact", chat_id, contact_type, contact_id))
            return {"status": "ok", "data": {}}

        async def send_music(chat_id, music):
            sent.append(("music", chat_id, music))
            return {"status": "ok", "data": {}}

        async def contact_json():
            return {"chat_id": "private_1", "type": "group", "id": "123"}

        async def music_json():
            return {
                "chat_id": "group_2", "type": "custom", "url": "https://page",
                "audio": "https://audio", "title": "Song", "image": "https://cover", "content": "Artist",
            }

        base = {"config": {"web_token": ""}, "napcat": SimpleNamespace(send_contact=send_contact, send_music=send_music)}
        contact_request = SimpleNamespace(app=base, query={}, cookies={}, headers={}, remote="", json=contact_json)
        music_request = SimpleNamespace(app=base, query={}, cookies={}, headers={}, remote="", json=music_json)
        self.assertEqual((await api.handle_send_contact(contact_request)).status, 200)
        self.assertEqual((await api.handle_send_music(music_request)).status, 200)
        self.assertEqual(sent[0], ("contact", "private_1", "group", "123"))
        self.assertEqual(sent[1][2]["type"], "custom")
        self.assertEqual(sent[1][2]["title"], "Song")


class VoiceTranscriptionTests(unittest.IsolatedAsyncioTestCase):
    async def test_transcription_is_cached_persisted_and_broadcast(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.append_simplified("group_2", {
                "chat_id": "group_2", "message_id": 55, "time": 1, "content": "[voice]",
                "records": [{"file": "voice.amr"}],
            })
            calls = []
            broadcasts = []

            async def fetch_ptt_text(message_id):
                calls.append(message_id)
                return {"status": "ok", "data": {"text": "hello there"}}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(fetch_ptt_text=fetch_ptt_text, _broadcast=broadcast)

            async def request_json():
                return {"chat_id": "group_2", "message_id": "55"}

            request = SimpleNamespace(
                app={"config": {"web_token": ""}, "store": store, "napcat": napcat},
                query={}, cookies={}, headers={}, remote="", json=request_json,
            )
            first = await api.handle_message_transcribe(request)
            second = await api.handle_message_transcribe(request)
            self.assertEqual(first.status, 200)
            self.assertEqual(second.status, 200)
            self.assertEqual(calls, ["55"])
            self.assertEqual(store.get_messages("group_2")[0]["records"][0]["transcript"], "hello there")
            self.assertEqual(broadcasts[0]["type"], "message_update")
            persisted = json.loads(Path(tmp, "group_2.json").read_text(encoding="utf-8"))
            self.assertEqual(persisted[0]["records"][0]["transcript"], "hello there")

    async def test_non_voice_message_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.append_simplified("private_2", {"chat_id": "private_2", "message_id": 5, "records": []})

            async def request_json():
                return {"chat_id": "private_2", "message_id": "5"}

            request = SimpleNamespace(
                app={"config": {"web_token": ""}, "store": store},
                query={}, cookies={}, headers={}, remote="", json=request_json,
            )
            response = await api.handle_message_transcribe(request)
            self.assertEqual(response.status, 400)


class QzoneHandlerTests(unittest.IsolatedAsyncioTestCase):
    class Part(ImageSendHandlerTests.Part):
        pass

    class Reader(ImageSendHandlerTests.Reader):
        pass

    async def _multipart(self, parts):
        return self.Reader(parts)

    async def test_json_post_validates_and_returns_tid(self):
        captured = {}

        async def send_qzone_post(content, images, ugc_right, target_uins):
            captured.update(content=content, images=images, ugc_right=ugc_right, target_uins=target_uins)
            return {"status": "ok", "data": {"tid": "tid-1"}}

        async def request_json():
            return {"content": "hello", "ugc_right": 16, "target_uins": [123, "456"]}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace(send_qzone_post=send_qzone_post)},
            query={}, cookies={}, headers={}, remote="", content_type="application/json", json=request_json,
        )
        response = await api.handle_qzone_post(request)

        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.text), {"ok": True, "tid": "tid-1"})
        self.assertEqual(captured, {"content": "hello", "images": [], "ugc_right": 16, "target_uins": ["123", "456"]})

    async def test_multipart_images_are_cleaned_after_send(self):
        captured = {}

        async def send_qzone_post(content, images, ugc_right, target_uins):
            captured.update(content=content, images=images, ugc_right=ugc_right, target_uins=target_uins)
            captured["exists_during_call"] = all(Path(path.removeprefix("file://")).exists() for path in images)
            return {"status": "ok", "data": {"tid": "tid-2"}}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace(send_qzone_post=send_qzone_post)},
            query={}, cookies={}, headers={}, remote="", content_type="multipart/form-data",
        )
        request.multipart = lambda: self._multipart([
            self.Part("content", "with images"),
            self.Part("target_uins", "123"),
            self.Part("target_uins", "456"),
            self.Part("images", b"one"),
            self.Part("images", b"two"),
        ])
        response = await api.handle_qzone_post(request)

        self.assertEqual(response.status, 200)
        self.assertTrue(captured["exists_during_call"])
        self.assertEqual(captured["target_uins"], ["123", "456"])
        self.assertTrue(all(not Path(path.removeprefix("file://")).exists() for path in captured["images"]))

    async def test_post_rejects_missing_targets_for_selected_visibility(self):
        async def request_json():
            return {"content": "hello", "ugc_right": 128}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace()},
            query={}, cookies={}, headers={}, remote="", content_type="application/json", json=request_json,
        )
        response = await api.handle_qzone_post(request)
        self.assertEqual(response.status, 400)
        self.assertIn("target_uins is required", response.text)

    async def test_delete_post_calls_napcat(self):
        captured = {}

        async def delete_qzone_post(tid):
            captured["tid"] = tid
            return {"status": "ok"}

        request = SimpleNamespace(
            app={"config": {"web_token": ""}, "napcat": SimpleNamespace(delete_qzone_post=delete_qzone_post)},
            query={}, cookies={}, headers={}, remote="", match_info={"tid": "tid-3"},
        )
        response = await api.handle_delete_qzone_post(request)
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.text), {"ok": True})
        self.assertEqual(captured["tid"], "tid-3")


class ForwardNodeValidationTests(unittest.TestCase):
    def test_normalizes_reference_and_custom_nodes(self):
        self.assertEqual(normalize_forward_nodes([
            {"message_id": 12},
            {"sender_id": 10001, "sender_name": " Alice ", "content": "long text"},
        ]), [
            {"message_id": "12"},
            {"sender_id": "10001", "sender_name": "Alice", "content": "long text"},
        ])

    def test_rejects_empty_mixed_and_oversized_nodes(self):
        invalid = (
            [],
            [{"message_id": "1", "content": "mixed"}],
            [{"sender_id": "1", "sender_name": "", "content": "text"}],
            [{"sender_id": "1", "sender_name": "name", "content": "x" * 20001}],
        )
        for nodes in invalid:
            with self.subTest(nodes=str(nodes)[:40]), self.assertRaises(ValueError):
                normalize_forward_nodes(nodes)


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


class WebBackgroundTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(source=""):
        return SimpleNamespace(
            app={
                "config": {"web_token": "", "web_background_image": source},
                "napcat": SimpleNamespace(ws=None),
                "store": SimpleNamespace(_data={}, _self_user={"user_id": "self", "name": "You"}),
            },
            query={}, cookies={}, headers={}, remote="",
        )

    async def test_status_exposes_background_availability_without_source(self):
        response = await api.handle_status(self.request("https://example.test/private-image.png"))
        payload = json.loads(response.text)
        self.assertTrue(payload["web_background_image"])
        self.assertNotIn("example.test", response.text)

    async def test_local_background_resolves_relative_to_repository(self):
        with tempfile.TemporaryDirectory() as tmp:
            image = Path(tmp, "background.png")
            image.write_bytes(b"png body")
            with patch.object(api, "ROOT_DIR", Path(tmp)):
                response = await api.handle_background_image(self.request("background.png"))
            self.assertEqual(response.status, 200)
            self.assertEqual(response._path, image)
            self.assertEqual(response.headers["Content-Type"], "image/png")
            self.assertEqual(response.headers["Cache-Control"], "private, no-cache")

    async def test_remote_background_uses_backend_image_fetcher(self):
        calls = []

        async def fetch(urls):
            calls.append(urls)
            return api.web.Response(body=b"image", content_type="image/jpeg")

        source = "https://example.test/background.jpg"
        with patch.object(api, "fetch_first_image", fetch):
            response = await api.handle_background_image(self.request(source))
        self.assertEqual(calls, [[source]])
        self.assertEqual(response.body, b"image")
        self.assertEqual(response.headers["Cache-Control"], "private, no-cache")

    async def test_missing_and_non_image_backgrounds_fail_cleanly(self):
        missing = await api.handle_background_image(self.request(""))
        self.assertEqual(missing.status, 404)
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "background.txt").write_text("not an image", encoding="utf-8")
            with patch.object(api, "ROOT_DIR", Path(tmp)):
                invalid = await api.handle_background_image(self.request("background.txt"))
        self.assertEqual(invalid.status, 400)

    class Part:
        def __init__(self, name, value, filename="background.png"):
            self.name = name
            self.value = value
            self.filename = filename
            self.sent = False

        async def read_chunk(self, size=0):
            if self.sent:
                return b""
            self.sent = True
            return self.value

        async def release(self):
            return None

    class Reader:
        def __init__(self, parts):
            self.parts = list(parts)

        async def next(self):
            return self.parts.pop(0) if self.parts else None

    async def test_upload_and_clear_managed_background(self):
        png = b"\x89PNG\r\n\x1a\n"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            request = self.request("")
            request.content_type = "multipart/form-data"
            async def multipart():
                return self.Reader([self.Part("file", png)])
            request.multipart = multipart
            request.app["config"] = {"web_token": "", "web_background_image": ""}
            with patch.object(api, "ROOT_DIR", root), patch.object(api, "save_config"):
                response = await api.handle_background_image_upload(request)
                self.assertEqual(response.status, 200)
                source = request.app["config"]["web_background_image"]
                self.assertTrue(source.startswith("data/web_background_image."))
                self.assertTrue((root / source).is_file())
                status = await api.handle_status(request)
                status_payload = json.loads(status.text)
                self.assertTrue(status_payload["web_background_image"])
                self.assertTrue(status_payload["web_background_revision"])
                cleared = await api.handle_background_image_clear(request)
                self.assertEqual(cleared.status, 200)
                self.assertEqual(json.loads(cleared.text)["web_background_revision"], "")
                self.assertFalse(list((root / "data").glob("web_background_image.*")))

    async def test_upload_rejects_non_image(self):
        request = self.request("")
        request.content_type = "multipart/form-data"
        async def multipart():
            return self.Reader([self.Part("file", b"not an image", "bad.txt")])
        request.multipart = multipart
        with tempfile.TemporaryDirectory() as tmp, patch.object(api, "ROOT_DIR", Path(tmp)):
            response = await api.handle_background_image_upload(request)
        self.assertEqual(response.status, 400)

    def test_web_assets_apply_backend_background_to_conversation(self):
        html = Path("static/index.html").read_text(encoding="utf-8")
        css = Path("static/app.css").read_text(encoding="utf-8")
        self.assertIn("custom-chat-background", html)
        self.assertIn("Boolean(d.web_background_image)", html)
        self.assertIn(":root.custom-chat-background .messages-scroll", css)
        self.assertIn("var(--web-background-image)", css)
        self.assertIn("backgroundButton", html)


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
    def test_incoming_poke_uses_window_vibration_preview_and_structured_segment(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            simplified = store.add({
                "post_type": "message",
                "message_type": "private",
                "user_id": 42,
                "message_id": 99,
                "time": 100,
                "message": [{"type": "poke", "data": {"type": "0", "id": "0"}}],
                "sender": {"user_id": 42, "nickname": "Alice"},
            })

            self.assertEqual(simplified["content"], "Window vibration")
            self.assertEqual(simplified["extra_segments"][0]["type"], "poke")
            self.assertEqual(simplified["extra_segments"][0]["label"], "Window vibration")
            self.assertEqual(store.get_chats()[0]["last_text"], "Window vibration")

    def test_legacy_poke_history_is_normalized_on_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "private_42.json").write_text(json.dumps([{
                "chat_id": "private_42",
                "type": "private",
                "user_id": 42,
                "message_id": 99,
                "time": 100,
                "content": "[poke]",
                "extra_segments": [{"type": "poke", "label": "[poke]"}],
            }]), encoding="utf-8")
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.load_all()

            message = store.get_messages("private_42")[0]
            self.assertEqual(message["content"], "Window vibration")
            self.assertEqual(message["extra_segments"][0]["label"], "Window vibration")
            self.assertEqual(store.get_chats()[0]["last_text"], "Window vibration")

    def test_structured_segments_preserve_card_and_media_details(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            card = json.dumps({
                "app": "com.tencent.structmsg",
                "prompt": "A shared article",
                "meta": {"news": {
                    "title": "Card title",
                    "desc": "Card description",
                    "jumpUrl": "https://example.test/read",
                    "preview": "https://example.test/cover.jpg",
                    "tag": "Example News",
                }},
            })
            content, _, _, _, _, videos, records, extras = store._extract_text({
                "message": [
                    {"type": "json", "data": {"data": card}},
                    {"type": "music", "data": {
                        "title": "Track", "audio": "https://example.test/track.mp3",
                        "url": "javascript:alert(1)", "image": "https://example.test/art.jpg",
                    }},
                    {"type": "location", "data": {
                        "title": "Meeting point", "lat": 31.2, "lon": 121.5,
                    }},
                    {"type": "video", "data": {"file": "clip.mp4", "url": "https://example.test/clip.mp4"}},
                    {"type": "record", "data": {"file": "voice.amr", "url": "https://example.test/voice.amr"}},
                ]
            })

            self.assertIn("[json card]", content)
            self.assertEqual(extras[0]["title"], "Card title")
            self.assertEqual(extras[0]["description"], "Card description")
            self.assertEqual(extras[0]["source"], "Example News")
            self.assertEqual(extras[0]["url"], "https://example.test/read")
            self.assertEqual(extras[0]["image"], "https://example.test/cover.jpg")
            self.assertEqual(extras[1]["audio"], "https://example.test/track.mp3")
            self.assertNotIn("url", extras[1])
            self.assertEqual(extras[2]["latitude"], "31.2")
            self.assertEqual(extras[2]["longitude"], "121.5")
            self.assertEqual(videos[0]["file"], "clip.mp4")
            self.assertEqual(records[0]["file"], "voice.amr")

    def test_invalid_json_card_has_bounded_text_fallback(self):
        extra = MessageStore._simplify_extra_segment("json", {"data": "{not json"})
        self.assertEqual(extra["label"], "[json card]")
        self.assertEqual(extra["text"], "{not json")

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

    def test_add_ignores_duplicate_live_message_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            message = {
                "post_type": "message",
                "message_type": "group",
                "group_id": 1,
                "user_id": 2,
                "message_id": 10,
                "time": int(time.time()),
                "message": "hello",
                "sender": {"user_id": 2, "nickname": "A"},
            }

            self.assertIsNotNone(store.add(message))
            self.assertIsNone(store.add(dict(message)))
            self.assertEqual(len(store.get_messages("group_1", limit=10)), 1)

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

    def test_failed_flush_preserves_existing_file_and_dirty_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.append_simplified("group_1", {"message_id": 1, "time": 1, "content": "new"})
            store._dirty.add("group_1")
            path = store._chat_path("group_1")
            path.write_text('[{"content":"old"}]', encoding="utf-8")

            with patch("webqq_app.store.json.dump", side_effect=OSError("disk full")):
                store.flush()

            self.assertEqual(path.read_text(encoding="utf-8"), '[{"content":"old"}]')
            self.assertIn("group_1", store._dirty)
            store.flush()
            self.assertNotIn("group_1", store._dirty)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))[0]["content"], "new")


class FileProxySafetyTests(unittest.IsolatedAsyncioTestCase):
    def test_file_url_rejects_local_and_private_addresses(self):
        for url in (
            "http://localhost/file",
            "http://127.0.0.1/file",
            "http://10.0.0.1/file",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::1]/file",
        ):
            self.assertFalse(file_url_allowed(url), url)
        self.assertTrue(file_url_allowed("https://8.8.8.8/file"))
        self.assertTrue(file_url_allowed("https://example.com/file"))

    async def test_resolver_rejects_domain_resolving_to_private_address(self):
        class Resolver:
            async def resolve(self, host, port=0, family=0):
                return [{"hostname": host, "host": "127.0.0.1", "port": port, "family": family, "proto": 0, "flags": 0}]

            async def close(self):
                return None

        resolver = PublicAddressResolver(Resolver())
        with self.assertRaises(OSError):
            await resolver.resolve("attacker.example", 80)


class PluginLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_unload_calls_teardown_and_cancels_context_tasks(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            manager = PluginManager(tmp, {"plugins": {"enabled": {}}}, store)
            ctx = PluginContext(manager, "worker", {})

            class Instance:
                stopped = False

                def teardown(self):
                    self.stopped = True

            instance = Instance()
            task = ctx.create_task(asyncio.Event().wait())
            manager._plugins["worker"] = {
                "id": "worker", "ctx": ctx, "instance": instance, "module": object(),
                "handler": lambda event, context: None, "portal_handler": None, "loaded": True,
            }

            manager.unload_plugin("worker")
            await asyncio.sleep(0)

            self.assertTrue(instance.stopped)
            self.assertTrue(task.cancelled())
            self.assertFalse(manager._plugins["worker"]["loaded"])


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

    async def test_confirmation_matches_assigned_id_when_mention_text_differs(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            local = {
                "chat_id": "group_1", "message_id": 123, "time": int(time.time()),
                "sender_id": "self", "sender_name": "You",
                "content": "hello @[10001](Alice)", "self": True,
            }
            store.register_pending_local_message("group_1", local)
            incoming = {
                "chat_id": "group_1", "message_id": 123, "time": local["time"],
                "sender_id": 999, "sender_name": "Me",
                "content": "hello @[10001]", "self": True,
            }

            reconciled = store.reconcile_self_message(incoming)

            self.assertTrue(reconciled["replaced"])
            self.assertEqual(len(store.get_messages("group_1")), 1)
            self.assertEqual(store.get_messages("group_1")[0]["message_id"], 123)

    async def test_forward_send_registers_renderable_local_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.set_self_user(10001, "Me")
            broadcasts = []

            async def send_forward(chat_id, nodes):
                return {"status": "ok", "data": {
                    "message_id": 123, "forward_id": "forward-1",
                }}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(
                send_forward=send_forward,
                _parse_message=NapCatConnection._parse_message,
                _broadcast=broadcast,
                plugins=None,
            )
            sent = await send_forward_and_register(napcat, store, "group_1", [{
                "sender_id": "10002", "sender_name": "Alice", "content": "long text",
            }])

            message = sent["message"]
            self.assertEqual(message["message_id"], 123)
            self.assertEqual(message["content"], "[forward]")
            self.assertFalse(message["pending"])
            self.assertEqual(message["forwards"][0]["id"], "forward-1")
            self.assertEqual(message["forwards"][0]["nodes"][0]["sender_name"], "Alice")
            self.assertEqual(broadcasts[0]["type"], "new_message")
            self.assertEqual(store.get_chats()[0]["last_text"], "[forward]")

    async def test_forward_early_echo_prevents_duplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            confirmed = {
                "chat_id": "group_1", "message_id": 123, "time": int(time.time()),
                "sender_id": 1, "sender_name": "Me", "content": "[forward]", "self": True,
            }
            store.append_simplified("group_1", confirmed)
            broadcasts = []

            async def send_forward(chat_id, nodes):
                return {"status": "ok", "data": {"message_id": 123}}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(send_forward=send_forward, _broadcast=broadcast, plugins=None)
            sent = await send_forward_and_register(
                napcat, store, "group_1", [{"message_id": "10"}],
            )

            self.assertIs(sent["message"], confirmed)
            self.assertEqual(len(store.get_messages("group_1")), 1)
            self.assertEqual(broadcasts, [])

    async def test_forward_json_echo_replaces_local_message(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            local = {
                "chat_id": "group_1",
                "message_id": None,
                "time": int(time.time()),
                "sender_id": "self",
                "sender_name": "You",
                "content": "[forward]",
                "forwards": [{
                    "id": "forward-1",
                    "title": "Forwarded messages",
                    "summary": "1 messages",
                    "status": "ok",
                    "nodes": [{"sender_name": "Alice", "content": "hello"}],
                }],
                "extra_segments": [],
                "self": True,
                "source": "user",
            }
            store.register_pending_local_message("group_1", local)
            incoming = {
                "chat_id": "group_1",
                "message_id": 123,
                "time": local["time"],
                "sender_id": 10001,
                "sender_name": "Me",
                "content": "[json card]",
                "forwards": [],
                "extra_segments": [{
                    "type": "json",
                    "label": "[json card]",
                    "description": "[\u804a\u5929\u8bb0\u5f55]",
                    "source": "\u7fa4\u804a\u7684\u804a\u5929\u8bb0\u5f55",
                }],
                "self": True,
            }

            reconciled = store.reconcile_self_message(incoming)

            self.assertTrue(reconciled["replaced"])
            self.assertEqual(len(store.get_messages("group_1")), 1)
            message = store.get_messages("group_1")[0]
            self.assertEqual(message["message_id"], 123)
            self.assertEqual(message["content"], "[forward]")
            self.assertEqual(message["forwards"], local["forwards"])
            self.assertEqual(message["extra_segments"], [])
            self.assertNotIn("pending", message)

    async def test_plugin_context_sends_forward_with_plugin_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            broadcasts = []

            async def send_forward(chat_id, nodes):
                return {"status": "ok", "data": {"message_id": 123}}

            async def broadcast(payload):
                broadcasts.append(payload)

            napcat = SimpleNamespace(
                send_forward=send_forward,
                _parse_message=NapCatConnection._parse_message,
                _broadcast=broadcast,
                plugins=None,
            )
            manager = PluginManager(tmp, {"plugins": {"enabled": {}}}, store, napcat=napcat)
            ctx = PluginContext(manager, "worker", {})

            result = await ctx.send_forward("group_1", [{
                "sender_id": "10001", "sender_name": "Worker", "content": "result",
            }])

            self.assertEqual(result["data"]["message_id"], 123)
            self.assertEqual(store.get_messages("group_1")[0]["source"], "plugin:worker")


class SendForwardHandlerTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(body, napcat, store):
        async def request_json():
            return body

        return SimpleNamespace(
            app={"config": {"web_token": ""}, "store": store, "napcat": napcat},
            query={}, cookies={}, headers={}, remote="", json=request_json,
        )

    async def test_handler_returns_validation_errors_as_bad_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            napcat = SimpleNamespace()
            response = await api.handle_send_forward(self.request(
                {"chat_id": "group_1", "nodes": []}, napcat, store,
            ))

        self.assertEqual(response.status, 400)
        self.assertIn("1 to 100", json.loads(response.text)["error"])

    async def test_handler_sends_valid_forward(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)

            async def send_forward(chat_id, nodes):
                return {"status": "ok", "data": {"message_id": 123}}

            async def broadcast(payload):
                return None

            napcat = SimpleNamespace(
                send_forward=send_forward,
                _parse_message=NapCatConnection._parse_message,
                _broadcast=broadcast,
                plugins=None,
            )
            response = await api.handle_send_forward(self.request({
                "chat_id": "group_1",
                "nodes": [{"sender_id": "10001", "sender_name": "Alice", "content": "text"}],
            }, napcat, store))

        self.assertEqual(response.status, 200)
        self.assertTrue(json.loads(response.text)["ok"])


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


class ForwardHandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_forward_payload_is_normalized_for_clients(self):
        async def fetch_forward(forward_id):
            self.assertEqual(forward_id, "forward-1")
            return {
                "status": "ok",
                "data": [{
                    "user_id": 2,
                    "nickname": "Alice",
                    "time": 10,
                    "message": [{"type": "text", "data": {"text": "inside"}}],
                }],
            }

        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            store.append_simplified("group_1", {
                "message_id": "message-1",
                "forwards": [{
                    "id": "forward-1",
                    "status": "unavailable",
                    "error": "forward unavailable",
                    "nodes": [],
                }],
            })
            request = SimpleNamespace(
                app={
                    "config": {"web_token": ""},
                    "store": store,
                    "napcat": SimpleNamespace(fetch_forward=fetch_forward),
                },
                query={"id": "forward-1"},
                cookies={},
                headers={},
                remote="",
            )
            response = await api.handle_forward(request)

        payload = json.loads(response.text)
        self.assertEqual(response.status, 200)
        self.assertEqual(payload["forward"]["status"], "ok")
        self.assertEqual(payload["forward"]["nodes"][0]["sender_name"], "Alice")
        self.assertEqual(payload["forward"]["nodes"][0]["content"], "inside")
        cached = store.get_messages("group_1")[0]["forwards"][0]
        self.assertEqual(cached, payload["forward"])
        self.assertIn("group_1", store._dirty)


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
