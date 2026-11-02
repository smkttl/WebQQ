import unittest
from types import SimpleNamespace

from webqq_app.common import (
    backend_capabilities,
    normalize_config,
)
from webqq_app.napcat import (
    NapCatConnection,
    SnowLumaConnection,
    create_backend,
)
from webqq_app.request_store import ContactRequestStore
from webqq_app.store import MessageStore
import json
import tempfile
import webqq_app.api as api


class BackendConfigTests(unittest.TestCase):
    def test_missing_backend_defaults_to_napcat(self):
        self.assertEqual(normalize_config({})["backend"], "napcat")

    def test_ws_token_takes_precedence_and_legacy_token_remains_supported(self):
        self.assertEqual(
            normalize_config({"ws_token": "new", "napcat_token": "old"})["ws_token"],
            "new",
        )
        self.assertEqual(
            normalize_config({"napcat_token": "old"})["ws_token"],
            "old",
        )

    def test_invalid_backend_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "invalid backend"):
            normalize_config({"backend": "other"})

    def test_factory_selects_backend(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = MessageStore(maxlen=10, data_dir=tmp)
            napcat = create_backend({"backend": "napcat", "ws_url": "ws://x"}, store)
            snowluma = create_backend({"backend": "snowluma", "ws_url": "ws://x"}, store)
        self.assertIsInstance(napcat, NapCatConnection)
        self.assertIsInstance(snowluma, SnowLumaConnection)
        self.assertEqual(napcat.backend_name, "NapCat")
        self.assertEqual(snowluma.backend_name, "SnowLuma")

    def test_snowluma_capabilities(self):
        capabilities = backend_capabilities("snowluma")
        self.assertFalse(capabilities["online_file_transfer"])
        self.assertTrue(capabilities["voice_transcription"])
        self.assertTrue(capabilities["window_vibration"])
        self.assertFalse(capabilities["collection_create"])


class SnowLumaAdapterTests(unittest.IsolatedAsyncioTestCase):
    def connection(self, store=None):
        return SnowLumaConnection(
            "", "", store or SimpleNamespace(),
            request_store=SimpleNamespace(),
        )

    async def test_mark_read_uses_latest_stored_message_id(self):
        store = SimpleNamespace(
            get_messages=lambda chat_id, limit=1: [{"message_id": 42}],
        )
        connection = self.connection(store)
        calls = []

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.mark_chat_read("group_7")
        await connection.mark_chat_read("private_8")

        self.assertEqual(calls, [
            ("mark_group_msg_as_read", {"group_id": "7", "message_id": 42}, 5),
            ("mark_private_msg_as_read", {"user_id": "8", "message_id": 42}, 5),
        ])

    async def test_mark_read_with_no_local_messages_is_noop(self):
        connection = self.connection(SimpleNamespace(get_messages=lambda *args, **kwargs: []))
        calls = []

        async def request(action, params, timeout=10):
            calls.append(action)

        connection._request = request
        result = await connection.mark_chat_read("group_7")

        self.assertEqual(result, {"status": "ok", "data": None})
        self.assertEqual(calls, [])

    async def test_group_system_messages_are_normalized(self):
        connection = self.connection()
        calls = []
        broadcasts = []
        connection.request_store = SimpleNamespace(
            upsert=lambda event, source="event": (
                {**event, "id": "r1"}, True,
            ),
            pending_count=lambda: 1,
        )

        async def request(action, params, timeout=10):
            calls.append(action)
            if action == "get_group_system_msg":
                return {"status": "ok", "data": [{
                    "flag": "f1", "invitor_uin": 10, "requester_uin": 20,
                    "requester_nick": "Alice", "message": "hi",
                }]}
            if action == "get_doubt_friends_add_request":
                return {"status": "ok", "data": [{
                    "uid": "u1", "user_id": 30, "nick": "Bob", "reqTime": 99,
                }]}
            return {"status": "ok", "data": []}

        async def broadcast(payload):
            broadcasts.append(payload)

        connection._request = request
        connection._broadcast = broadcast
        await connection._fetch_contact_requests()

        self.assertEqual(calls, ["get_group_system_msg", "get_doubt_friends_add_request"])
        self.assertEqual(len(broadcasts), 2)

    async def test_move_group_file_uses_snowluma_parameter_names(self):
        connection = self.connection()
        calls = []

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok"}

        connection._request = request
        await connection.move_group_file("7", "file", "/from", "/to")

        self.assertEqual(calls, [("move_group_file", {
            "group_id": "7", "file_id": "file",
            "parent_directory": "/from", "target_directory": "/to",
        }, 30)])

    async def test_ignored_notifications_are_filtered(self):
        connection = self.connection()

        async def request(action, params, timeout=10):
            return {"status": "ok", "data": [
                {"group_id": 7, "flag": "a"}, {"group_id": 8, "flag": "b"},
            ]}

        connection._request = request
        response = await connection.group_content(7, "ignored")

        self.assertEqual(response["data"]["notifies"], [{"group_id": 7, "flag": "a"}])

    async def test_snowluma_voice_and_window_shake_use_supported_actions(self):
        connection = self.connection()
        calls = []

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            return {"status": "ok", "data": {"text": "hello"}}

        async def send_segments(chat_id, message, timeout=10):
            calls.append((chat_id, message, timeout))
            return {"status": "ok", "data": {}}

        connection._request = request
        connection.send_segments = send_segments
        await connection.fetch_ptt_text("42")
        await connection.send_window_vibration("private_8")

        self.assertEqual(calls, [
            ("fetch_ptt_text", {"message_id": "42"}, 30),
            ("private_8", [{"type": "shake", "data": {}}], 10),
        ])

    async def test_snowluma_group_request_includes_sub_type(self):
        with tempfile.TemporaryDirectory() as tmp:
            request_store = ContactRequestStore(tmp)
            record, _ = request_store.upsert({
                "request_type": "group", "sub_type": "invite", "flag": "g1",
            })
            connection = SnowLumaConnection(
                "", "", SimpleNamespace(), request_store=request_store,
            )
            calls = []

            async def request(action, params, timeout=10):
                calls.append((action, params, timeout))
                return {"status": "ok"}

            async def broadcast(payload):
                return None

            connection._request = request
            connection._broadcast = broadcast
            await connection.act_on_contact_request(record["id"], True)

            self.assertEqual(calls, [("set_group_add_request", {
                "flag": "g1", "approve": True, "sub_type": "invite",
            }, 30)])

    async def test_snowluma_can_reject_doubtful_friend_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            request_store = ContactRequestStore(tmp)
            record, _ = request_store.upsert({
                "request_type": "friend", "sub_type": "add", "flag": "u1",
            }, source="doubt_backfill")
            connection = SnowLumaConnection(
                "", "", SimpleNamespace(), request_store=request_store,
            )
            calls = []

            async def request(action, params, timeout=10):
                calls.append((action, params, timeout))
                return {"status": "ok"}

            async def broadcast(payload):
                return None

            connection._request = request
            connection._broadcast = broadcast
            updated, _ = await connection.act_on_contact_request(record["id"], False)

            self.assertEqual(calls, [("set_doubt_friends_add_request", {
                "flag": "u1", "approve": False,
            }, 30)])
            self.assertEqual(updated["status"], "rejected")

    async def test_unavailable_features_fail_before_upstream_request(self):
        connection = self.connection()

        with self.assertRaisesRegex(RuntimeError, "SnowLuma"):
            await connection.create_collection("brief", "data")


class SnowLumaApiTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(connection, body=None):
        async def request_json():
            return dict(body or {})

        return SimpleNamespace(
            app={
                "config": {"web_token": "", "backend": "snowluma"},
                "napcat": connection,
            },
            query={}, cookies={}, headers={}, remote="", json=request_json,
        )

    async def test_unsupported_online_transfers_return_501_before_upstream_call(self):
        connection = SnowLumaConnection("", "", SimpleNamespace())
        calls = []

        async def get_online_files(chat_id):
            calls.append(chat_id)
            raise AssertionError("SnowLuma online-file action was invoked")

        connection.get_online_files = get_online_files
        response = await api.handle_online_files(self.request(connection))
        payload = json.loads(response.text)

        self.assertEqual(response.status, 501)
        self.assertEqual(payload["capability"], "online_file_transfer")
        self.assertFalse(payload["ok"])
        self.assertEqual(calls, [])

    async def test_unsupported_collection_creation_returns_501_before_validation(self):
        connection = SnowLumaConnection("", "", SimpleNamespace())
        calls = []

        async def create_collection(brief, raw_data):
            calls.append((brief, raw_data))
            raise AssertionError("SnowLuma collection action was invoked")

        connection.create_collection = create_collection
        response = await api.handle_collection_create(self.request(connection, {
            "brief": "title", "raw_data": "body",
        }))
        payload = json.loads(response.text)

        self.assertEqual(response.status, 501)
        self.assertEqual(payload["capability"], "collection_create")
        self.assertEqual(calls, [])


if __name__ == "__main__":
    unittest.main()
