import asyncio
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

from webqq_app import api
from webqq_app.common import normalize_message_id
from webqq_app.messaging import normalize_forward_nodes
from webqq_app.napcat import NapCatConnection, SnowLumaConnection
from webqq_app import repair_messages as repair
from webqq_app.store import MessageStore


def event(mid=1, sender=20, target=None, timestamp=None, **extra):
    result = {
        "post_type": "message", "message_type": "private", "sub_type": "friend",
        "message_id": mid, "user_id": sender, "time": timestamp if timestamp is not None else abs(mid),
        "sender": {"user_id": sender, "nickname": str(sender)},
        "message": [{"type": "text", "data": {"text": f"message {mid}"}}],
    }
    if target is not None:
        result["target_id"] = target
    return {**result, **extra}


class DirectionAndHistoryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = MessageStore(maxlen=4, data_dir=self.temporary.name)
        self.store.set_self_user(10, "Me")

    def test_direction_uses_sender_not_recipient(self):
        for target in (None, 10, "10"):
            with self.subTest(target=target):
                self.assertFalse(self.store._simplify(event(target=target))["self"])
                self.assertFalse(self.store._simplify(event(target=target, self_user_id=10))["self"])
        for own in (10, "10"):
            result = self.store._simplify(event(sender=own, target=20))
            self.assertTrue(result["self"])
            self.assertEqual(result["chat_id"], "private_20")
            self.assertEqual(result["user_id"], 20)
            self.assertEqual(result["target_id"], 20)
            self.assertNotEqual(result["chat_name"], "Me")
        self.assertFalse(self.store._simplify(event(message_type="group", group_id=30))["self"])
        self.assertTrue(self.store._simplify(event(sender=10, message_type="group", group_id=30))["self"])
        self.assertTrue(self.store._simplify(event(sender=10, target=10))["self"])
        self.assertEqual(self.store.chat_key(event(sender=10, target=10)), "private_10")

    def test_direction_with_no_identity_or_explicit_sent_marker(self):
        self.store._self_user = {"user_id": "self"}
        self.assertFalse(self.store._simplify(event())["self"])
        self.assertTrue(self.store._simplify(event(sender="10", self_id=10))["self"])
        self.assertTrue(self.store._simplify(event(sender=10, self_user_id="10"))["self"])
        self.assertTrue(self.store._simplify(event(post_type="message_sent", target=30))["self"])
        self.assertTrue(self.store._simplify(event(message_sent_type="self"))["self"])

    async def test_private_history_normalizes_both_directions_and_cursor(self):
        backend = SnowLumaConnection("", "", self.store)
        inbound = event(mid=-1)
        outbound = event(mid=-2, sender=10, target=20)
        del inbound["post_type"]
        del outbound["post_type"]
        backend._request = AsyncMock(return_value={"status": "ok", "data": {"messages": [inbound, outbound]}})
        history = await backend.fetch_history("private_20", -3)
        params = backend._request.call_args.args[1]
        self.assertEqual(params["message_id"], "-3")
        self.assertNotIn("message_seq", params)
        self.store.add_history_messages(history)
        messages = self.store.get_messages("private_20")
        self.assertEqual([m["message_id"] for m in messages], [-1, -2])
        self.assertEqual([m["self"] for m in messages], [False, True])
        self.assertNotIn("private_10", self.store._data)
        self.assertEqual(messages[-1]["target_id"], 20)

    async def test_history_initializes_identity_and_rejects_wrong_session(self):
        self.store._self_user = {"user_id": "self"}
        backend = SnowLumaConnection("", "", self.store)
        backend._request = AsyncMock(side_effect=[
            {"status": "ok", "data": {"user_id": 10}},
            {"status": "ok", "data": {"messages": [
                event(mid=1, sender=10), event(mid=2, sender=10, target=99),
                event(mid=3, sender=99), event(mid=4, message_type="group", group_id=20),
            ]}},
        ])
        messages = await backend.fetch_history("private_20")
        self.assertEqual([m["message_id"] for m in messages], [1])
        self.assertEqual(messages[0]["target_id"], 20)
        self.assertEqual(messages[0]["post_type"], "message_sent")
        self.assertEqual(self.store._self_user["user_id"], "10")

    async def test_history_fails_closed_without_identity(self):
        self.store._self_user = {"user_id": "self"}
        backend = SnowLumaConnection("", "", self.store)
        backend._request = AsyncMock(return_value={"status": "failed"})
        self.assertEqual(await backend.fetch_history("private_20"), [])
        self.assertEqual(backend._request.await_count, 1)

    async def test_napcat_cursor_is_unchanged(self):
        backend = NapCatConnection("", "", self.store)
        backend._request = AsyncMock(return_value={"status": "ok", "data": {"messages": []}})
        await backend.fetch_history("group_30", -42)
        params = backend._request.call_args.args[1]
        self.assertEqual(params["message_seq"], "-42")
        self.assertNotIn("message_id", params)

    async def test_group_history_directions(self):
        backend = SnowLumaConnection("", "", self.store)
        backend._request = AsyncMock(return_value={"status": "ok", "data": {"messages": [
            event(mid=1, message_type="group", group_id=30),
            event(mid=2, sender=10, message_type="group", group_id=30),
        ]}})
        self.store.add_history_messages(await backend.fetch_history("group_30"))
        self.assertEqual([m["self"] for m in self.store.get_messages("group_30")], [False, True])

    def test_history_merge_orders_deduplicates_and_keeps_recent_messages(self):
        self.store.add(event(8))
        self.store.add(event(9))
        self.store.add_history_messages([event(3), event(2), event(1), event(3)])
        self.assertEqual([m["message_id"] for m in self.store.get_messages("private_20")], [2, 3, 8, 9])
        self.assertEqual(self.store.oldest_message_id("private_20"), 2)
        self.assertEqual(self.store._chat_meta["private_20"]["last_time"], 9)
        self.assertEqual(self.store._chat_meta["private_20"]["last_text"], "message 9")
        self.store.add_history_messages([event(1), event(2), event(3)])
        self.assertEqual([m["message_id"] for m in self.store.get_messages("private_20")], [2, 3, 8, 9])
        self.assertNotIn("1", self.store._message_chat_index)

    def test_equal_timestamps_keep_stable_order_across_reload(self):
        self.store.add_history_messages([event(9, timestamp=5), event(-8, timestamp=5), event(2, timestamp=5)])
        self.assertEqual([m["message_id"] for m in self.store.get_messages("private_20")], [9, -8, 2])
        self.store.flush()
        loaded = MessageStore(data_dir=self.temporary.name)
        loaded.load_all()
        self.assertEqual([m["message_id"] for m in loaded.get_messages("private_20")], [9, -8, 2])


class MessageIdTests(unittest.IsolatedAsyncioTestCase):
    def test_signed_ids_and_malformed_values(self):
        for value, expected in [(42, "42"), (-42, "-42"), (" -0042 ", "-42")]:
            self.assertEqual(normalize_message_id(value), expected)
            self.assertEqual(normalize_forward_nodes([{"message_id": value}]), [{"message_id": expected}])
        for value in (0, "0", "-0", True, False, 1.5, 1.0, "+1", "--1", "1.0", "local-1", "system-1", "١", "", None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    normalize_message_id(value)
                with self.assertRaises(ValueError):
                    normalize_forward_nodes([{"message_id": value}])

    async def test_both_reply_endpoints_accept_signed_ids_and_reject_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MessageStore(data_dir=directory)
            backend = SimpleNamespace(send_message=AsyncMock(return_value={"status": "ok", "data": {"message_id": -88}}),
                                      _broadcast=AsyncMock(), plugins=None)
            plugins = SimpleNamespace(dispatch_portal_message=AsyncMock())
            app = {"config": {"web_token": ""}, "napcat": backend, "store": store, "plugins": plugins}
            for endpoint in (api.handle_send, api.handle_plugin_portal_message):
                for value, status in [(-42, 200), ("42", 200), (0, 400), (False, 400), (1.0, 400), ("--2", 400), ("local-1", 400)]:
                    request = SimpleNamespace(app=app, headers={}, query={}, cookies={}, match_info={"plugin_id": "test"},
                                              json=AsyncMock(return_value={"chat_id": "private_20", "text": "reply", "reply_to": value}))
                    response = await endpoint(request)
                    self.assertEqual(response.status, status, (endpoint.__name__, value, response.text))
            self.assertEqual(backend.send_message.call_args_list[0].kwargs["reply_to"], "-42")
            self.assertEqual(plugins.dispatch_portal_message.call_args_list[0].args[1]["reply_to"], "-42")


class FakeSocket:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.sent = asyncio.Queue()
        self.entered = asyncio.Event()

    async def __aenter__(self):
        self.entered.set()
        return self

    async def __aexit__(self, *args):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()

    async def send_json(self, data):
        self.sent.put_nowait(data)

    def emit(self, data):
        self.queue.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(data)))

    def close(self):
        self.queue.put_nowait(SimpleNamespace(type=aiohttp.WSMsgType.CLOSED))


class SocketLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.store = MessageStore(data_dir=self.temporary.name)
        self.backend = SnowLumaConnection("ws://test", "", self.store)
        self.backend._fetch_contacts = AsyncMock()
        self.socket = FakeSocket()
        self.backend.session = SimpleNamespace(ws_connect=lambda *a, **k: self.socket)
        self.connection = asyncio.create_task(self.backend._connect_and_listen())
        await self.socket.entered.wait()

    async def asyncTearDown(self):
        self.socket.close()
        await asyncio.wait_for(self.connection, 1)

    async def test_forward_expansion_and_requests_work_without_blocking_reader(self):
        broadcasts = []
        completed = asyncio.Event()

        async def broadcast(data):
            broadcasts.append(data)
            if len(broadcasts) == 2:
                completed.set()

        self.backend._broadcast = broadcast
        self.socket.emit(event(1, message_type="group", group_id=30, self_id=10,
                               message=[{"type": "forward", "data": {"id": "resource"}}]))
        forward = await asyncio.wait_for(self.socket.sent.get(), 1)
        self.assertEqual(forward["action"], "get_forward_msg")
        self.socket.emit(event(2, message_type="group", group_id=30))
        request = asyncio.create_task(self.backend._request("get_login_info", {}))
        login = await asyncio.wait_for(self.socket.sent.get(), 1)
        self.socket.emit({"echo": login["echo"], "status": "ok", "data": {"user_id": 10}})
        self.assertEqual((await asyncio.wait_for(request, 1))["data"]["user_id"], 10)
        self.assertEqual(broadcasts, [])
        self.socket.emit({"echo": forward["echo"], "status": "ok", "data": {"messages": [
            {"user_id": 20, "nickname": "Peer", "content": [{"type": "text", "data": {"text": "forwarded"}}]},
        ]}})
        await asyncio.wait_for(completed.wait(), 1)
        self.assertEqual([b["data"]["message_id"] for b in broadcasts], [1, 2])
        self.assertEqual(broadcasts[0]["data"]["forwards"][0]["nodes"][0]["content"], "forwarded")
        self.assertEqual(self.store._self_user["user_id"], "10")

    async def test_forward_timeout_does_not_kill_worker(self):
        original = self.backend._request
        async def short_request(action, params, timeout=10):
            return await original(action, params, timeout=.01)
        self.backend._request = short_request
        finished = asyncio.Event()
        async def broadcast(data):
            if data["data"]["message_id"] == 2:
                finished.set()
        self.backend._broadcast = broadcast
        self.socket.emit(event(1, message=[{"type": "forward", "data": {"id": "resource"}}]))
        self.socket.emit(event(2))
        await asyncio.wait_for(finished.wait(), 1)
        self.assertEqual(self.store.find_message(1)["message"]["forwards"][0]["status"], "unavailable")
        self.assertEqual(self.backend._pending, {})

    async def test_disconnect_settles_requests_streams_and_initialization_tasks(self):
        cancelled = asyncio.Event()
        async def background():
            try:
                await asyncio.Future()
            finally:
                cancelled.set()
        task = self.backend._start_connection_task(background())
        await asyncio.sleep(0)
        request = asyncio.create_task(self.backend._request("get_msg", {"message_id": 1}))
        stream = asyncio.create_task(self.backend._stream_file("file"))
        await self.socket.sent.get()
        await self.socket.sent.get()
        self.socket.close()
        await asyncio.wait_for(self.connection, 1)
        self.assertIsNone(await asyncio.wait_for(request, 1))
        self.assertIsNone(await asyncio.wait_for(stream, 1))
        self.assertTrue(task.cancelled())
        self.assertTrue(cancelled.is_set())
        self.assertIsNone(self.backend.ws)
        self.assertEqual(self.backend._pending, {})
        self.assertEqual(self.backend._stream_pending, {})
        # A stale echo must never resolve a request on the next connection.
        self.socket = FakeSocket()
        self.connection = asyncio.create_task(self.backend._connect_and_listen())
        await self.socket.entered.wait()
        next_request = asyncio.create_task(self.backend._request("get_msg", {"message_id": 2}))
        call = await self.socket.sent.get()
        self.socket.emit({"echo": "stale-echo", "status": "ok", "data": {"message_id": 1}})
        self.socket.emit({"echo": call["echo"], "status": "ok", "data": {"message_id": 2}})
        self.assertEqual((await asyncio.wait_for(next_request, 1))["data"]["message_id"], 2)

    async def test_cancelled_and_failed_sends_clear_pending_entries(self):
        request = asyncio.create_task(self.backend._request("get_msg", {}))
        await self.socket.sent.get()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertEqual(self.backend._pending, {})
        self.socket.send_json = AsyncMock(side_effect=ConnectionError("closed"))
        with self.assertRaises(ConnectionError):
            await self.backend._request("get_msg", {})
        with self.assertRaises(ConnectionError):
            await self.backend._stream_file("file")
        self.assertEqual(self.backend._pending, {})
        self.assertEqual(self.backend._stream_pending, {})


class RepairTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def write(self, name, messages):
        (self.root / name).write_text(json.dumps(messages))

    def saved(self, mid, sender=10, **extra):
        return {"message_id": mid, "sender_id": sender, "self": True, "time": abs(mid),
                "content": str(mid), "type": "private", "chat_id": "private_10", **extra}

    async def test_safe_repair_backup_and_idempotence(self):
        self.write("private_10.json", [self.saved(-1, images=[{"url": "image"}], reactions=[{"count": 1}]),
                                       self.saved(-2), self.saved(-3)])
        self.write("private_20.json", [self.saved(4, sender=20), self.saved(5, sender="self"),
                                       self.saved(6, sender="system", type="system")])
        before = {p.name: p.read_bytes() for p in self.root.glob("*.json")}
        async def lookup(action, params):
            if action == "get_login_info":
                return {"user_id": 10}
            mid = params["message_id"]
            if mid == -1:
                return {"message_id": mid, "message_type": "private", "user_id": 10, "target_id": 20, "time": abs(mid)}
            if mid == -3:
                return {"message_id": mid, "message_type": "private", "user_id": 10, "target_id": 10, "time": abs(mid)}
            return None
        changes, report = await repair.plan_repair(self.root, lookup)
        self.assertEqual({p.name: p.read_bytes() for p in self.root.glob("*.json")}, before)
        self.assertEqual(report["direction_changes"], 1)
        self.assertEqual(len(report["moved"]), 1)
        self.assertEqual(len(report["unresolved"]), 1)
        backup = repair.apply_repair(self.root, changes, report)
        for name, raw in before.items():
            self.assertEqual((backup / name).read_bytes(), raw)
        manifest = json.loads((backup / "manifest.json").read_text())
        self.assertEqual(manifest["state"], "applied")
        peer = json.loads((self.root / "private_20.json").read_text())
        moved = peer[0]
        self.assertEqual(moved["message_id"], -1)
        self.assertEqual(moved["images"], [{"url": "image"}])
        self.assertEqual(moved["reactions"], [{"count": 1}])
        self.assertFalse(peer[1]["self"])
        self.assertTrue(peer[2]["self"])
        self.assertTrue(peer[3]["self"])
        self.assertEqual(sum(len(json.loads(p.read_text())) for p in self.root.glob("*.json")), 6)
        repeated, _ = await repair.plan_repair(self.root, lookup)
        self.assertEqual(repeated, {})

    async def test_repair_leaves_conflicts_mismatches_and_unknown_recipients(self):
        self.write("private_10.json", [self.saved(-1), self.saved(-2), self.saved(-3), self.saved(-4, sender="self")])
        self.write("private_20.json", [self.saved(-1, content="conflict")])
        async def lookup(action, params):
            if action == "get_login_info":
                return {"user_id": 10}
            mid = params["message_id"]
            if mid == -3:
                raise TimeoutError("lookup timed out")
            return {"message_id": mid, "message_type": "private", "user_id": 10, "target_id": 20,
                    "time": 999 if mid == -2 else abs(mid)}
        changes, report = await repair.plan_repair(self.root, lookup)
        self.assertEqual(changes, {})
        self.assertEqual(len(report["unresolved"]), 3)
        self.assertEqual(report["moved"], [])

    async def test_repair_deduplicates_only_verified_copies_without_trimming(self):
        self.write("private_10.json", [self.saved(-1, target_id=20, images=[{"url": "extra"}])])
        self.write("private_20.json", [self.saved(-1), *[self.saved(i, sender=20) for i in range(1, 1100)]])
        lookup = AsyncMock(return_value={"user_id": 10})
        changes, report = await repair.plan_repair(self.root, lookup)
        repair.apply_repair(self.root, changes, report)
        messages = json.loads((self.root / "private_20.json").read_text())
        self.assertEqual(len(messages), 1100)
        self.assertEqual(messages[0]["images"], [{"url": "extra"}])
        self.assertEqual(report["duplicates_removed"], 1)
        self.assertEqual(lookup.await_count, 1)

    async def test_repair_requires_verified_account(self):
        self.write("private_10.json", [self.saved(1)])
        with self.assertRaisesRegex(RuntimeError, "cannot verify"):
            await repair.plan_repair(self.root, AsyncMock(return_value=None))
        self.assertFalse((self.root / "repair-backups").exists())

    def test_atomic_apply_detects_concurrent_writes_and_rolls_back_failures(self):
        original = b"[1]"
        self.root.joinpath("private_10.json").write_bytes(original)
        changes = {"private_10.json": (original, b"[]"), "private_20.json": (None, b"[1]")}
        self.root.joinpath("private_10.json").write_bytes(b"[2]")
        with self.assertRaisesRegex(RuntimeError, "changed during repair"):
            repair.apply_repair(self.root, changes, {})
        self.root.joinpath("private_10.json").write_bytes(original)
        atomic = repair.atomic_write
        def fail_once(path, raw):
            if path == self.root / "private_20.json":
                raise OSError("disk full")
            return atomic(path, raw)
        with patch.object(repair, "atomic_write", side_effect=fail_once):
            with self.assertRaisesRegex(RuntimeError, "recovery manifest"):
                repair.apply_repair(self.root, changes, {})
        self.assertEqual(self.root.joinpath("private_10.json").read_bytes(), original)
        self.assertFalse(self.root.joinpath("private_20.json").exists())
        manifest = next(self.root.glob("repair-backups/*/manifest.json"))
        self.assertEqual(json.loads(manifest.read_text())["state"], "rolled_back")
