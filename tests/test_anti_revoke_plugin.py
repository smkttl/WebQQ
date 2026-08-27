import asyncio
import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path


PLUGIN_PATH = Path(__file__).resolve().parents[1] / "plugins" / "anti-revoke" / "main.py"
SPEC = importlib.util.spec_from_file_location("anti_revoke_plugin", PLUGIN_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class FakeContext:
    def __init__(self, messages=None, config=None, state_path=None):
        self.config = config or {}
        self.messages = list(messages or [])
        self.state_path = Path(state_path) if state_path else Path(tempfile.mkdtemp()) / "state.json"
        self.sent = []
        self.segment_sent = []
        self.deleted = []
        self.tasks = []
        self.logs = []
        self.next_message_id = 9000
        self.last_message_id = None

    def get_messages(self, chat_id, limit=50, before=None):
        return self.messages[-limit:]

    async def send_message(self, chat_id, text, reply_to=None):
        self.sent.append({"chat_id": chat_id, "text": text, "reply_to": reply_to})

    async def send_segments(self, chat_id, segments, text="", reply_to=None):
        self.segment_sent.append({
            "chat_id": chat_id,
            "segments": segments,
            "text": text,
            "reply_to": reply_to,
        })
        self.next_message_id += 1
        self.last_message_id = str(self.next_message_id)
        return {"status": "ok", "message_id": self.last_message_id}

    def create_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self.tasks.append(task)
        return task

    async def napcat(self, action, params=None, timeout=10):
        if action == "delete_msg":
            self.deleted.append(params["message_id"])
            return {"status": "ok"}
        raise AssertionError(f"unexpected napcat action: {action}")

    def log(self, message):
        self.logs.append(message)


def event(message):
    return {"type": "message", "message": message}


def message(message_id, content, time_value, **extra):
    return {
        "chat_id": "group_123",
        "type": "group",
        "sender_id": "1",
        "sender_name": "Alice",
        "message_id": message_id,
        "content": content,
        "time": time_value,
        "system": False,
        "recalled": False,
        **extra,
    }


class AntiRevokePluginTests(unittest.IsolatedAsyncioTestCase):
    def plugin(self, ctx):
        return MODULE.AntiRevokePlugin(ctx, state_path=ctx.state_path)

    async def test_prev_reveals_recalled_message(self):
        ctx = FakeContext([
            message("a", "secret text", 100, recalled=True),
            message("b", "hello", 101),
            message("cmd", "[reply:b]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(len(ctx.segment_sent), 1)
        self.assertEqual(
            ctx.segment_sent[0]["text"],
            "Message Reproduction (Timeout: 110s): Sender: @[1] Message: secret text",
        )
        self.assertEqual(ctx.segment_sent[0]["reply_to"], "cmd")
        self.assertEqual(len(plugin.state["pending"]), 1)

    async def test_next_finds_following_recalled_message(self):
        ctx = FakeContext([
            message("a", "anchor", 100),
            message("b", "revoked next", 101, recalled=True),
            message("cmd", "[reply:a]/next", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(len(ctx.segment_sent), 1)
        self.assertEqual(
            ctx.segment_sent[0]["text"],
            "Message Reproduction (Timeout: 110s): Sender: @[1] Message: revoked next",
        )

    async def test_next_excludes_the_command_message(self):
        ctx = FakeContext([
            message("a", "anchor", 100),
            message("cmd", "[reply:a]/next", 101),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(ctx.sent, [{
            "chat_id": "group_123",
            "text": "No adjacent message found",
            "reply_to": "cmd",
        }])
        self.assertEqual(ctx.segment_sent, [])

    async def test_not_recalled_target_reports_status(self):
        ctx = FakeContext([
            message("a", "not revoked", 100),
            message("b", "anchor", 101),
            message("cmd", "[reply:b]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(ctx.sent, [{
            "chat_id": "group_123",
            "text": "The adjacent message was not revoked",
            "reply_to": "cmd",
        }])

    async def test_no_adjacent_message_reports_status(self):
        ctx = FakeContext([
            message("a", "anchor", 100),
            message("cmd", "[reply:a]/prev", 101),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(ctx.sent[0]["text"], "No adjacent message found")

    async def test_missing_anchor_uses_exact_stale_message(self):
        ctx = FakeContext([
            message("b", "hello", 101),
            message("cmd", "[reply:unknown]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(ctx.sent[0]["text"], "sorry, the message is too stale")

    async def test_system_notices_are_skipped(self):
        ctx = FakeContext([
            message("a", "anchor", 100),
            message("sys", "recalled a message", 100.5, system=True),
            message("b", "revoked next", 101, recalled=True),
            message("cmd", "[reply:a]/next", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(len(ctx.segment_sent), 1)
        self.assertEqual(
            ctx.segment_sent[0]["text"],
            "Message Reproduction (Timeout: 110s): Sender: @[1] Message: revoked next",
        )

    async def test_private_and_self_messages_are_ignored(self):
        ctx = FakeContext()
        plugin = self.plugin(ctx)
        await plugin.handle_event(event({
            "chat_id": "private_1",
            "type": "private",
            "sender_id": "1",
            "sender_name": "Alice",
            "content": "[reply:1]/prev",
            "time": 100,
        }), ctx)
        await plugin.handle_event(event({
            "chat_id": "group_123",
            "type": "group",
            "self": True,
            "sender_id": "1",
            "content": "[reply:1]/prev",
            "time": 100,
        }), ctx)
        self.assertEqual(ctx.sent, [])
        self.assertEqual(ctx.segment_sent, [])

    async def test_command_without_quote_receives_help(self):
        ctx = FakeContext([message("cmd", "/prev", 100)])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(
            ctx.sent[0]["text"],
            "Please reply to a message with /prev or /next.",
        )

    async def test_media_reproduction_builds_segments(self):
        target = message("a", "[face:478] hello @[42]", 100, recalled=True)
        target["images"] = [{"url": "https://example.com/a.png"}]
        target["extra_segments"] = [{
            "type": "music",
            "label": "[music]",
            "title": "Song",
            "url": "https://example.com/jump",
            "audio": "https://example.com/audio.mp3",
        }]
        ctx = FakeContext([
            target,
            message("b", "anchor", 101),
            message("cmd", "[reply:b]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        segments = ctx.segment_sent[0]["segments"]
        self.assertEqual(
            segments[0],
            {"type": "text", "data": {"text": "Message Reproduction (Timeout: 110s): Sender: "}},
        )
        self.assertEqual(segments[1], {"type": "at", "data": {"qq": "1"}})
        self.assertTrue(any(
            segment.get("type") == "text"
            and "Message: " in segment.get("data", {}).get("text", "")
            for segment in segments
        ))
        self.assertIn({"type": "face", "data": {"id": "478"}}, segments)
        self.assertIn({"type": "at", "data": {"qq": "42"}}, segments)
        self.assertIn({"type": "image", "data": {"file": "https://example.com/a.png"}}, segments)
        music = next(segment for segment in segments if segment["type"] == "music")
        self.assertEqual(music["data"]["type"], "custom")
        self.assertEqual(music["data"]["title"], "Song")

    async def test_reproduction_pings_the_original_sender(self):
        target = message("a", "secret", 100, sender_id=42, recalled=True)
        ctx = FakeContext([
            target,
            message("b", "anchor", 101),
            message("cmd", "[reply:b]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        segments = ctx.segment_sent[0]["segments"]
        self.assertEqual(segments[1], {"type": "at", "data": {"qq": "42"}})
        self.assertEqual(
            ctx.segment_sent[0]["text"],
            "Message Reproduction (Timeout: 110s): Sender: @[42] Message: secret",
        )

    async def test_reproduction_timeout_uses_configured_delay(self):
        ctx = FakeContext(
            [
                message("a", "secret", 100, recalled=True),
                message("b", "anchor", 101),
                message("cmd", "[reply:b]/prev", 102),
            ],
            config={"recall_delay_seconds": 60},
        )
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertEqual(
            ctx.segment_sent[0]["text"],
            "Message Reproduction (Timeout: 60s): Sender: @[1] Message: secret",
        )

    async def test_unsupported_card_becomes_placeholder(self):
        target = message("a", "", 100, recalled=True)
        target["extra_segments"] = [{"type": "json", "label": "[json]", "title": "Card"}]
        ctx = FakeContext([
            target,
            message("b", "anchor", 101),
            message("cmd", "[reply:b]/prev", 102),
        ])
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        self.assertIn("[json]: Card", ctx.segment_sent[0]["text"])

    async def test_reproduction_is_recalled_after_delay(self):
        ctx = FakeContext(
            [
                message("a", "secret", 100, recalled=True),
                message("b", "anchor", 101),
                message("cmd", "[reply:b]/prev", 102),
            ],
            config={"recall_delay_seconds": 0.05},
        )
        plugin = self.plugin(ctx)

        await plugin.handle_event(event(ctx.messages[-1]), ctx)

        sent_id = int(ctx.last_message_id)
        await asyncio.sleep(0.1)
        self.assertEqual(ctx.deleted, [sent_id])
        self.assertEqual(plugin.state["pending"], [])

    async def test_pending_recall_rearms_after_restart(self):
        state = {
            "pending": [{
                "message_id": "777",
                "chat_id": "group_123",
                "deadline": time.time() + 0.05,
            }]
        }
        state_path = Path(tempfile.mkdtemp()) / "state.json"
        with open(state_path, "w", encoding="utf-8") as stream:
            json.dump(state, stream)
        ctx = FakeContext(state_path=state_path)

        plugin = self.plugin(ctx)
        await asyncio.sleep(0.1)

        self.assertEqual(ctx.deleted, [777])
        self.assertEqual(plugin.state["pending"], [])


if __name__ == "__main__":
    unittest.main()
