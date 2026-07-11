import unittest
from unittest.mock import patch

from plugins.recent.main import MESSAGE_LIMIT, setup


class FakeContext:
    def __init__(self, messages=None, config=None):
        self.config = config or {}
        self.messages = list(messages or [])
        self.sent = []
        self.get_messages_calls = []

    def get_messages(self, chat_id, limit=50, before=None):
        self.get_messages_calls.append({"chat_id": chat_id, "limit": limit, "before": before})
        return self.messages[-limit:]

    async def send_message(self, chat_id, text, reply_to=None):
        self.sent.append({"chat_id": chat_id, "text": text, "reply_to": reply_to})


def event(message):
    return {"type": "message", "message": message}


def group_message(sender_id, sender_name, content="hello", at=1000, message_id=None, **extra):
    return {
        "chat_id": "group_123",
        "type": "group",
        "sender_id": str(sender_id),
        "sender_name": sender_name,
        "content": content,
        "time": at,
        "message_id": message_id if message_id is not None else f"m-{sender_id}-{at}-{content}",
        **extra,
    }


class RecentPluginTests(unittest.IsolatedAsyncioTestCase):
    async def test_group_rank_reads_last_1000_stored_messages(self):
        messages = [
            group_message(1, "Alice", at=1000),
            group_message(2, "Bob", at=1001),
            group_message(1, "Alice", at=1002),
        ]
        command = group_message(3, "Carol", "/recent rank", at=1003, message_id="cmd")
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        await plugin.handle_event(event(command), ctx)

        self.assertEqual(ctx.get_messages_calls, [{"chat_id": "group_123", "limit": MESSAGE_LIMIT, "before": None}])
        self.assertEqual(len(ctx.sent), 1)
        self.assertEqual(ctx.sent[0]["chat_id"], "group_123")
        self.assertEqual(
            ctx.sent[0]["text"],
            "Recent senders in the last 1000 messages:\n1. Alice (1): 2\n2. Carol (3): 1\n3. Bob (2): 1",
        )

    async def test_rank_survives_plugin_restart_when_store_has_messages(self):
        messages = [
            group_message(1, "Alice", at=1000),
            group_message(1, "Alice", at=1001),
            group_message(2, "Bob", at=1002),
        ]
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        await plugin.handle_event(event(group_message(2, "Bob", "/recent rank", at=1003)), ctx)

        self.assertIn("1. Bob (2): 2", ctx.sent[0]["text"])
        self.assertIn("2. Alice (1): 2", ctx.sent[0]["text"])

    async def test_private_rank_is_ignored(self):
        ctx = FakeContext()
        plugin = setup(ctx)
        await plugin.handle_event(event({
            "chat_id": "private_1",
            "type": "private",
            "sender_id": "1",
            "sender_name": "Alice",
            "content": "/recent rank",
            "time": 1000,
        }), ctx)
        self.assertEqual(ctx.sent, [])
        self.assertEqual(ctx.get_messages_calls, [])

    async def test_private_random_is_ignored(self):
        ctx = FakeContext()
        plugin = setup(ctx)
        await plugin.handle_event(event({
            "chat_id": "private_1",
            "type": "private",
            "sender_id": "1",
            "sender_name": "Alice",
            "content": "/recent random",
            "time": 1000,
        }), ctx)
        self.assertEqual(ctx.sent, [])
        self.assertEqual(ctx.get_messages_calls, [])

    async def test_ignored_messages_do_not_count(self):
        messages = [
            group_message(1, "Alice", at=1000, self=True),
            group_message(2, "Bob", at=1001, system=True),
            group_message(3, "Carol", at=1002, recalled=True),
        ]
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        await plugin.handle_event(event(group_message(4, "Dan", "/recent rank", at=1003)), ctx)

        self.assertEqual(
            ctx.sent[0]["text"],
            "Recent senders in the last 1000 messages:\n1. Dan (4): 1",
        )

    async def test_current_command_message_is_not_duplicated(self):
        command = group_message(1, "Alice", "/recent rank", at=1000, message_id="cmd")
        ctx = FakeContext([command])
        plugin = setup(ctx)

        await plugin.handle_event(event(command), ctx)

        self.assertEqual(
            ctx.sent[0]["text"],
            "Recent senders in the last 1000 messages:\n1. Alice (1): 1",
        )

    async def test_ties_sort_by_latest_then_sender_id(self):
        messages = [
            group_message(1, "Alice", at=1000),
            group_message(3, "Carol", at=1001),
            group_message(2, "Bob", at=1002),
        ]
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        await plugin.handle_event(event(group_message(4, "Dan", "/recent rank", at=1003)), ctx)

        self.assertEqual(
            ctx.sent[0]["text"],
            "Recent senders in the last 1000 messages:\n1. Dan (4): 1\n2. Bob (2): 1\n3. Carol (3): 1\n4. Alice (1): 1",
        )

    async def test_configured_max_rank_limits_rows(self):
        messages = [
            group_message(1, "Alice", at=1000),
            group_message(2, "Bob", at=1001),
        ]
        ctx = FakeContext(messages, config={"max_rank": 1})
        plugin = setup(ctx)

        await plugin.handle_event(event(group_message(3, "Carol", "/recent rank", at=1002)), ctx)

        self.assertEqual(
            ctx.sent[0]["text"],
            "Recent senders in the last 1000 messages:\n1. Carol (3): 1",
        )

    async def test_random_selects_weighted_sender_and_mentions_user(self):
        messages = [
            group_message(1, "Alice", at=1000),
            group_message(1, "Alice", at=1001),
            group_message(1, "Alice", at=1002),
            group_message(2, "Bob", at=1003),
        ]
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        with patch("plugins.recent.main.random.randrange", return_value=4) as randrange:
            await plugin.handle_event(event(group_message(3, "Carol", "/recent random", at=1004)), ctx)

        randrange.assert_called_once_with(5)
        self.assertEqual(ctx.get_messages_calls, [{"chat_id": "group_123", "limit": MESSAGE_LIMIT, "before": None}])
        self.assertEqual(ctx.sent[0]["text"], "Random recent sender: @[2] (Bob)")

    async def test_random_counts_current_command_once_when_not_stored(self):
        messages = [
            group_message(1, "Alice", at=1000),
        ]
        ctx = FakeContext(messages)
        plugin = setup(ctx)

        with patch("plugins.recent.main.random.randrange", return_value=0):
            await plugin.handle_event(event(group_message(2, "Bob", "/recent random", at=1001, message_id="cmd")), ctx)

        self.assertEqual(ctx.sent[0]["text"], "Random recent sender: @[2] (Bob)")

    async def test_random_does_not_duplicate_current_command_when_stored(self):
        command = group_message(2, "Bob", "/recent random", at=1001, message_id="cmd")
        ctx = FakeContext([group_message(1, "Alice", at=1000), command])
        plugin = setup(ctx)

        with patch("plugins.recent.main.random.randrange", return_value=0) as randrange:
            await plugin.handle_event(event(command), ctx)

        randrange.assert_called_once_with(2)
        self.assertEqual(ctx.sent[0]["text"], "Random recent sender: @[2] (Bob)")

    async def test_configured_random_command_is_supported(self):
        ctx = FakeContext([group_message(1, "Alice", at=1000)], config={"random_command": "/pick"})
        plugin = setup(ctx)

        with patch("plugins.recent.main.random.randrange", return_value=1):
            await plugin.handle_event(event(group_message(2, "Bob", "/pick", at=1001)), ctx)

        self.assertEqual(ctx.sent[0]["text"], "Random recent sender: @[1] (Alice)")


if __name__ == "__main__":
    unittest.main()
