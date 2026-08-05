import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from plugins.td.main import handle_event


class TdPluginTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = SimpleNamespace(
            send_message=AsyncMock(),
            log=unittest.mock.Mock(),
        )

    async def dispatch(self, overrides=None):
        message = {
            "chat_id": "group_123",
            "type": "group",
            "content": "notice @[all]",
            "mentions": {"all": "全体成员"},
            "self": False,
            "source": "",
            **(overrides or {}),
        }
        await handle_event({"type": "message", "message": message}, self.ctx)

    async def test_replies_td_to_all_mention(self):
        await self.dispatch()

        self.ctx.send_message.assert_awaited_once_with("group_123", "td")

    async def test_accepts_normalized_all_token_without_mentions_map(self):
        await self.dispatch({"mentions": {}})

        self.ctx.send_message.assert_awaited_once_with("group_123", "td")

    async def test_ignores_regular_mentions_and_private_messages(self):
        await self.dispatch({"content": "hello @[42]", "mentions": {"42": "Alice"}})
        await self.dispatch({"chat_id": "private_42", "type": "private"})

        self.ctx.send_message.assert_not_awaited()

    async def test_ignores_self_and_plugin_messages(self):
        await self.dispatch({"self": True})
        await self.dispatch({"source": "plugin:worker"})

        self.ctx.send_message.assert_not_awaited()

    async def test_logs_send_failures(self):
        self.ctx.send_message.side_effect = RuntimeError("offline")

        await self.dispatch()

        self.ctx.log.assert_called_once_with("send failed: offline")
