import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

from textual.widgets import Input, ListView

from webqq_tui_app.app import Composer, MemberPicker, WebQQTui
from webqq_tui_app.models import Chat, Message


class FakeClient:
    def __init__(self):
        self.config = SimpleNamespace(server_url="http://test", download_dir=Path("/tmp"))
        self.sent = []
        self.read = []

    async def status(self):
        return {"napcat_connected": True, "chats_count": 2, "self_user": {"name": "Me"}}

    async def chats(self):
        return [
            Chat("group_1", "A very long group name for narrow terminals", "group", 20, "latest message"),
            Chat("private_2", "Alice", "private", 10, "hello"),
        ]

    async def messages(self, chat_id, limit=50, before=None):
        if before is not None:
            return []
        return [Message.from_json({
            "chat_id": chat_id,
            "message_id": 1,
            "time": 1,
            "sender_id": 2,
            "sender_name": "Alice",
            "content": "hello from a message that wraps in a small terminal",
            "files": [{"name": "report.txt", "id": "f1"}],
        })]

    async def group_members(self, chat_id):
        return [{"user_id": 2, "display_name": "Alice", "role": "member"}]

    async def mark_read(self, chat_id):
        self.read.append(chat_id)

    async def send_message(self, chat_id, text, reply_to=""):
        self.sent.append((chat_id, text, reply_to))
        return {"ok": True}

    async def websocket(self):
        await asyncio.Event().wait()


class WebQQTuiTests(unittest.IsolatedAsyncioTestCase):
    async def wait_loaded(self, pilot, app):
        for _ in range(20):
            if app.chats:
                return
            await pilot.pause(0.05)
        self.fail("chat list did not load")

    async def test_responsive_layouts_and_minimum_size(self):
        for size, narrow, short in (
            ((120, 35), False, False),
            ((80, 24), False, False),
            ((90, 140), True, False),
            ((60, 20), True, False),
            ((40, 12), True, True),
            ((32, 10), True, True),
        ):
            with self.subTest(size=size):
                app = WebQQTui(FakeClient())
                async with app.run_test(size=size) as pilot:
                    await self.wait_loaded(pilot, app)
                    self.assertEqual(app.narrow, narrow)
                    self.assertEqual(app.short, short)
                    self.assertEqual(app.query_one("#workspace").styles.display, "block")
                    await pilot.press("enter")
                    await pilot.pause(0.1)
                    self.assertIsNotNone(app.current_chat)
                    if narrow:
                        self.assertEqual(app.query_one("#sidebar").styles.display, "none")
                    composer = app.query_one("#composer", Composer)
                    status = app.query_one("#status_bar")
                    self.assertLessEqual(composer.region.y + composer.region.height, status.region.y)

        app = WebQQTui(FakeClient())
        async with app.run_test(size=(31, 9)) as pilot:
            await pilot.pause(0.05)
            self.assertEqual(app.query_one("#workspace").styles.display, "none")
            self.assertEqual(app.query_one("#too_small").styles.display, "block")

    async def test_escape_and_refresh_preserve_chat_selection(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(120, 35)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertTrue(app.conversation_visible)

            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertFalse(app.conversation_visible)
            chat_list = app.query_one("#chat_list", ListView)
            self.assertIs(app.focused, chat_list)

            await pilot.press("j")
            self.assertEqual(chat_list.highlighted_child.chat.chat_id, "private_2")
            await app._handle_socket_event({
                "type": "new_message",
                "data": {"chat_id": "group_1", "message_id": 2, "time": 2, "content": "refresh"},
            })
            self.assertEqual(chat_list.highlighted_child.chat.chat_id, "private_2")

        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertEqual(app.query_one("#sidebar").styles.display, "block")
            self.assertEqual(app.query_one("#conversation").styles.display, "none")

    async def test_open_send_reply_filter_and_back(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertEqual(app.current_chat.chat_id, "group_1")
            self.assertEqual(client.read, ["group_1"])

            await pilot.press("r")
            composer = app.query_one("#composer", Composer)
            self.assertIsNotNone(app.reply_to)
            composer.load_text("reply text")
            await pilot.press("ctrl+j")
            self.assertIn("\n", composer.text)
            composer.load_text("reply text")
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.sent, [("group_1", "reply text", "1")])
            self.assertEqual(composer.text, "")

            app.action_find()
            search = app.query_one("#message_search", Input)
            search.value = "wraps"
            await pilot.pause(0.05)
            self.assertEqual(app._match_indexes, [0])

            app.action_back()
            self.assertEqual(search.styles.display, "none")
            app.action_back()
            self.assertFalse(app.conversation_visible)
            self.assertEqual(app.query_one("#sidebar").styles.display, "block")

    async def test_member_picker_inserts_server_mention_syntax(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            composer = app.query_one("#composer", Composer)
            composer.focus()
            composer.load_text("hello @")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, MemberPicker)
            picker = app.screen
            member_list = picker.query_one("#member_list", ListView)
            picker.on_list_view_selected(SimpleNamespace(item=member_list.children[0]))
            for _ in range(20):
                if not isinstance(app.screen, MemberPicker):
                    break
                await pilot.pause(0.05)
            await pilot.pause(0.1)
            self.assertEqual(composer.text, "hello @[2] ")

    async def test_chat_filter(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(80, 24)) as pilot:
            await self.wait_loaded(pilot, app)
            chat_filter = app.query_one("#chat_filter", Input)
            chat_filter.focus()
            chat_filter.value = "Alice"
            await pilot.pause(0.05)
            self.assertEqual(len(app.query_one("#chat_list", ListView).children), 1)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertEqual(chat_filter.value, "")
            self.assertIs(app.focused, app.query_one("#chat_list", ListView))
            self.assertEqual(len(app.query_one("#chat_list", ListView).children), 2)

    async def test_escape_unwinds_reply_composer_and_chat(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("r")
            composer = app.query_one("#composer", Composer)
            message_list = app.query_one("#message_list", ListView)
            self.assertIsNotNone(app.reply_to)
            self.assertIs(app.focused, composer)

            await pilot.press("escape")
            self.assertIsNone(app.reply_to)
            self.assertIs(app.focused, composer)
            await pilot.press("escape")
            self.assertIs(app.focused, message_list)
            self.assertTrue(app.conversation_visible)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertFalse(app.conversation_visible)
            self.assertIs(app.focused, app.query_one("#chat_list", ListView))


if __name__ == "__main__":
    unittest.main()
