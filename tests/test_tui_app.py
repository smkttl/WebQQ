import asyncio
import unittest
from pathlib import Path
from types import SimpleNamespace

from textual.widgets import Input, ListView, Static

from webqq_tui_app.app import Composer, FaceReplyPicker, ForwardViewer, MemberPicker, RichMediaDialog, WebQQTui
from webqq_tui_app.models import Chat, Message


class FakeClient:
    def __init__(self):
        self.config = SimpleNamespace(server_url="http://test", download_dir=Path("/tmp"))
        self.sent = []
        self.poked = []
        self.reactions = []
        self.forward_ids = []
        self.read = []
        self.images = []
        self.rich_media = []

    async def status(self):
        return {"napcat_connected": True, "chats_count": 2, "self_user": {"user_id": 1, "name": "Me"}}

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

    async def send_image(self, chat_id, path):
        self.images.append((chat_id, path))
        return {"ok": True}

    async def send_video(self, chat_id, path):
        self.rich_media.append(("video", chat_id, path))
        return {"ok": True}

    async def send_voice(self, chat_id, path):
        self.rich_media.append(("voice", chat_id, path))
        return {"ok": True}

    async def send_contact(self, chat_id, contact_type, contact_id):
        self.rich_media.append(("contact", chat_id, contact_type, contact_id))
        return {"ok": True}

    async def send_music(self, chat_id, music):
        self.rich_media.append(("music", chat_id, music))
        return {"ok": True}

    async def poke(self, chat_id, user_id):
        self.poked.append((chat_id, user_id))
        return {"ok": True}

    async def send_face_reply(self, chat_id, message_id, emoji_id):
        self.reactions.append((chat_id, message_id, emoji_id))
        return {
            "ok": True,
            "message_id": message_id,
            "reactions": [{"emoji_id": emoji_id, "count": 1}],
        }

    async def forward(self, forward_id):
        self.forward_ids.append(forward_id)
        return {
            "id": forward_id,
            "title": "Saved thread",
            "status": "ok",
            "nodes": [
                {"sender_id": 2, "sender_name": "Alice", "time": 2, "content": "first"},
                {"sender_id": 3, "sender_name": "Bob", "time": 3, "content": "second"},
            ],
        }

    async def websocket(self):
        await asyncio.Event().wait()


class SlowHistoryClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.older_started = asyncio.Event()
        self.older_release = asyncio.Event()

    async def messages(self, chat_id, limit=50, before=None):
        if before is not None:
            self.older_started.set()
            await self.older_release.wait()
            return [Message.from_json({
                "chat_id": chat_id,
                "message_id": 99,
                "time": 0.5,
                "sender_id": 2,
                "sender_name": "Alice",
                "content": "older",
            })]
        return await super().messages(chat_id, limit=limit, before=before)


class WebQQTuiTests(unittest.IsolatedAsyncioTestCase):
    def test_rich_media_command_parser(self):
        self.assertEqual(RichMediaDialog.parse_command('video "/tmp/a b.mp4"'), {"kind": "video", "path": "/tmp/a b.mp4"})
        self.assertEqual(RichMediaDialog.parse_command("contact group 123"), {"kind": "contact", "type": "group", "id": "123"})
        custom = RichMediaDialog.parse_command('music custom {"url":"https://p","audio":"https://a","title":"T"}')
        self.assertEqual(custom["music"]["type"], "custom")

    def test_internal_text_selection_is_disabled_for_stable_mouse_events(self):
        self.assertFalse(WebQQTui.ALLOW_SELECT)

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

    async def test_stale_history_load_does_not_modify_new_chat(self):
        client = SlowHistoryClient()
        app = WebQQTui(client)
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.no_more_messages = False
            history_task = asyncio.create_task(app._load_older())
            await client.older_started.wait()

            await app._open_chat(app.chats[1])
            client.older_release.set()
            await history_task

            self.assertEqual(app.current_chat.chat_id, "private_2")
            self.assertTrue(app.messages)
            self.assertTrue(all(message.chat_id == "private_2" for message in app.messages))

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

    async def test_poke_selected_sender_and_keep_p_as_composer_text(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)

            await pilot.press("p")
            await pilot.pause(0.05)
            self.assertEqual(client.poked, [("group_1", "2")])

            composer = app.query_one("#composer", Composer)
            composer.focus()
            await pilot.press("p")
            self.assertEqual(composer.text, "p")
            self.assertEqual(client.poked, [("group_1", "2")])

            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": 2, "sender_id": 1,
                "sender_name": "Me", "content": "self", "self": True,
            })]
            await app._render_messages(select_last=True)
            app.query_one("#message_list", ListView).focus()
            app.action_poke()
            await pilot.pause(0.05)

            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": "system-1", "sender_id": 999,
                "sender_name": "System", "content": "notice", "system": True,
            })]
            await app._render_messages(select_last=True)
            app.action_poke()
            await pilot.pause(0.05)
            self.assertEqual(client.poked, [("group_1", "2")])

    async def test_ctrl_i_sends_image_and_escape_closes_prompt_in_narrow_layout(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)

            await pilot.press("ctrl+i")
            image_prompt = app.query_one("#image_path", Input)
            self.assertEqual(image_prompt.styles.display, "block")
            self.assertIs(app.focused, image_prompt)
            await pilot.press("escape")
            self.assertEqual(image_prompt.styles.display, "none")
            self.assertIs(app.focused, app.query_one("#composer", Composer))

            await pilot.press("ctrl+i")
            image_prompt.value = "/tmp/photo.png"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.images, [("group_1", Path("/tmp/photo.png"))])

    async def test_f3_media_dialog_sends_contact_and_escapes(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f3")
            self.assertIsInstance(app.screen, RichMediaDialog)
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, RichMediaDialog)
            await pilot.press("f3")
            command = app.screen.query_one("#media_command", Input)
            command.value = "contact qq 123"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.rich_media, [("contact", "group_1", "qq", "123")])

    async def test_face_reply_picker_filters_sends_and_escapes_on_small_terminal(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)

            await pilot.press("e")
            await pilot.pause(0.05)
            self.assertIsInstance(app.screen, FaceReplyPicker)
            self.assertLessEqual(app.screen.query_one("#face_list", ListView).region.right, app.size.width)
            face_filter = app.screen.query_one("#face_filter", Input)
            face_filter.value = "微笑"
            await pilot.pause(0.05)
            await pilot.press("enter")
            for _ in range(20):
                if not isinstance(app.screen, FaceReplyPicker) and client.reactions:
                    break
                await pilot.pause(0.05)
            self.assertEqual(client.reactions, [("group_1", "1", "14")])
            self.assertEqual(app.messages[0].reactions[0]["emoji_id"], "14")

            app.query_one("#message_list", ListView).focus()
            await pilot.press("e")
            await pilot.pause(0.05)
            self.assertIsInstance(app.screen, FaceReplyPicker)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertNotIsInstance(app.screen, FaceReplyPicker)
            self.assertEqual(client.reactions, [("group_1", "1", "14")])

    async def test_enter_opens_and_lazy_loads_forward_on_small_terminal(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.messages = [Message.from_json({
                "chat_id": "group_1",
                "message_id": 2,
                "sender_id": 2,
                "sender_name": "Alice",
                "content": "[forward]",
                "forwards": [{
                    "id": "forward-1",
                    "title": "Saved thread",
                    "status": "unavailable",
                    "error": "initial load failed",
                    "nodes": [],
                }],
            })]
            await app._render_messages(select_last=True)
            app.query_one("#message_list", ListView).focus()

            await pilot.press("enter")
            for _ in range(20):
                if isinstance(app.screen, ForwardViewer) and len(app.screen.query_one("#forward_list", ListView).children) == 2:
                    break
                await pilot.pause(0.05)

            self.assertIsInstance(app.screen, ForwardViewer)
            self.assertEqual(client.forward_ids, ["forward-1"])
            self.assertEqual(len(app.screen.query_one("#forward_list", ListView).children), 2)
            self.assertLessEqual(app.screen.query_one("#forward_list", ListView).region.right, app.size.width)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertNotIsInstance(app.screen, ForwardViewer)
            self.assertEqual(len(app.messages[0].forwards[0]["nodes"]), 2)

    async def test_open_chat_hydrates_unavailable_forward_without_opening_it(self):
        class ForwardClient(FakeClient):
            async def messages(self, chat_id, limit=50, before=None):
                return [Message.from_json({
                    "chat_id": chat_id,
                    "message_id": 2,
                    "sender_id": 2,
                    "sender_name": "Alice",
                    "content": "[forward]",
                    "forwards": [{
                        "id": "forward-1",
                        "status": "unavailable",
                        "error": "initial load failed",
                        "nodes": [],
                    }],
                })]

        client = ForwardClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            for _ in range(20):
                if app.messages and len(app.messages[0].forwards[0].get("nodes", [])) == 2:
                    break
                await pilot.pause(0.05)

            self.assertEqual(client.forward_ids, ["forward-1"])
            self.assertEqual(len(app.messages[0].forwards[0]["nodes"]), 2)
            row = app.query_one("#message_list", ListView).children[0]
            self.assertIn("2 messages", row.query_one(Static).render().plain)

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
