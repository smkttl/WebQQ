import asyncio
import io
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from textual import events
from textual.widgets import Button, Input, ListView, Static
from PIL import Image

from webqq_tui_app.app import CollectionBrowser, Composer, CustomFacePicker, FaceReplyPicker, ForwardViewer, FriendRemarkDialog, GroupFileManager, GroupManager, HelpPanel, MemberPicker, MessageListItem, MessageListView, OnlineTransferManager, RichMediaDialog, WebQQTui
from webqq_tui_app.management import ActionPalette, ConfirmDialog, ContactsManager, ForwardComposer, PluginManagerScreen
from webqq_tui_app.models import Chat, Message


def static_plain(widget):
    renderable = getattr(widget, "renderable", None)
    if renderable is not None:
        return getattr(renderable, "plain", str(renderable))
    rendered = widget.render()
    return getattr(rendered, "plain", str(rendered))


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
        self.transcriptions = []
        self.group_file_calls = []
        self.group_management_calls = []
        self.remarks = []
        self.custom_faces_sent = []
        self.forwards_sent = []
        self.revoked = []
        self.temp_chats = []
        self.portal_sent = []
        self.online_actions = []
        self.games = []
        self.image_fetches = 0

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

    async def send_portal_message(self, plugin_id, chat_id, text, reply_to=""):
        self.portal_sent.append((plugin_id, chat_id, text, reply_to))
        return {"ok": True}

    async def send_forward(self, chat_id, nodes):
        self.forwards_sent.append((chat_id, nodes))
        return {"ok": True}

    async def revoke_message(self, chat_id, message_id):
        self.revoked.append((chat_id, message_id))
        return {"ok": True, "message": {"chat_id": chat_id, "message_id": message_id, "recalled": True}}

    async def start_temp_chat(self, group_id, user_id, name="", group_name=""):
        self.temp_chats.append((group_id, user_id, name, group_name))
        return {"ok": True, "chat_id": "private_{}".format(user_id), "name": name}

    async def reaction_details(self, chat_id, message_id, emoji_id=""):
        return [{"emoji_id": "14", "count": 1, "users": [{"user_id": "2", "name": "Alice"}]}]

    async def plugins(self):
        return [{"id": "echo", "enabled": True, "loaded": True, "portal_receiver": True}]

    def _attachment_params(self, chat_id, attachment):
        return {"url": str(attachment.data.get("url") or "")}

    async def fetch_bytes(self, path, params=None):
        self.image_fetches += 1
        image = Image.new("RGB", (4, 4), (20, 120, 180))
        body = io.BytesIO()
        image.save(body, format="PNG")
        return body.getvalue(), "image/png"

    async def contact_requests(self, status="", request_type=""):
        return {"ok": True, "requests": [{"id": "r1", "status": "pending", "request_type": "friend", "user_id": "3"}], "pending_count": 1}

    async def contact_settings(self):
        return {"ok": True, "auto_approve_requests": False, "pending_count": 1}

    async def friends(self):
        return {"ok": True, "categories": [{"name": "Friends", "friends": [{"user_id": "2", "nickname": "Alice"}]}]}

    async def profile(self):
        return {"user_id": "1", "nickname": "Me", "personal_note": "Hi"}

    async def send_image(self, chat_id, path):
        self.images.append((chat_id, path))
        return {"ok": True}

    async def send_video(self, chat_id, path):
        self.rich_media.append(("video", chat_id, path))
        return {"ok": True}

    async def send_voice(self, chat_id, path):
        self.rich_media.append(("voice", chat_id, path))
        return {"ok": True}

    async def send_online_file(self, chat_id, path):
        self.rich_media.append(("online", chat_id, path))
        return {"ok": True}

    async def send_online_folder(self, chat_id, path):
        self.rich_media.append(("online_folder", chat_id, path))
        return {"ok": True}

    async def online_files(self, chat_id):
        return [{"message_id": "m1", "element_id": "e1", "name": "offer.zip", "size": "10", "direction": "incoming"}]

    async def online_file_action(self, chat_id, action, message_id, element_id=""):
        self.online_actions.append((chat_id, action, message_id, element_id))
        return {"ok": True}

    async def send_game(self, chat_id, game, result=None):
        self.games.append((chat_id, game, result))
        return {"ok": True}

    async def send_contact(self, chat_id, contact_type, contact_id):
        self.rich_media.append(("contact", chat_id, contact_type, contact_id))
        return {"ok": True}

    async def send_music(self, chat_id, music):
        self.rich_media.append(("music", chat_id, music))
        return {"ok": True}

    async def custom_faces(self, count=48):
        return [{"id": "0123456789abcdef01234567", "url": "https://example/face.png"}]

    async def send_custom_face(self, chat_id, face_id):
        self.custom_faces_sent.append((chat_id, face_id))
        return {"ok": True}

    async def collections(self, category=0, count=50):
        return [{"id": "one", "brief": "Saved", "text": "Collection body"}]

    async def transcribe_message(self, chat_id, message_id):
        self.transcriptions.append((chat_id, message_id))
        return {"ok": True, "message": {
            "chat_id": chat_id, "message_id": message_id, "time": 1, "sender_name": "Alice",
            "content": "[voice]", "records": [{"file": "voice.amr", "transcript": "spoken words"}],
        }}

    async def group_files(self, chat_id, folder_id=""):
        self.group_file_calls.append(("list", chat_id, folder_id))
        return {
            "ok": True, "packet_available": False, "info": {"file_count": 1},
            "folders": [{"folder_id": "dir", "folder_name": "Docs", "total_file_count": 1}],
            "files": [{"file_id": "file", "file_name": "readme.txt", "file_size": 10}],
        }

    async def group_dashboard(self, chat_id):
        self.group_management_calls.append(("dashboard", chat_id))
        return {
            "ok": True, "role": "admin", "can_manage": True, "can_manage_admins": False,
            "info": {"group_name": "Group"}, "detail": {"member_count": 2, "group_all_shut": 0},
            "at_all": {"remain_at_all_count_for_group": 3, "remain_at_all_count_for_uin": 1},
            "muted": [], "packet_available": False,
        }

    async def group_content(self, chat_id, kind, **params):
        self.group_management_calls.append(("content", chat_id, kind, params))
        return {"ok": True, "kind": kind, "data": []}

    async def group_action(self, chat_id, action, **values):
        self.group_management_calls.append(("action", chat_id, action, values))
        return {"ok": True}

    async def upload_group_album_image(self, chat_id, album_id, album_name, path):
        self.group_management_calls.append(("upload", chat_id, album_id, album_name, path))
        return {"ok": True}

    async def download_group_file(self, chat_id, file, progress=None):
        self.group_file_calls.append(("download", chat_id, file["file_id"]))
        return Path("/tmp/readme.txt")

    async def update_friend_remark(self, user_id, remark):
        self.remarks.append((user_id, remark))
        return {"ok": True, "name": remark or "Alice", "remark": remark, "nickname": "Alice"}

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


class PagedHistoryClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.history_before = []

    async def messages(self, chat_id, limit=50, before=None):
        if before is None:
            values = range(101, 151)
        elif before > 51:
            self.history_before.append(before)
            values = range(51, 101)
        else:
            self.history_before.append(before)
            values = range(1, 21)
        return [Message.from_json({
            "chat_id": chat_id, "message_id": value, "time": value,
            "sender_id": 2, "sender_name": "Alice", "content": "message {}".format(value),
        }) for value in values]


class WebQQTuiTests(unittest.IsolatedAsyncioTestCase):
    def test_rich_media_command_parser(self):
        self.assertEqual(RichMediaDialog.parse_command('video "/tmp/a b.mp4"'), {"kind": "video", "path": "/tmp/a b.mp4"})
        self.assertEqual(RichMediaDialog.parse_command("contact group 123"), {"kind": "contact", "type": "group", "id": "123"})
        custom = RichMediaDialog.parse_command('music custom {"url":"https://p","audio":"https://a","title":"T"}')
        self.assertEqual(custom["music"]["type"], "custom")
        self.assertEqual(RichMediaDialog.parse_command("faces"), {"kind": "faces"})
        self.assertEqual(RichMediaDialog.parse_command("collections"), {"kind": "collections"})
        mini_app = RichMediaDialog.parse_command(
            'miniapp bili {"title":"T","picUrl":"https://p","jumpUrl":"https://j"}',
        )
        self.assertEqual(mini_app["mini_app"]["mode"], "bili")
        collection = RichMediaDialog.parse_command(
            'collection create {"brief":"B","raw_data":"R"}',
        )
        self.assertEqual(collection["collection"]["raw_data"], "R")
        self.assertEqual(RichMediaDialog.parse_command("collection save"), {"kind": "collection_save"})
        self.assertEqual(RichMediaDialog.parse_command('online "/tmp/a b"'), {"kind": "online", "path": "/tmp/a b"})
        self.assertEqual(RichMediaDialog.parse_command("online-folder /tmp/docs"), {"kind": "online_folder", "path": "/tmp/docs"})
        self.assertEqual(RichMediaDialog.parse_command("transfers"), {"kind": "transfers"})
        self.assertEqual(RichMediaDialog.parse_command("dice"), {"kind": "dice", "result": None})
        self.assertEqual(RichMediaDialog.parse_command("dice 6"), {"kind": "dice", "result": "6"})
        self.assertEqual(RichMediaDialog.parse_command("rps scissors"), {"kind": "rps", "result": "scissors"})
        with self.assertRaises(ValueError):
            RichMediaDialog.parse_command("dice 7")

    async def test_f3_sends_forced_rps(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f3")
            command = app.screen.query_one("#media_command", Input)
            command.value = "rps scissors"
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertEqual(client.games, [("group_1", "rps", "scissors")])

    async def test_online_transfer_manager_fits_small_terminal_and_receives(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("down", "enter")
            await pilot.pause(0.1)
            await pilot.press("f3")
            command = app.screen.query_one("#media_command", Input)
            command.value = "transfers"
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, OnlineTransferManager)
            await pilot.press("a")
            await pilot.pause(0.1)
            self.assertEqual(client.online_actions, [("private_2", "receive", "m1", "e1")])
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, OnlineTransferManager)

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

    async def test_help_panel_opens_and_returns_at_minimum_size(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("?")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, HelpPanel)
            help_list = app.screen.query_one("#help_list", ListView)
            self.assertGreater(len(help_list.children), 20)
            self.assertIs(app.focused, help_list)
            await pilot.press("j", "j", "k")
            self.assertGreater(help_list.index, 0)
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, HelpPanel)

            await pilot.press("f1")
            self.assertIsInstance(app.screen, HelpPanel)
            await pilot.press("escape")

    async def test_long_message_folds_and_toggles_at_minimum_size(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            long_body = "Long content " * 100
            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": 77, "time": 1,
                "sender_id": 2, "sender_name": "Alice", "content": long_body,
                "files": [{"name": "visible.txt", "size": 10, "id": "f"}],
            })]
            await app._render_messages(select_last=True)
            item = app.query_one("#message_list", ListView).highlighted_child
            self.assertIsInstance(item, MessageListItem)
            self.assertTrue(item.is_long)
            self.assertFalse(item.expanded)
            self.assertIn("visible.txt", static_plain(item.query_one(Static)))
            self.assertNotIn(long_body.strip(), static_plain(item.query_one(Static)))

            fold_button = item.query_one(Button)
            fold_button.scroll_visible(animate=False)
            await pilot.pause(0.05)
            clicked = await pilot.click(fold_button)
            self.assertTrue(clicked)
            await pilot.pause(0.1)
            self.assertTrue(item.expanded)
            self.assertIn(long_body.strip(), static_plain(item.query_one(Static)))
            self.assertEqual(str(item.query_one(Button).label), "Collapse message")

            await app._render_messages()
            view = app.query_one("#message_list", ListView)
            item = view.highlighted_child
            self.assertTrue(item.expanded)
            view.focus()
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertFalse(item.expanded)
            self.assertEqual(str(item.query_one(Button).label), "Show full message")

            search = app.query_one("#message_search", Input)
            search.value = "Long content"
            await pilot.pause(0.2)
            item = app.query_one("#message_list", ListView).highlighted_child
            self.assertFalse(item.is_long)
            self.assertIn(long_body.strip(), static_plain(item.query_one(Static)))
            self.assertEqual(len(item.query(Button)), 0)

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

    async def test_new_message_only_follows_when_view_is_at_bottom(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": index, "time": index,
                "sender_name": "Alice", "content": "message {}".format(index),
            }) for index in range(1, 31)]
            await app._render_messages(select_last=True)
            await pilot.pause(0.1)

            view = app.query_one("#message_list", ListView)
            view.scroll_to(y=0, animate=False, immediate=True)
            await pilot.pause(0.05)
            self.assertFalse(app._message_view_follows_latest())

            await app._handle_socket_event({
                "type": "new_message",
                "data": {
                    "chat_id": "group_1", "message_id": 31, "time": 31,
                    "sender_name": "Alice", "content": "message 31",
                },
            })
            await pilot.pause(0.1)
            self.assertEqual(view.scroll_y, 0)
            self.assertEqual(view.highlighted_child.message.message_id, "30")

            view.index = len(view.children) - 1
            view.scroll_to(y=view.max_scroll_y, animate=False, immediate=True)
            await pilot.pause(0.05)
            self.assertTrue(app._message_view_follows_latest())
            await app._handle_socket_event({
                "type": "new_message",
                "data": {
                    "chat_id": "group_1", "message_id": 32, "time": 32,
                    "sender_name": "Alice", "content": "message 32",
                },
            })
            await pilot.pause(0.1)
            self.assertEqual(view.highlighted_child.message.message_id, "32")
            self.assertTrue(view.is_vertical_scroll_end)

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

    async def test_scrolling_above_loaded_messages_fetches_earlier_history(self):
        client = PagedHistoryClient()
        app = WebQQTui(client)
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            view = app.query_one("#message_list", MessageListView)

            view.index = 0
            view.focus()
            await pilot.press("k")
            for _ in range(30):
                if len(app.messages) == 100:
                    break
                await pilot.pause(0.05)
            self.assertEqual(len(app.messages), 100)
            self.assertEqual(view.highlighted_child.message.message_id, "101")

            view.scroll_to(y=0, animate=False, immediate=True)
            view.post_message(events.MouseScrollUp(
                view, 1, 1, 0, -1, 0, False, False, False,
            ))
            for _ in range(30):
                if len(app.messages) == 120 and view.highlighted_child is not None:
                    break
                await pilot.pause(0.05)
            self.assertEqual(len(app.messages), 120)
            self.assertEqual(view.highlighted_child.message.message_id, "51")
            self.assertEqual(len(client.history_before), 2)

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

    async def test_f3_face_and_collection_browsers_fit_small_terminal(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f3")
            command = app.screen.query_one("#media_command", Input)
            command.value = "faces"
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, CustomFacePicker)
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertEqual(client.custom_faces_sent, [("group_1", "0123456789abcdef01234567")])

            await pilot.press("f3")
            command = app.screen.query_one("#media_command", Input)
            command.value = "collections"
            await pilot.press("enter")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, CollectionBrowser)
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, CollectionBrowser)

    async def test_t_transcribes_selected_voice(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": 7, "time": 1, "sender_name": "Alice",
                "content": "[voice]", "records": [{"file": "voice.amr"}],
            })]
            await app._render_messages(select_last=True)
            await pilot.press("t")
            await pilot.pause(0.05)
            self.assertEqual(client.transcriptions, [("group_1", "7")])
            self.assertEqual(app.messages[0].attachments[0].data["transcript"], "spoken words")

    async def test_f4_group_file_manager_is_small_terminal_safe(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f4")
            await pilot.pause(0.05)
            self.assertIsInstance(app.screen, GroupFileManager)
            self.assertLessEqual(app.screen.query_one("#group_file_list", ListView).region.right, app.size.width)
            self.assertEqual(client.group_file_calls[0], ("list", "group_1", ""))
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, GroupFileManager)

    async def test_f5_group_manager_is_small_terminal_safe(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f5")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, GroupManager)
            self.assertLessEqual(app.screen.query_one("#group_manage_list", ListView).region.right, app.size.width)
            self.assertEqual(client.group_management_calls[0], ("dashboard", "group_1"))
            await pilot.press("3")
            await pilot.pause(0.1)
            self.assertTrue(any(call[:3] == ("content", "group_1", "notices") for call in client.group_management_calls))
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, GroupManager)

    async def test_f5_group_manager_contains_backend_load_failure(self):
        class FailingGroupClient(FakeClient):
            async def group_dashboard(self, chat_id):
                raise RuntimeError("NapCat unavailable")

        app = WebQQTui(FailingGroupClient())
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("f5")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, GroupManager)
            self.assertIn("error", static_plain(app.screen.query_one("#group_manage_title", Static)).lower())
            await pilot.press("escape")
            self.assertNotIsInstance(app.screen, GroupManager)

    async def test_f2_updates_and_clears_private_friend_remark(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("down", "enter")
            await pilot.pause(0.1)
            await pilot.press("f2")
            self.assertIsInstance(app.screen, FriendRemarkDialog)
            field = app.screen.query_one("#friend_remark", Input)
            field.value = "Best friend"
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.remarks, [("2", "Best friend")])
            self.assertEqual(app.current_chat.name, "Best friend")
            await pilot.press("f2")
            field = app.screen.query_one("#friend_remark", Input)
            field.value = ""
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.remarks[-1], ("2", ""))
            self.assertEqual(app.current_chat.name, "Alice")

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
            face_filter = app.screen.query_one("#face_filter", Input)
            face_filter.value = "478"
            matching_face = None
            for _ in range(30):
                matching_face = app.screen.query_one("#face_list", ListView).highlighted_child
                if matching_face is not None and matching_face.emoji_id == "478":
                    break
                await pilot.pause(0.05)
            self.assertEqual(matching_face.emoji_id, "478")
            await pilot.press("enter")
            for _ in range(20):
                if not isinstance(app.screen, FaceReplyPicker) and len(client.reactions) == 2:
                    break
                await pilot.pause(0.05)
            self.assertEqual(client.reactions[-1], ("group_1", "1", "478"))

            app.query_one("#message_list", ListView).focus()
            await pilot.press("e")
            await pilot.pause(0.05)
            self.assertIsInstance(app.screen, FaceReplyPicker)
            await pilot.press("escape")
            await pilot.pause(0.05)
            self.assertNotIsInstance(app.screen, FaceReplyPicker)
            self.assertEqual(len(client.reactions), 2)

    async def test_x_revokes_selected_message_but_remains_typable_in_composer(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)

            composer = app.query_one("#composer", Composer)
            composer.focus()
            await pilot.press("x")
            self.assertEqual(composer.text, "x")
            self.assertFalse(client.revoked)

            composer.load_text("")
            app.query_one("#message_list", ListView).focus()
            await pilot.press("x")
            await pilot.pause(0.05)
            self.assertIsInstance(app.screen, ConfirmDialog)
            await pilot.press("y")
            for _ in range(20):
                if client.revoked:
                    break
                await pilot.pause(0.05)
            self.assertEqual(client.revoked, [("group_1", "1")])
            self.assertTrue(app.messages[0].recalled)

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
            self.assertIn("2 messages", static_plain(row.query_one(Static)))

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

    async def test_action_palette_message_selection_and_forward(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(40, 12)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("space")
            await pilot.pause(0.1)
            self.assertEqual(len(app._selected_message_ids), 1)
            item = app.query_one("#message_list", ListView).highlighted_child
            self.assertIn("selected", static_plain(item.query_one(Static)))

            await pilot.press("ctrl+p")
            self.assertIsInstance(app.screen, ActionPalette)
            action_filter = app.screen.query_one("#action_filter", Input)
            action_filter.value = "forward selected"
            await pilot.pause(0.1)
            await pilot.press("down", "enter")
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, ForwardComposer)
            chats = app.screen.query_one("#forward_chats", ListView)
            await app.screen.on_list_view_selected(SimpleNamespace(list_view=chats, item=chats.children[0]))
            await pilot.pause(0.1)
            await pilot.press("ctrl+s")
            await pilot.pause(0.1)
            self.assertEqual(client.forwards_sent[0][1], [{"message_id": "1"}])
            self.assertFalse(app._selected_message_ids)

    async def test_palette_opens_contacts_plugins_and_portal_send(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(32, 10)) as pilot:
            await self.wait_loaded(pilot, app)
            app.action_contacts()
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, ContactsManager)
            self.assertLessEqual(app.screen.query_one("#contact_list", ListView).region.right, app.size.width)
            await pilot.press("escape")

            app.action_plugins()
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, PluginManagerScreen)
            await pilot.press("escape")

            await pilot.press("enter")
            await pilot.pause(0.1)
            app.action_send_target()
            await pilot.pause(0.1)
            self.assertIsInstance(app.screen, ActionPalette)
            portal_list = app.screen.query_one("#action_list", ListView)
            portal_list.index = 1
            await pilot.press("enter")
            await pilot.pause(0.05)
            composer = app.query_one("#composer", Composer)
            composer.load_text("through plugin")
            composer.focus()
            await pilot.press("enter")
            await pilot.pause(0.05)
            self.assertEqual(client.portal_sent, [("echo", "group_1", "through plugin", "")])

    async def test_escape_cancels_message_selection_before_leaving_chat(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            await pilot.press("space")
            self.assertTrue(app._selected_message_ids)
            await pilot.press("escape")
            await pilot.pause(0.1)
            self.assertFalse(app._selected_message_ids)
            self.assertTrue(app.conversation_visible)

    async def test_inline_image_preview_is_removed_when_terminal_becomes_narrow(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(100, 30)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": 8, "sender_id": 2,
                "sender_name": "Alice", "content": "[image]",
                "images": [{"name": "photo.png", "url": "https://example.test/photo.png"}],
            })]
            await app._render_messages(select_last=True)
            await pilot.pause(0.1)
            thumbnail = app.query_one(".message-thumbnail", Static)
            self.assertIn("▀", static_plain(thumbnail))
            self.assertLessEqual(thumbnail.region.height, 5)
            self.assertEqual(client.image_fetches, 1)

            await pilot.resize_terminal(60, 20)
            await pilot.pause(0.1)
            self.assertTrue(app.narrow)
            self.assertEqual(len(app.query(".message-thumbnail")), 0)
            self.assertEqual(client.image_fetches, 1)

    async def test_narrow_terminal_uses_image_attachment_fallback_without_loading_preview(self):
        client = FakeClient()
        app = WebQQTui(client)
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            await pilot.press("enter")
            await pilot.pause(0.1)
            app.messages = [Message.from_json({
                "chat_id": "group_1", "message_id": 8, "sender_name": "Alice",
                "content": "[image]", "images": [{"name": "photo.png", "url": "https://example.test/photo.png"}],
            })]
            await app._render_messages(select_last=True)
            await pilot.pause(0.1)
            self.assertEqual(len(app.query(".message-thumbnail")), 0)
            self.assertEqual(client.image_fetches, 0)
            self.assertIn("[image: photo.png]", static_plain(app.query_one(MessageListItem).query_one(Static)))

    async def test_theme_toggle_and_contact_badge_update(self):
        app = WebQQTui(FakeClient())
        async with app.run_test(size=(60, 20)) as pilot:
            await self.wait_loaded(pilot, app)
            original = app._theme
            original_background = app.screen.styles.background
            with patch("webqq_tui_app.app.save_tui_preferences") as save:
                app.action_toggle_theme()
            await pilot.pause(0.05)
            self.assertNotEqual(app._theme, original)
            self.assertEqual(app.has_class("light"), app._theme == "light")
            self.assertNotEqual(app.screen.styles.background, original_background)
            save.assert_called_once_with({"theme": app._theme})

            await app._handle_socket_event({
                "type": "contact_request_update",
                "pending_count": 3,
                "data": {"id": "request", "status": "pending"},
            })
            self.assertEqual(app._pending_contact_requests, 3)
            self.assertIn("3 pending contacts", app._base_status)


if __name__ == "__main__":
    unittest.main()
