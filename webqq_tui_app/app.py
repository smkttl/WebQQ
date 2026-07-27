import asyncio
import json
import time
from pathlib import Path
from typing import Any, Coroutine, List, Mapping, Optional, Set

import aiohttp
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.css.query import NoMatches
from textual.message import Message as TextualMessage
from textual.screen import ModalScreen
from textual.widgets import Input, ListItem, ListView, Static, TextArea

from .client import AuthenticationError, WebQQClient, WebQQClientError
from .models import (
    Attachment,
    Chat,
    Message,
    deduplicate_messages,
    display_content,
    format_chat,
    format_message,
    forward_nodes,
    forward_status_label,
    message_matches,
)


class ChatListItem(ListItem):
    def __init__(self, chat: Chat, compact: bool = False):
        super().__init__(Static(format_chat(chat, compact=compact), markup=False))
        self.chat = chat


class MessageListItem(ListItem):
    def __init__(self, message: Message, compact: bool = False, search: str = ""):
        super().__init__(Static(format_message(message, compact=compact, search=search), markup=False))
        self.message = message


class MemberListItem(ListItem):
    def __init__(self, member: Mapping[str, Any]):
        self.member = dict(member)
        user_id = str(member.get("user_id") or member.get("uid") or "")
        name = str(
            member.get("display_name")
            or member.get("card")
            or member.get("nickname")
            or member.get("name")
            or user_id
        )
        role = str(member.get("role") or "")
        label = Text(name, style="bold")
        label.append("  " + user_id, style="dim")
        if role and role != "member":
            label.append("  " + role, style="yellow")
        super().__init__(Static(label, markup=False))


class AttachmentListItem(ListItem):
    def __init__(self, attachment: Attachment):
        self.attachment = attachment
        super().__init__(Static("{}: {}".format(attachment.kind, attachment.name), markup=False))


class NavigableListView(ListView):
    BINDINGS = [
        Binding("j", "cursor_down", show=False),
        Binding("k", "cursor_up", show=False),
    ]


class MessageListView(NavigableListView):
    class LoadOlder(TextualMessage):
        pass

    BINDINGS = NavigableListView.BINDINGS + [Binding("pageup", "load_older", show=False)]

    def action_load_older(self) -> None:
        self.post_message(self.LoadOlder())


class Composer(TextArea):
    class Submit(TextualMessage):
        pass

    BINDINGS = [Binding("ctrl+j", "newline", show=False)]

    async def _on_key(self, event: events.Key) -> None:
        # TextArea inserts a newline before its normal binding dispatch for Enter.
        if event.key == "enter":
            event.stop()
            event.prevent_default()
            self.post_message(self.Submit())
            return
        await super()._on_key(event)

    def action_submit(self) -> None:
        self.post_message(self.Submit())

    def action_newline(self) -> None:
        self.insert("\n")


class MemberPicker(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    MemberPicker { align: center middle; background: $background 70%; }
    MemberPicker > Container { width: 72; max-width: 92%; height: 24; max-height: 88%; border: solid $accent; background: $surface; padding: 1; }
    MemberPicker Input { margin-bottom: 1; }
    MemberPicker ListView { height: 1fr; }
    MemberPicker .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, members: List[Mapping[str, Any]]):
        super().__init__()
        self.members = members

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Mention group member", classes="dialog-title")
            yield Input(placeholder="Filter by name or QQ number", id="member_filter")
            yield NavigableListView(id="member_list")
            yield Static("Enter select  Esc cancel", classes="hint")

    async def on_mount(self) -> None:
        await self._render_members("")
        self.query_one("#member_filter", Input).focus()

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "member_filter":
            await self._render_members(event.value)

    async def _render_members(self, query: str) -> None:
        query = query.strip().casefold()
        matches = []
        for member in self.members:
            haystack = " ".join(
                str(member.get(key) or "")
                for key in ("user_id", "uid", "display_name", "card", "nickname", "name")
            ).casefold()
            if not query or query in haystack:
                matches.append(MemberListItem(member))
        view = self.query_one("#member_list", ListView)
        await view.clear()
        if matches:
            await view.extend(matches)
            view.index = 0

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, MemberListItem):
            self.dismiss(event.item.member)

    def action_cancel(self) -> None:
        self.dismiss(None)


class AttachmentPicker(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    AttachmentPicker { align: center middle; background: $background 70%; }
    AttachmentPicker > Container { width: 72; max-width: 92%; height: auto; max-height: 80%; border: solid $accent; background: $surface; padding: 1; }
    AttachmentPicker ListView { height: auto; max-height: 18; }
    AttachmentPicker .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, attachments: List[Attachment]):
        super().__init__()
        self.attachments = attachments

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Download attachment", classes="dialog-title")
            yield NavigableListView(*(AttachmentListItem(item) for item in self.attachments), id="attachment_list")
            yield Static("Enter download  Esc cancel", classes="hint")

    def on_mount(self) -> None:
        self.query_one("#attachment_list", ListView).focus()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, AttachmentListItem):
            self.dismiss(event.item.attachment)

    def action_cancel(self) -> None:
        self.dismiss(None)


class ForwardNodeListItem(ListItem):
    def __init__(self, node: Mapping[str, Any], compact: bool = False):
        self.message = Message.from_json(node)
        super().__init__(Static(format_message(self.message, compact=compact), markup=False))


class ForwardViewer(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    ForwardViewer { align: center middle; background: $background 70%; }
    ForwardViewer > Container { width: 96; max-width: 96%; height: 90%; min-height: 6; border: solid $accent; background: $surface; padding: 1; }
    ForwardViewer #forward_title { height: 2; text-style: bold; }
    ForwardViewer #forward_list { height: 1fr; }
    ForwardViewer #forward_list > ListItem { height: auto; min-height: 3; padding: 0 1 1 1; }
    ForwardViewer .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient, forward: Mapping[str, Any]):
        super().__init__()
        self.client = client
        self.forward = forward if isinstance(forward, dict) else dict(forward)

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Forwarded messages", id="forward_title")
            yield NavigableListView(id="forward_list")
            yield Static("Esc return", classes="hint")

    async def on_mount(self) -> None:
        await self._load_and_render()

    async def _load_and_render(self) -> None:
        nodes = forward_nodes(self.forward)
        forward_id = str(self.forward.get("id") or "")
        if not nodes and forward_id:
            self.query_one("#forward_title", Static).update("Forwarded messages  [loading]")
            try:
                resolved = dict(await self.client.forward(forward_id))
                self.forward.clear()
                self.forward.update(resolved)
            except Exception as exc:
                self.forward["status"] = "unavailable"
                self.forward["error"] = str(exc)
        nodes = forward_nodes(self.forward)
        title = str(self.forward.get("title") or "Forwarded messages")
        self.query_one("#forward_title", Static).update(
            "{}  [{}]".format(title, forward_status_label(self.forward))
        )
        view = self.query_one("#forward_list", ListView)
        await view.clear()
        if nodes:
            await view.extend(ForwardNodeListItem(node, compact=self.app.short) for node in nodes)
            view.index = 0
        else:
            error = str(self.forward.get("error") or "Forward content is unavailable")
            await view.append(ListItem(Static(error, markup=False)))
        view.focus()

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if isinstance(event.item, ForwardNodeListItem) and event.item.message.forwards:
            self.app.push_screen(ForwardViewer(self.client, event.item.message.forwards[0]))

    def action_cancel(self) -> None:
        self.dismiss(None)


class WebQQTui(App):
    TITLE = "WebQQ"
    SUB_TITLE = "Terminal client"
    ALLOW_SELECT = False
    BINDINGS = [
        Binding("q", "quit_requested", "Quit", show=False),
        Binding("escape", "back", "Back", show=False),
        Binding("ctrl+f", "find", "Find", show=False),
        Binding("n", "next_match", show=False),
        Binding("shift+n", "previous_match", show=False),
        Binding("r", "reply", show=False),
        Binding("p", "poke", show=False),
        Binding("d", "download", show=False),
        Binding("ctrl+o", "send_file", show=False),
    ]
    CSS = """
    Screen { background: #111418; color: #e8eaed; }
    #workspace { height: 1fr; }
    #sidebar { width: 34; min-width: 24; border-right: solid #3c4043; background: #171a1f; }
    #sidebar_title, #chat_header { height: 2; padding: 0 1; content-align: left middle; text-style: bold; background: #20242a; }
    #chat_filter, #message_search, #file_path { margin: 0; border: none; }
    #chat_list, #message_list { height: 1fr; background: transparent; }
    #chat_list > ListItem { height: 3; padding: 0 1; }
    #message_list > ListItem { height: auto; min-height: 3; padding: 0 1 1 1; }
    ListView > ListItem.--highlight { background: #2b3138; }
    #conversation { width: 1fr; }
    #message_search, #file_path { display: none; }
    #reply_bar { display: none; height: 1; padding: 0 1; color: #fbbc04; background: #20242a; }
    #composer { height: 4; border: tall #3c4043; background: #171a1f; }
    #composer:focus { border: tall #45a3c7; }
    #status_bar { height: 1; padding: 0 1; background: #20242a; color: #bdc1c6; }
    #too_small { display: none; width: 100%; height: 100%; content-align: center middle; background: #111418; color: #fbbc04; }
    .short #sidebar_title { display: none; }
    .short #chat_list > ListItem { height: 2; }
    .short #composer { height: 3; border: none; }
    .short #chat_header { height: 1; }
    .dialog-title { height: 2; text-style: bold; }
    """

    def __init__(self, client: WebQQClient):
        super().__init__()
        self.client = client
        self.chats: List[Chat] = []
        self.messages: List[Message] = []
        self.members: List[Mapping[str, Any]] = []
        self.current_chat: Optional[Chat] = None
        self.reply_to: Optional[Message] = None
        self.narrow = False
        self.short = False
        self.conversation_visible = False
        self.no_more_messages = False
        self.loading_older = False
        self._rendering = False
        self._running = True
        self._tasks: Set[asyncio.Task] = set()
        self._load_token = 0
        self._match_indexes: List[int] = []
        self._match_position = -1
        self._mention_open = False
        self._chat_refreshing = False
        self._chat_cursor_id = ""
        self._live_status = "Live connecting"
        self._napcat_status = "NapCat unknown"
        self._account_status = ""
        self._self_user_id = ""
        self._chat_count = 0
        self._base_status = "Connecting to {}".format(client.config.server_url)
        self._notice = ""
        self._notice_serial = 0
        self._last_quit_warning = 0.0

    def compose(self) -> ComposeResult:
        with Horizontal(id="workspace"):
            with Vertical(id="sidebar"):
                yield Static("Chats", id="sidebar_title")
                yield Input(placeholder="Filter chats (Ctrl+F)", id="chat_filter")
                yield NavigableListView(id="chat_list")
            with Vertical(id="conversation"):
                yield Static("Select a chat", id="chat_header")
                yield Input(placeholder="Search loaded messages", id="message_search")
                yield MessageListView(id="message_list")
                yield Static("", id="reply_bar")
                yield Input(placeholder="Path to file; Enter uploads", id="file_path")
                yield Composer("", id="composer", soft_wrap=True, show_line_numbers=False)
        yield Static(self._base_status, id="status_bar")
        yield Static("Terminal is too small. Resize to at least 32x10.", id="too_small")

    async def on_mount(self) -> None:
        self._apply_layout(self.size.width, self.size.height)
        self.query_one("#chat_list", ListView).focus()
        self._spawn(self._initial_load())
        self._spawn(self._websocket_loop())
        self._spawn(self._poll_loop())

    async def on_unmount(self) -> None:
        self._running = False
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def on_resize(self, event: events.Resize) -> None:
        self._apply_layout(event.size.width, event.size.height)

    def _apply_layout(self, width: int, height: int) -> None:
        too_small = width < 32 or height < 10
        workspace = self.query_one("#workspace")
        status = self.query_one("#status_bar")
        resize_notice = self.query_one("#too_small")
        workspace.styles.display = "none" if too_small else "block"
        status.styles.display = "none" if too_small else "block"
        resize_notice.styles.display = "block" if too_small else "none"
        if too_small:
            return

        old_short = self.short
        self.narrow = width < 80 or height > width
        self.short = height < 18
        self.set_class(self.short, "short")
        sidebar = self.query_one("#sidebar")
        conversation = self.query_one("#conversation")
        if self.narrow:
            sidebar.styles.width = "1fr"
            show_conversation = self.conversation_visible and self.current_chat is not None
            sidebar.styles.display = "none" if show_conversation else "block"
            conversation.styles.display = "block" if show_conversation else "none"
        else:
            sidebar.styles.width = 34
            sidebar.styles.display = "block"
            conversation.styles.display = "block"
        if old_short != self.short and self.is_mounted:
            self._spawn(self._render_all())

    def _spawn(self, coroutine: Coroutine[Any, Any, Any]) -> asyncio.Task:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task) -> None:
        self._tasks.discard(task)
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error:
            self._set_notice(str(error))

    async def _initial_load(self) -> None:
        results = await asyncio.gather(self._refresh_status(), self._refresh_chats(), return_exceptions=True)
        errors = [str(item) for item in results if isinstance(item, Exception)]
        if errors:
            self._set_notice(errors[0])

    async def _poll_loop(self) -> None:
        counter = 0
        while self._running:
            await asyncio.sleep(10)
            await self._refresh_chats(silent=True)
            counter += 1
            if counter % 2 == 0:
                await self._refresh_status(silent=True)

    async def _websocket_loop(self) -> None:
        backoff = 1
        while self._running:
            try:
                websocket = await self.client.websocket()
                self._set_connection_status(True)
                backoff = 1
                async with websocket:
                    async for incoming in websocket:
                        if incoming.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(incoming.data)
                            except (TypeError, json.JSONDecodeError):
                                continue
                            if isinstance(payload, dict):
                                await self._handle_socket_event(payload)
                        elif incoming.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                if self._running:
                    raise WebQQClientError("live connection closed")
            except AuthenticationError as exc:
                self._set_connection_status(False, str(exc))
                return
            except (WebQQClientError, aiohttp.ClientError, OSError) as exc:
                self._set_connection_status(False, "reconnecting in {}s".format(backoff))
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
                if self._running and backoff == 2:
                    self._set_notice(str(exc), seconds=2)
            else:
                await self._refresh_chats(silent=True)

    async def _handle_socket_event(self, payload: Mapping[str, Any]) -> None:
        event_type = payload.get("type")
        data = payload.get("data")
        if not isinstance(data, dict):
            return
        if event_type == "new_message":
            message = Message.from_json(data)
            if self.current_chat and message.chat_id == self.current_chat.chat_id:
                self.messages = deduplicate_messages(self.messages + [message])
                await self._render_messages(select_last=True)
        elif event_type == "message_update":
            await self._apply_message_update(data)
        elif event_type == "emoji_like":
            await self._apply_reaction_update(data)
        if event_type in ("new_message", "message_update"):
            await self._refresh_chats(silent=True)

    async def _apply_message_update(self, data: Mapping[str, Any]) -> None:
        if self.current_chat and data.get("chat_id") and str(data.get("chat_id")) != self.current_chat.chat_id:
            return
        message_id = str(data.get("message_id") or "")
        local_id = str(data.get("local_id") or "")
        full = data.get("message") if isinstance(data.get("message"), dict) else None
        patch = data.get("patch") if isinstance(data.get("patch"), dict) else {}
        changed = False
        updated: List[Message] = []
        for message in self.messages:
            matches = (message_id and message.message_id == message_id) or (local_id and message.local_id == local_id)
            if matches:
                update = dict(full or patch)
                if self.current_chat and not update.get("chat_id"):
                    update["chat_id"] = self.current_chat.chat_id
                updated.append(message.merged(update))
                changed = True
            else:
                updated.append(message)
        if not changed and full:
            updated.append(Message.from_json(full))
        if changed or full:
            self.messages = deduplicate_messages(updated)
            await self._render_messages()

    async def _apply_reaction_update(self, data: Mapping[str, Any]) -> None:
        message_id = str(data.get("message_id") or "")
        reactions = data.get("reactions") if isinstance(data.get("reactions"), list) else []
        changed = False
        for index, message in enumerate(self.messages):
            if message.message_id == message_id:
                self.messages[index] = message.merged({"reactions": reactions})
                changed = True
        if changed:
            await self._render_messages()

    async def _refresh_status(self, silent: bool = False) -> None:
        try:
            status = await self.client.status()
            connected = bool(status.get("napcat_connected"))
            self_user = status.get("self_user") if isinstance(status.get("self_user"), dict) else {}
            name = str(self_user.get("name") or self_user.get("user_id") or "")
            self._self_user_id = str(self_user.get("user_id") or "")
            self._napcat_status = "NapCat connected" if connected else "NapCat disconnected"
            self._account_status = name
            self._chat_count = int(status.get("chats_count", len(self.chats)) or 0)
            self._rebuild_base_status()
        except Exception as exc:
            self._live_status = "Server unreachable"
            self._rebuild_base_status()
            if not silent:
                raise exc

    async def _refresh_chats(self, silent: bool = False) -> None:
        if self._chat_refreshing:
            return
        self._chat_refreshing = True
        try:
            self.chats = await self.client.chats()
            if self.current_chat:
                replacement = next((chat for chat in self.chats if chat.chat_id == self.current_chat.chat_id), None)
                if replacement:
                    self.current_chat = replacement
                    self.query_one("#chat_header", Static).update(self._chat_header_text())
            await self._render_chats()
        except Exception:
            if not silent:
                raise
        finally:
            self._chat_refreshing = False

    async def _render_all(self) -> None:
        await self._render_chats()
        await self._render_messages()

    async def _render_chats(self) -> None:
        if not self.is_mounted:
            return
        query = self.query_one("#chat_filter", Input).value.strip().casefold()
        chats = [
            chat
            for chat in self.chats
            if not query or query in "{} {} {}".format(chat.name, chat.chat_id, chat.last_text).casefold()
        ]
        view = self.query_one("#chat_list", ListView)
        highlighted = view.highlighted_child
        if isinstance(highlighted, ChatListItem):
            self._chat_cursor_id = highlighted.chat.chat_id
        current_id = self.current_chat.chat_id if self.current_chat else ""
        preferred_id = self._chat_cursor_id or current_id
        self._rendering = True
        try:
            await view.clear()
            if chats:
                await view.extend(ChatListItem(chat, compact=self.short) for chat in chats)
                selected = next((index for index, chat in enumerate(chats) if chat.chat_id == preferred_id), 0)
                view.index = selected
                self._chat_cursor_id = chats[selected].chat_id
        finally:
            self._rendering = False

    async def _render_messages(self, select_last: bool = False) -> None:
        if not self.is_mounted:
            return
        view = self.query_one("#message_list", ListView)
        selected = self._selected_message()
        selected_id = selected.stable_id if selected else ""
        search = self.query_one("#message_search", Input).value.strip()
        self._match_indexes = [index for index, message in enumerate(self.messages) if message_matches(message, search)]
        self._rendering = True
        try:
            await view.clear()
            if self.messages:
                await view.extend(MessageListItem(message, compact=self.short, search=search) for message in self.messages)
                if select_last:
                    index = len(self.messages) - 1
                else:
                    index = next(
                        (position for position, message in enumerate(self.messages) if message.stable_id == selected_id),
                        len(self.messages) - 1,
                    )
                view.index = max(0, index)
                view.scroll_to_widget(view.children[view.index], animate=False)
        finally:
            self._rendering = False

    def _refresh_message_row(self, message: Message) -> None:
        if not self.is_mounted:
            return
        try:
            view = self.query_one("#message_list", ListView)
            search = self.query_one("#message_search", Input).value.strip()
        except NoMatches:
            return
        for child in view.children:
            if isinstance(child, MessageListItem) and child.message is message and child.is_mounted:
                child.query_one(Static).update(format_message(message, compact=self.short, search=search))

    async def _hydrate_forwards(self, chat_id: str, token: int) -> None:
        pending = {}
        for message in self.messages:
            for forward in message.forwards:
                if not isinstance(forward, dict):
                    continue
                forward_id = str(forward.get("id") or "")
                if forward_id and not forward_nodes(forward):
                    pending.setdefault(forward_id, []).append((message, forward))
        if not pending:
            return

        forward_ids = list(pending)
        results = await asyncio.gather(
            *(self.client.forward(forward_id) for forward_id in forward_ids),
            return_exceptions=True,
        )
        if token != self._load_token or not self.current_chat or self.current_chat.chat_id != chat_id:
            return
        for forward_id, result in zip(forward_ids, results):
            if isinstance(result, Exception) or not isinstance(result, Mapping) or not forward_nodes(result):
                continue
            resolved = dict(result)
            resolved.setdefault("id", forward_id)
            for message, forward in pending[forward_id]:
                forward.clear()
                forward.update(resolved)
                self._refresh_message_row(message)

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "chat_filter":
            await self._render_chats()
        elif event.input.id == "message_search":
            self._match_position = -1
            await self._render_messages()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "file_path":
            value = event.value.strip()
            event.input.value = ""
            event.input.styles.display = "none"
            self.query_one("#composer", Composer).styles.display = "block"
            if value:
                self._spawn(self._upload_file(Path(value)))
        elif event.input.id == "message_search":
            self.action_next_match()
            self.query_one("#message_list", ListView).focus()

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if self._rendering:
            return
        if event.list_view.id == "chat_list" and isinstance(event.item, ChatListItem):
            await self._open_chat(event.item.chat)
        elif event.list_view.id == "message_list" and isinstance(event.item, MessageListItem):
            if event.item.message.forwards:
                item = event.item

                def refresh_item(_: Any) -> None:
                    if item.is_mounted:
                        item.query_one(Static).update(format_message(item.message, compact=self.short))

                self.push_screen(ForwardViewer(self.client, item.message.forwards[0]), refresh_item)

    def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
        if event.list_view.id == "chat_list" and isinstance(event.item, ChatListItem):
            self._chat_cursor_id = event.item.chat.chat_id

    async def on_message_list_view_load_older(self, event: MessageListView.LoadOlder) -> None:
        await self._load_older()

    async def on_composer_submit(self, event: Composer.Submit) -> None:
        await self._send_draft()

    def on_text_area_changed(self, event: TextArea.Changed) -> None:
        if event.text_area.id != "composer" or self._mention_open:
            return
        if event.text_area.text.endswith("@") and self.current_chat and self.current_chat.chat_id.startswith("group_"):
            if not self.members:
                self._set_notice("Group member list is not available")
                return
            self._mention_open = True
            self.push_screen(MemberPicker(self.members), self._mention_selected)

    async def _open_chat(self, chat: Chat) -> None:
        self.current_chat = chat
        self._chat_cursor_id = chat.chat_id
        self.conversation_visible = True
        self.reply_to = None
        self.no_more_messages = False
        self.loading_older = False
        self._load_token += 1
        token = self._load_token
        self.query_one("#chat_header", Static).update(self._chat_header_text("loading"))
        self._update_reply_bar()
        self._apply_layout(self.size.width, self.size.height)
        try:
            if chat.chat_id.startswith("group_"):
                messages, members, _ = await asyncio.gather(
                    self.client.messages(chat.chat_id, limit=50),
                    self.client.group_members(chat.chat_id),
                    self.client.mark_read(chat.chat_id),
                    return_exceptions=True,
                )
                if isinstance(members, list):
                    self.members = members
            else:
                messages, _ = await asyncio.gather(
                    self.client.messages(chat.chat_id, limit=50),
                    self.client.mark_read(chat.chat_id),
                    return_exceptions=True,
                )
                self.members = []
            if token != self._load_token:
                return
            if isinstance(messages, Exception):
                raise messages
            self.messages = deduplicate_messages(messages)
            self.no_more_messages = len(messages) < 50
            self.query_one("#chat_header", Static).update(self._chat_header_text())
            await self._render_messages(select_last=True)
            self.query_one("#message_list", ListView).focus()
            self._spawn(self._hydrate_forwards(chat.chat_id, token))
        except Exception as exc:
            self.query_one("#chat_header", Static).update(self._chat_header_text("load failed"))
            self._set_notice("Failed to load messages: {}".format(exc))

    async def _load_older(self) -> None:
        if not self.current_chat or not self.messages or self.loading_older or self.no_more_messages:
            return
        self.loading_older = True
        token = self._load_token
        chat_id = self.current_chat.chat_id
        before = self.messages[0].timestamp
        selected = self._selected_message()
        selected_id = selected.stable_id if selected else ""
        try:
            older = await self.client.messages(chat_id, limit=50, before=before)
            if token != self._load_token or not self.current_chat or self.current_chat.chat_id != chat_id:
                return
            self.no_more_messages = len(older) < 50
            self.messages = deduplicate_messages(older + self.messages)
            await self._render_messages()
            if selected_id:
                index = next((i for i, item in enumerate(self.messages) if item.stable_id == selected_id), 0)
                self.query_one("#message_list", ListView).index = index
            self._set_notice("Loaded {} older messages".format(len(older)), seconds=2)
        except Exception as exc:
            if token == self._load_token and self.current_chat and self.current_chat.chat_id == chat_id:
                self._set_notice("History load failed: {}".format(exc))
        finally:
            if token == self._load_token and self.current_chat and self.current_chat.chat_id == chat_id:
                self.loading_older = False

    async def _send_draft(self) -> None:
        if not self.current_chat:
            self._set_notice("Select a chat first")
            return
        composer = self.query_one("#composer", Composer)
        text = composer.text.strip()
        if not text:
            return
        reply_id = self.reply_to.message_id if self.reply_to else ""
        composer.load_text("")
        try:
            await self.client.send_message(self.current_chat.chat_id, text, reply_to=reply_id)
            self.reply_to = None
            self._update_reply_bar()
        except Exception as exc:
            composer.load_text(text)
            self._set_notice("Send failed: {}".format(exc))

    async def _upload_file(self, path: Path) -> None:
        if not self.current_chat:
            return
        self._set_notice("Uploading {}...".format(path.name), seconds=120)
        try:
            await self.client.send_file(self.current_chat.chat_id, path)
            self._set_notice("Sent {}".format(path.name))
        except Exception as exc:
            self._set_notice("Upload failed: {}".format(exc))

    def _mention_selected(self, member: Optional[Mapping[str, Any]]) -> None:
        self._mention_open = False
        composer = self.query_one("#composer", Composer)
        if member:
            user_id = str(member.get("user_id") or member.get("uid") or "")
            if user_id:
                text = composer.text
                if text.endswith("@"):
                    text = text[:-1]
                composer.load_text(text + "@[{}] ".format(user_id))
        composer.focus()

    def action_reply(self) -> None:
        if isinstance(self.focused, Composer):
            return
        message = self._selected_message()
        if not message or not message.message_id:
            self._set_notice("Select a server-confirmed message to reply")
            return
        self.reply_to = message
        self._update_reply_bar()
        self.query_one("#composer", Composer).focus()

    def action_poke(self) -> None:
        if isinstance(self.focused, (Composer, Input)):
            return
        message = self._selected_message()
        if (
            not message
            or message.self_sent
            or bool(message.raw.get("system"))
            or not message.sender_id.isdigit()
            or (self._self_user_id and message.sender_id == self._self_user_id)
        ):
            self._set_notice("Select a message from another user to poke")
            return
        if not self.current_chat:
            self._set_notice("Select a chat first")
            return
        self._spawn(self._poke(self.current_chat.chat_id, message))

    async def _poke(self, chat_id: str, message: Message) -> None:
        self._set_notice("Poking {}...".format(message.sender_name), seconds=120)
        try:
            await self.client.poke(chat_id, message.sender_id)
            self._set_notice("Poked {}".format(message.sender_name))
        except Exception as exc:
            self._set_notice("Poke failed: {}".format(exc))

    def action_download(self) -> None:
        if isinstance(self.focused, (Composer, Input)):
            return
        message = self._selected_message()
        attachments = message.downloadable_attachments if message else []
        if not attachments:
            self._set_notice("Selected message has no downloadable attachment")
        elif len(attachments) == 1:
            self._download_selected(attachments[0])
        else:
            self.push_screen(AttachmentPicker(attachments), self._download_selected)

    def _download_selected(self, attachment: Optional[Attachment]) -> None:
        if attachment:
            self._spawn(self._download(attachment))

    async def _download(self, attachment: Attachment) -> None:
        if not self.current_chat:
            return

        def progress(received: int, total: int) -> None:
            if total:
                self._set_notice("Downloading {}... {}%".format(attachment.name, int(received * 100 / total)), seconds=120)
            else:
                self._set_notice("Downloading {}... {} KB".format(attachment.name, received // 1024), seconds=120)

        try:
            path = await self.client.download_attachment(self.current_chat.chat_id, attachment, progress=progress)
            self._set_notice("Downloaded to {}".format(path))
        except Exception as exc:
            self._set_notice("Download failed: {}".format(exc))

    def action_send_file(self) -> None:
        if not self.current_chat:
            self._set_notice("Select a chat first")
            return
        composer = self.query_one("#composer", Composer)
        prompt = self.query_one("#file_path", Input)
        composer.styles.display = "none"
        prompt.styles.display = "block"
        prompt.focus()

    def action_find(self) -> None:
        if self.narrow and not self.conversation_visible:
            self.query_one("#chat_filter", Input).focus()
            return
        if self.current_chat and self.query_one("#conversation").styles.display != "none":
            search = self.query_one("#message_search", Input)
            search.styles.display = "block"
            search.focus()
        else:
            self.query_one("#chat_filter", Input).focus()

    def action_next_match(self) -> None:
        self._select_match(1)

    def action_previous_match(self) -> None:
        self._select_match(-1)

    def _select_match(self, direction: int) -> None:
        if isinstance(self.focused, Composer):
            return
        if not self._match_indexes:
            self._set_notice("No message matches")
            return
        self._match_position = (self._match_position + direction) % len(self._match_indexes)
        index = self._match_indexes[self._match_position]
        view = self.query_one("#message_list", ListView)
        view.index = index
        view.scroll_to_widget(view.children[index], animate=False)
        self._set_notice("Match {}/{}".format(self._match_position + 1, len(self._match_indexes)), seconds=2)

    def action_back(self) -> None:
        file_prompt = self.query_one("#file_path", Input)
        if file_prompt.styles.display != "none":
            file_prompt.styles.display = "none"
            self.query_one("#composer", Composer).styles.display = "block"
            self.query_one("#composer", Composer).focus()
            return
        search = self.query_one("#message_search", Input)
        if search.styles.display != "none":
            search.value = ""
            search.styles.display = "none"
            self.query_one("#message_list", ListView).focus()
            return
        chat_filter = self.query_one("#chat_filter", Input)
        if self.focused is chat_filter:
            chat_filter.value = ""
            self.query_one("#chat_list", ListView).focus()
            return
        if self.reply_to:
            self.reply_to = None
            self._update_reply_bar()
            return
        if self.focused is self.query_one("#composer", Composer):
            self.query_one("#message_list", ListView).focus()
            return
        if self.conversation_visible:
            self.conversation_visible = False
            self._apply_layout(self.size.width, self.size.height)
            self.query_one("#chat_list", ListView).focus()

    def action_quit_requested(self) -> None:
        draft = self.query_one("#composer", Composer).text.strip()
        now = time.monotonic()
        if draft and now - self._last_quit_warning > 3:
            self._last_quit_warning = now
            self._set_notice("Draft is not empty; press q again within 3 seconds to quit", seconds=3)
            return
        self.exit()

    def _selected_message(self) -> Optional[Message]:
        if not self.is_mounted:
            return None
        view = self.query_one("#message_list", ListView)
        child = view.highlighted_child
        return child.message if isinstance(child, MessageListItem) else None

    def _chat_header_text(self, suffix: str = "") -> str:
        if not self.current_chat:
            return "Select a chat"
        label = "{}  {}".format(self.current_chat.name, self.current_chat.chat_id)
        return label + ("  [{}]".format(suffix) if suffix else "")

    def _update_reply_bar(self) -> None:
        bar = self.query_one("#reply_bar", Static)
        if self.reply_to:
            preview = display_content(self.reply_to).replace("\n", " ")[:80]
            bar.update("Reply to {}: {}  (Esc cancel)".format(self.reply_to.sender_name, preview))
            bar.styles.display = "block"
        else:
            bar.update("")
            bar.styles.display = "none"

    def _set_connection_status(self, connected: bool, detail: str = "") -> None:
        self._live_status = "Live connected" if connected else "Live offline"
        if detail:
            self._live_status += " ({})".format(detail)
        self._rebuild_base_status()

    def _rebuild_base_status(self) -> None:
        parts = [self._live_status, self._napcat_status, "{} chats".format(self._chat_count)]
        if self._account_status:
            parts.append(self._account_status)
        if not self.short:
            parts.append("Ctrl+F find  Ctrl+O file")
        self._base_status = " | ".join(parts)
        self._update_status_bar()

    def _set_notice(self, text: str, seconds: float = 5) -> None:
        self._notice_serial += 1
        serial = self._notice_serial
        self._notice = str(text)
        self._update_status_bar()

        def clear() -> None:
            if self._notice_serial == serial:
                self._notice = ""
                self._update_status_bar()

        if self.is_mounted and seconds > 0:
            self.set_timer(seconds, clear)

    def _update_status_bar(self) -> None:
        if self.is_mounted:
            try:
                self.query_one("#status_bar", Static).update(self._notice or self._base_status)
            except NoMatches:
                pass
