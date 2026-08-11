import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from rich.style import Style
from rich.text import Text
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Input, ListItem, ListView, Static, TextArea

from .client import WebQQClient
from .models import Chat, Message, display_content


class DataListItem(ListItem):
    def __init__(self, label: Any, data: Any = None):
        super().__init__(Static(label, markup=False))
        self.data = data


@dataclass(frozen=True)
class ActionEntry:
    action_id: str
    label: str
    detail: str = ""
    enabled: bool = True

    @property
    def search_text(self) -> str:
        return "{} {}".format(self.label, self.detail).lower()


class ActionPalette(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    ActionPalette { align: center middle; background: $background 70%; }
    ActionPalette > Container { width: 76; max-width: 96%; height: 28; max-height: 92%; border: solid $accent; background: $surface; padding: 1; }
    ActionPalette #action_filter { height: 3; margin: 0; }
    ActionPalette #action_list { height: 1fr; }
    ActionPalette #action_list > ListItem { height: auto; min-height: 2; padding: 0 1; }
    ActionPalette .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, entries: Sequence[ActionEntry]):
        super().__init__()
        self.entries = list(entries)

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Actions", classes="dialog-title")
            yield Input(placeholder="Search actions", id="action_filter")
            yield ListView(id="action_list")
            yield Static("Enter choose  Esc return", classes="hint")

    async def on_mount(self) -> None:
        await self._render_actions("")
        self.query_one("#action_filter", Input).focus()

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "action_filter":
            await self._render_actions(event.value)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "action_filter":
            return
        item = self.query_one("#action_list", ListView).highlighted_child
        if isinstance(item, DataListItem) and isinstance(item.data, ActionEntry) and item.data.enabled:
            self.dismiss(item.data.action_id)

    async def _render_actions(self, needle: str) -> None:
        needle = needle.strip().lower()
        view = self.query_one("#action_list", ListView)
        await view.clear()
        entries = [entry for entry in self.entries if not needle or needle in entry.search_text]
        for entry in entries:
            text = Text(entry.label, style="bold" if entry.enabled else "dim")
            if entry.detail:
                text.append("\n" + entry.detail, style="dim")
            await view.append(DataListItem(text, entry))
        view.index = 0 if entries else None

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        if not isinstance(event.item, DataListItem):
            return
        entry = event.item.data
        if isinstance(entry, ActionEntry) and entry.enabled:
            self.dismiss(entry.action_id)

    def action_cancel(self) -> None:
        self.dismiss(None)


class ConfirmDialog(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False), Binding("y", "confirm", show=False)]
    CSS = """
    ConfirmDialog { align: center middle; background: $background 70%; }
    ConfirmDialog > Container { width: 64; max-width: 96%; height: 8; max-height: 90%; border: solid $accent; background: $surface; padding: 1; }
    ConfirmDialog #confirm_text { height: 1fr; }
    ConfirmDialog .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, title: str, prompt: str):
        super().__init__()
        self.title = title
        self.prompt = prompt

    def compose(self) -> ComposeResult:
        with Container():
            yield Static(self.title, classes="dialog-title")
            yield Static(self.prompt, id="confirm_text")
            yield Static("Y confirm  Esc cancel", classes="hint")

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class TextPrompt(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    TextPrompt { align: center middle; background: $background 70%; }
    TextPrompt > Container { width: 68; max-width: 96%; height: 9; max-height: 90%; border: solid $accent; background: $surface; padding: 1; }
    TextPrompt #prompt_value { height: 3; margin: 0; }
    TextPrompt #prompt_error { height: 1; color: $error; }
    TextPrompt .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, title: str, placeholder: str = "", value: str = "", allow_empty: bool = True):
        super().__init__()
        self.title = title
        self.placeholder = placeholder
        self.value = value
        self.allow_empty = allow_empty

    def compose(self) -> ComposeResult:
        with Container():
            yield Static(self.title, classes="dialog-title")
            yield Input(value=self.value, placeholder=self.placeholder, id="prompt_value")
            yield Static("", id="prompt_error")
            yield Static("Enter submit  Esc return", classes="hint")

    def on_mount(self) -> None:
        field = self.query_one("#prompt_value", Input)
        field.action_end()
        field.focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "prompt_value":
            return
        value = event.value.strip()
        if not value and not self.allow_empty:
            self.query_one("#prompt_error", Static).update("A value is required")
            return
        self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class ReactionUsers(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    ReactionUsers { align: center middle; background: $background 70%; }
    ReactionUsers > Container { width: 68; max-width: 96%; height: 24; max-height: 92%; border: solid $accent; background: $surface; padding: 1; }
    ReactionUsers #reaction_users { height: 1fr; }
    ReactionUsers #reaction_users > ListItem { height: auto; min-height: 1; padding: 0 1; }
    ReactionUsers .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient, message: Message):
        super().__init__()
        self.client = client
        self.message = message

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Reaction users", id="reaction_title", classes="dialog-title")
            yield ListView(id="reaction_users")
            yield Static("Esc return", classes="hint")

    async def on_mount(self) -> None:
        view = self.query_one("#reaction_users", ListView)
        try:
            reactions = await self.client.reaction_details(
                self.message.chat_id, self.message.message_id,
            )
            rows = []
            for reaction in reactions:
                emoji_id = str(reaction.get("emoji_id") or reaction.get("emojiId") or "?")
                users = reaction.get("users") if isinstance(reaction.get("users"), list) else []
                if not users:
                    rows.append(DataListItem("Face {} - {} reactions".format(emoji_id, reaction.get("count") or 0)))
                for user in users:
                    if not isinstance(user, dict):
                        continue
                    user_id = str(user.get("user_id") or user.get("uin") or "")
                    name = str(user.get("name") or user.get("nickname") or user_id or "Unknown")
                    rows.append(DataListItem("Face {}  {}  {}".format(emoji_id, name, user_id)))
            await view.extend(rows or [DataListItem("No reaction-user details available")])
        except Exception as error:
            await view.append(DataListItem("Load failed: {}".format(error)))
        view.index = 0
        view.focus()

    def action_cancel(self) -> None:
        self.dismiss(None)


class ForwardComposer(ModalScreen):
    BINDINGS = [
        Binding("escape", "cancel", show=False),
        Binding("ctrl+a", "add_node", show=False),
        Binding("f6", "add_node", show=False),
        Binding("delete", "delete_node", show=False),
        Binding("ctrl+up", "move_up", show=False),
        Binding("ctrl+down", "move_down", show=False),
        Binding("ctrl+s", "send", show=False),
    ]
    CSS = """
    ForwardComposer { align: center middle; background: $background 70%; }
    ForwardComposer > Container { width: 100; max-width: 98%; height: 94%; min-height: 8; border: solid $accent; background: $surface; padding: 1; }
    ForwardComposer #forward_chat_filter, ForwardComposer #forward_sender_id, ForwardComposer #forward_sender_name { height: 3; margin: 0; }
    ForwardComposer #forward_chats { height: 5; min-height: 1; }
    ForwardComposer #forward_nodes { height: 1fr; min-height: 1; }
    ForwardComposer #forward_content { height: 5; min-height: 2; border: solid $panel; }
    ForwardComposer #forward_status { height: 1; color: $warning; }
    ForwardComposer .hint { height: 1; color: $text-muted; }
    ForwardComposer .custom-only { display: block; }
    ForwardComposer.-references .custom-only { display: none; }
    """

    def __init__(
        self, client: WebQQClient, chats: Sequence[Chat], nodes: Optional[Sequence[Mapping[str, Any]]] = None,
    ):
        super().__init__()
        self.client = client
        self.chats = list(chats)
        self.nodes = [dict(node) for node in (nodes or [])]
        self.reference_mode = bool(nodes)
        self.destination = ""
        self.sending = False

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Forward selected messages" if self.reference_mode else "Create combined forward", classes="dialog-title")
            yield Input(placeholder="Filter and choose destination", id="forward_chat_filter")
            yield ListView(id="forward_chats")
            yield ListView(id="forward_nodes")
            yield Input(placeholder="Sender QQ ID", id="forward_sender_id", classes="custom-only")
            yield Input(placeholder="Sender name", id="forward_sender_name", classes="custom-only")
            yield TextArea("", id="forward_content", classes="custom-only", soft_wrap=True, show_line_numbers=False)
            yield Static("", id="forward_status")
            yield Static("F6 add  Del remove  Ctrl+Up/Down reorder  Ctrl+S send  Esc return", classes="hint")

    async def on_mount(self) -> None:
        self.set_class(self.reference_mode, "-references")
        await self._render_chats("")
        await self._render_nodes()
        self.query_one("#forward_chat_filter", Input).focus()

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "forward_chat_filter":
            await self._render_chats(event.value)

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "forward_chat_filter":
            return
        item = self.query_one("#forward_chats", ListView).highlighted_child
        if isinstance(item, DataListItem) and isinstance(item.data, Chat):
            self.destination = item.data.chat_id
            await self._render_chats(event.value)
            self.query_one("#forward_status", Static).update("Destination: {}".format(item.data.name))
            (self.query_one("#forward_nodes", ListView) if self.reference_mode else self.query_one("#forward_sender_id", Input)).focus()

    async def _render_chats(self, needle: str) -> None:
        needle = needle.strip().lower()
        view = self.query_one("#forward_chats", ListView)
        await view.clear()
        values = [chat for chat in self.chats if not needle or needle in (chat.name + " " + chat.chat_id).lower()]
        await view.extend(
            DataListItem(("* " if chat.chat_id == self.destination else "  ") + chat.name + "  " + chat.chat_id, chat)
            for chat in values
        )
        view.index = 0 if values else None

    async def _render_nodes(self, selected: Optional[int] = None) -> None:
        view = self.query_one("#forward_nodes", ListView)
        await view.clear()
        for index, node in enumerate(self.nodes):
            if node.get("message_id"):
                label = "{}. Message #{}".format(index + 1, node["message_id"])
            else:
                content = str(node.get("content") or "").replace("\n", " ")
                label = "{}. {} ({}): {}".format(
                    index + 1, node.get("sender_name") or "Unknown", node.get("sender_id") or "?", content[:80],
                )
            await view.append(DataListItem(label, index))
        if self.nodes:
            view.index = min(selected if selected is not None else len(self.nodes) - 1, len(self.nodes) - 1)

    async def on_list_view_selected(self, event: ListView.Selected) -> None:
        if event.list_view.id != "forward_chats" or not isinstance(event.item, DataListItem):
            return
        chat = event.item.data
        if isinstance(chat, Chat):
            self.destination = chat.chat_id
            await self._render_chats(self.query_one("#forward_chat_filter", Input).value)
            self.query_one("#forward_status", Static).update("Destination: {}".format(chat.name))

    def action_add_node(self) -> None:
        if self.reference_mode:
            return
        sender_id = self.query_one("#forward_sender_id", Input).value.strip()
        sender_name = self.query_one("#forward_sender_name", Input).value.strip()
        content = self.query_one("#forward_content", TextArea).text.strip()
        if not sender_id or not sender_name or not content:
            self.query_one("#forward_status", Static).update("Sender ID, name, and content are required")
            return
        self.nodes.append({"sender_id": sender_id, "sender_name": sender_name, "content": content})
        self.query_one("#forward_content", TextArea).load_text("")
        self.run_worker(self._render_nodes())

    def _selected_node(self) -> Optional[int]:
        item = self.query_one("#forward_nodes", ListView).highlighted_child
        return item.data if isinstance(item, DataListItem) and isinstance(item.data, int) else None

    def action_delete_node(self) -> None:
        index = self._selected_node()
        if index is None:
            return
        self.nodes.pop(index)
        self.run_worker(self._render_nodes(max(0, index - 1)))

    def _move(self, offset: int) -> None:
        index = self._selected_node()
        if index is None or not 0 <= index + offset < len(self.nodes):
            return
        self.nodes[index], self.nodes[index + offset] = self.nodes[index + offset], self.nodes[index]
        self.run_worker(self._render_nodes(index + offset))

    def action_move_up(self) -> None:
        self._move(-1)

    def action_move_down(self) -> None:
        self._move(1)

    def action_send(self) -> None:
        if self.sending:
            return
        if not self.destination:
            self.query_one("#forward_status", Static).update("Choose a destination")
            return
        if not self.nodes:
            self.query_one("#forward_status", Static).update("Add at least one forward node")
            return
        total = sum(len(str(node.get("content") or "")) for node in self.nodes)
        if len(self.nodes) > 100 or total > 100000:
            self.query_one("#forward_status", Static).update("Forward exceeds 100 nodes or 100,000 characters")
            return
        self.sending = True
        self.query_one("#forward_status", Static).update("Sending...")
        self.run_worker(self._send())

    async def _send(self) -> None:
        try:
            await self.client.send_forward(self.destination, self.nodes)
        except Exception as error:
            self.sending = False
            self.query_one("#forward_status", Static).update("Send failed: {}".format(error))
            return
        self.dismiss({"sent": True, "chat_id": self.destination})

    def action_cancel(self) -> None:
        self.dismiss(None)


class ProfileEditor(ModalScreen):
    BINDINGS = [
        Binding("escape", "cancel", show=False), Binding("ctrl+s", "save", show=False),
        Binding("f7", "preview_avatar", show=False),
    ]
    CSS = """
    ProfileEditor { align: center middle; background: $background 70%; }
    ProfileEditor > Container { width: 72; max-width: 96%; height: 17; max-height: 92%; border: solid $accent; background: $surface; padding: 1; }
    ProfileEditor Input { height: 3; margin: 0; }
    ProfileEditor #profile_note { height: 4; min-height: 2; }
    ProfileEditor #profile_status { height: 1; color: $warning; }
    ProfileEditor .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient, profile: Mapping[str, Any]):
        super().__init__()
        self.client = client
        self.profile = dict(profile)

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Profile - {}".format(self.profile.get("user_id") or ""), classes="dialog-title")
            yield Input(value=str(self.profile.get("nickname") or ""), placeholder="Nickname", id="profile_nickname")
            yield TextArea(str(self.profile.get("personal_note") or ""), id="profile_note", soft_wrap=True, show_line_numbers=False)
            yield Input(placeholder="Avatar image path (optional)", id="profile_avatar")
            yield Static("", id="profile_status")
            yield Static("Ctrl+S save  F7 preview avatar  Esc return", classes="hint")

    def on_mount(self) -> None:
        self.query_one("#profile_nickname", Input).focus()

    def action_save(self) -> None:
        self.run_worker(self._save())

    def action_preview_avatar(self) -> None:
        user_id = str(self.profile.get("user_id") or "")
        if user_id:
            self.app.push_screen(ImagePreview(
                self.client, "Your avatar", "/api/avatar", {"type": "user", "id": user_id},
            ))

    async def _save(self) -> None:
        nickname = self.query_one("#profile_nickname", Input).value.strip()
        note = self.query_one("#profile_note", TextArea).text.strip()
        avatar = self.query_one("#profile_avatar", Input).value.strip()
        status = self.query_one("#profile_status", Static)
        if not nickname:
            status.update("Nickname is required")
            return
        status.update("Saving...")
        try:
            await self.client.update_profile(nickname, note)
            if avatar:
                await self.client.upload_profile_avatar(Path(avatar))
        except Exception as error:
            status.update("Save failed: {}".format(error))
            return
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class ContactsManager(ModalScreen):
    BINDINGS = [
        Binding("escape", "cancel", show=False), Binding("1", "requests", show=False),
        Binding("2", "friends", show=False), Binding("3", "profile", show=False),
        Binding("a", "approve", show=False), Binding("x", "reject_or_delete", show=False),
        Binding("e", "edit", show=False), Binding("t", "toggle_auto", show=False),
    ]
    CSS = """
    ContactsManager { align: center middle; background: $background 70%; }
    ContactsManager > Container { width: 100; max-width: 98%; height: 94%; min-height: 8; border: solid $accent; background: $surface; padding: 1; }
    ContactsManager #contact_filter { height: 3; margin: 0; }
    ContactsManager #contact_list { height: 1fr; }
    ContactsManager #contact_list > ListItem { height: auto; min-height: 2; padding: 0 1; }
    ContactsManager #contact_status { height: 1; color: $warning; }
    ContactsManager .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient):
        super().__init__()
        self.client = client
        self.tab = "requests"
        self.items: List[Mapping[str, Any]] = []
        self.profile_data: Dict[str, Any] = {}
        self.auto_approve = False

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Contacts - Requests", id="contact_title", classes="dialog-title")
            yield Input(placeholder="Filter requests", id="contact_filter")
            yield ListView(id="contact_list")
            yield Static("", id="contact_status")
            yield Static("1 requests  2 friends  3 profile  A approve  X reject/delete  E edit  T auto  Esc return", classes="hint")

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "contact_filter":
            await self._render_items()

    def _set_tab(self, tab: str) -> None:
        self.tab = tab
        field = self.query_one("#contact_filter", Input)
        field.value = ""
        field.placeholder = "Filter {}".format(tab)
        self.query_one("#contact_title", Static).update("Contacts - {}".format(tab.title()))
        self.run_worker(self._load())

    def action_requests(self) -> None:
        self._set_tab("requests")

    def action_friends(self) -> None:
        self._set_tab("friends")

    def action_profile(self) -> None:
        self._set_tab("profile")

    async def _load(self) -> None:
        status = self.query_one("#contact_status", Static)
        status.update("Loading...")
        try:
            if self.tab == "requests":
                payload = await self.client.contact_requests()
                settings = await self.client.contact_settings()
                self.items = [dict(item) for item in payload.get("requests", []) if isinstance(item, dict)]
                self.auto_approve = bool(settings.get("auto_approve_requests"))
                status.update("{} pending - auto approval {}".format(payload.get("pending_count") or 0, "on" if self.auto_approve else "off"))
            elif self.tab == "friends":
                payload = await self.client.friends()
                values = []
                for category in payload.get("categories", []):
                    if not isinstance(category, dict):
                        continue
                    for friend in category.get("friends", []):
                        if isinstance(friend, dict):
                            values.append({**friend, "category": category.get("name") or "Friends"})
                self.items = values
                status.update("{} friends".format(len(values)))
            else:
                self.profile_data = dict(await self.client.profile())
                self.items = [self.profile_data]
                status.update("E edits profile and avatar")
            await self._render_items()
        except Exception as error:
            self.items = []
            await self._render_items()
            status.update("Load failed: {}".format(error))

    async def _render_items(self) -> None:
        view = self.query_one("#contact_list", ListView)
        await view.clear()
        needle = self.query_one("#contact_filter", Input).value.strip().lower()
        shown = []
        for item in self.items:
            if self.tab == "requests":
                label = "{} [{}]\n{} {}  {}".format(
                    "Friend request" if item.get("request_type") == "friend" else "Group request",
                    item.get("status") or "unknown", item.get("user_name") or item.get("user_id") or "Unknown",
                    item.get("user_id") or "", item.get("comment") or item.get("group_name") or "",
                )
            elif self.tab == "friends":
                user_id = str(item.get("user_id") or item.get("uin") or "")
                nickname = str(item.get("nickname") or item.get("nick_name") or item.get("nick") or "")
                remark = str(item.get("remark") or "")
                label = "{}  {}\n{}".format(remark or nickname or user_id, user_id, item.get("category") or "Friends")
            else:
                label = "{}  {}\n{}".format(
                    item.get("nickname") or "Unnamed", item.get("user_id") or "", item.get("personal_note") or "No signature",
                )
            if not needle or needle in (label + " " + json.dumps(item, ensure_ascii=False)).lower():
                shown.append(DataListItem(label, item))
        await view.extend(shown or [DataListItem("No matching entries")])
        view.index = 0
        view.focus()

    def _selected(self) -> Optional[Mapping[str, Any]]:
        item = self.query_one("#contact_list", ListView).highlighted_child
        return item.data if isinstance(item, DataListItem) and isinstance(item.data, dict) else None

    def action_approve(self) -> None:
        item = self._selected()
        if self.tab != "requests" or not item or item.get("status") not in ("pending", "failed"):
            return
        title = "Friend remark (optional)" if item.get("request_type") == "friend" else "Approve request"
        self.app.push_screen(TextPrompt(title), lambda value: self.run_worker(self._request_action(item, True, value)))

    def action_reject_or_delete(self) -> None:
        item = self._selected()
        if not item:
            return
        if self.tab == "requests" and item.get("status") in ("pending", "failed"):
            self.app.push_screen(TextPrompt("Rejection reason (optional)"), lambda value: self.run_worker(self._request_action(item, False, value)))
        elif self.tab == "friends":
            user_id = str(item.get("user_id") or item.get("uin") or "")
            self.app.push_screen(
                ConfirmDialog("Delete friend", "Delete {}? Chat history is retained.".format(user_id)),
                lambda confirmed: self.run_worker(self._delete_friend(user_id)) if confirmed else None,
            )

    async def _request_action(self, item: Mapping[str, Any], approve: bool, value: Optional[str]) -> None:
        if value is None:
            return
        try:
            await self.client.act_on_contact_request(
                str(item.get("id") or ""), approve,
                remark=value if approve and item.get("request_type") == "friend" else "",
                reason=value if not approve else "",
            )
            await self._load()
        except Exception as error:
            self.query_one("#contact_status", Static).update("Action failed: {}".format(error))

    async def _delete_friend(self, user_id: str) -> None:
        try:
            await self.client.delete_friend(user_id)
            await self._load()
        except Exception as error:
            self.query_one("#contact_status", Static).update("Delete failed: {}".format(error))

    def action_edit(self) -> None:
        item = self._selected()
        if not item:
            return
        if self.tab == "friends":
            user_id = str(item.get("user_id") or item.get("uin") or "")
            current = str(item.get("remark") or "")
            self.app.push_screen(TextPrompt("Friend remark", value=current), lambda value: self.run_worker(self._edit_friend(user_id, value)))
        elif self.tab == "profile":
            self.app.push_screen(ProfileEditor(self.client, item), lambda saved: self.run_worker(self._load()) if saved else None)

    async def _edit_friend(self, user_id: str, value: Optional[str]) -> None:
        if value is None:
            return
        try:
            await self.client.update_friend_remark(user_id, value)
            await self._load()
        except Exception as error:
            self.query_one("#contact_status", Static).update("Remark failed: {}".format(error))

    def action_toggle_auto(self) -> None:
        self.run_worker(self._toggle_auto())

    async def _toggle_auto(self) -> None:
        try:
            payload = await self.client.update_contact_settings(not self.auto_approve)
            self.auto_approve = bool(payload.get("auto_approve_requests"))
            await self._load()
        except Exception as error:
            self.query_one("#contact_status", Static).update("Setting failed: {}".format(error))

    def action_cancel(self) -> None:
        self.dismiss(None)


class PluginConfigEditor(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False), Binding("ctrl+s", "save", show=False)]
    CSS = """
    PluginConfigEditor { align: center middle; background: $background 70%; }
    PluginConfigEditor > Container { width: 90; max-width: 98%; height: 90%; min-height: 7; border: solid $accent; background: $surface; padding: 1; }
    PluginConfigEditor #plugin_config_text { height: 1fr; }
    PluginConfigEditor #plugin_config_status { height: 1; color: $warning; }
    PluginConfigEditor .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient, plugin_id: str):
        super().__init__()
        self.client = client
        self.plugin_id = plugin_id

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Plugin config - {}".format(self.plugin_id), classes="dialog-title")
            yield TextArea("", id="plugin_config_text", soft_wrap=False, show_line_numbers=True, language="json")
            yield Static("Loading...", id="plugin_config_status")
            yield Static("Ctrl+S validate and save  Esc return", classes="hint")

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def _load(self) -> None:
        try:
            payload = await self.client.plugin_config(self.plugin_id)
            self.query_one("#plugin_config_text", TextArea).load_text(str(payload.get("text") or "{}"))
            self.query_one("#plugin_config_status", Static).update(str(payload.get("error") or ""))
            self.query_one("#plugin_config_text", TextArea).focus()
        except Exception as error:
            self.query_one("#plugin_config_status", Static).update("Load failed: {}".format(error))

    def action_save(self) -> None:
        text = self.query_one("#plugin_config_text", TextArea).text
        try:
            parsed = json.loads(text)
            if not isinstance(parsed, dict):
                raise ValueError("config.json must contain an object")
        except Exception as error:
            self.query_one("#plugin_config_status", Static).update("Invalid JSON: {}".format(error))
            return
        self.run_worker(self._save(text))

    async def _save(self, text: str) -> None:
        try:
            await self.client.update_plugin_config(self.plugin_id, text)
        except Exception as error:
            self.query_one("#plugin_config_status", Static).update("Save failed: {}".format(error))
            return
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


class PluginManagerScreen(ModalScreen):
    BINDINGS = [
        Binding("escape", "cancel", show=False), Binding("r", "refresh", show=False),
        Binding("e", "enable", show=False), Binding("d", "disable", show=False),
        Binding("x", "restart", show=False), Binding("c", "config", show=False),
    ]
    CSS = """
    PluginManagerScreen { align: center middle; background: $background 70%; }
    PluginManagerScreen > Container { width: 92; max-width: 98%; height: 90%; min-height: 7; border: solid $accent; background: $surface; padding: 1; }
    PluginManagerScreen #plugin_list { height: 1fr; }
    PluginManagerScreen #plugin_list > ListItem { height: auto; min-height: 2; padding: 0 1; }
    PluginManagerScreen #plugin_status { height: 1; color: $warning; }
    PluginManagerScreen .hint { height: 1; color: $text-muted; }
    """

    def __init__(self, client: WebQQClient):
        super().__init__()
        self.client = client
        self.plugins: List[Mapping[str, Any]] = []

    def compose(self) -> ComposeResult:
        with Container():
            yield Static("Plugins", classes="dialog-title")
            yield ListView(id="plugin_list")
            yield Static("", id="plugin_status")
            yield Static("R refresh  E enable  D disable  X restart  C config  Esc return", classes="hint")

    def on_mount(self) -> None:
        self.run_worker(self._load(False))

    async def _load(self, refresh: bool) -> None:
        self.query_one("#plugin_status", Static).update("Loading...")
        try:
            self.plugins = await (self.client.refresh_plugins() if refresh else self.client.plugins())
            await self._render_plugins()
            self.query_one("#plugin_status", Static).update("{} plugins".format(len(self.plugins)))
        except Exception as error:
            self.query_one("#plugin_status", Static).update("Load failed: {}".format(error))

    async def _render_plugins(self) -> None:
        view = self.query_one("#plugin_list", ListView)
        selected_id = ""
        selected = view.highlighted_child
        if isinstance(selected, DataListItem) and isinstance(selected.data, dict):
            selected_id = str(selected.data.get("id") or "")
        await view.clear()
        index = 0
        for offset, plugin in enumerate(self.plugins):
            plugin_id = str(plugin.get("id") or "")
            state = "enabled" if plugin.get("enabled") else "disabled"
            if plugin.get("enabled") and not plugin.get("loaded"):
                state = "error"
            capabilities = []
            if plugin.get("portal_receiver"):
                capabilities.append("portal")
            error = str(plugin.get("error") or plugin.get("config_error") or "")
            label = "{} [{}]{}\n{}".format(
                plugin_id, state, " - " + ", ".join(capabilities) if capabilities else "", error or plugin.get("description") or "",
            )
            await view.append(DataListItem(label, plugin))
            if plugin_id == selected_id:
                index = offset
        view.index = index if self.plugins else None
        view.focus()

    def _selected(self) -> Optional[Mapping[str, Any]]:
        item = self.query_one("#plugin_list", ListView).highlighted_child
        return item.data if isinstance(item, DataListItem) and isinstance(item.data, dict) else None

    def action_refresh(self) -> None:
        self.run_worker(self._load(True))

    def _action(self, name: str) -> None:
        plugin = self._selected()
        if plugin:
            self.run_worker(self._perform(str(plugin.get("id") or ""), name))

    def action_enable(self) -> None:
        self._action("enable")

    def action_disable(self) -> None:
        plugin = self._selected()
        if plugin:
            plugin_id = str(plugin.get("id") or "")
            self.app.push_screen(
                ConfirmDialog("Disable plugin", "Disable {} and stop its background work?".format(plugin_id)),
                lambda confirmed: self.run_worker(self._perform(plugin_id, "disable")) if confirmed else None,
            )

    def action_restart(self) -> None:
        self._action("restart")

    async def _perform(self, plugin_id: str, action: str) -> None:
        try:
            await self.client.plugin_action(plugin_id, action)
            await self._load(False)
        except Exception as error:
            self.query_one("#plugin_status", Static).update("{} failed: {}".format(action.title(), error))

    def action_config(self) -> None:
        plugin = self._selected()
        if plugin:
            self.app.push_screen(
                PluginConfigEditor(self.client, str(plugin.get("id") or "")),
                lambda saved: self.run_worker(self._load(False)) if saved else None,
            )

    def action_cancel(self) -> None:
        self.dismiss(None)


def render_image_cells(data: bytes, max_width: int, max_height: int) -> Text:
    try:
        from PIL import Image
    except ImportError as error:
        raise RuntimeError("Pillow is required for terminal image previews") from error
    with Image.open(io.BytesIO(data)) as source:
        image = source.convert("RGB")
        width_limit = max(2, max_width)
        pixel_height_limit = max(2, max_height * 2)
        scale = min(width_limit / image.width, pixel_height_limit / image.height, 1.0)
        size = (max(1, int(image.width * scale)), max(1, int(image.height * scale)))
        image = image.resize(size)
        if image.height % 2:
            padded = Image.new("RGB", (image.width, image.height + 1), (0, 0, 0))
            padded.paste(image, (0, 0))
            image = padded
        text = Text()
        pixels = image.load()
        for y in range(0, image.height, 2):
            for x in range(image.width):
                top = pixels[x, y]
                bottom = pixels[x, y + 1]
                text.append("▀", style=Style(color="rgb({},{},{})".format(*top), bgcolor="rgb({},{},{})".format(*bottom)))
            if y + 2 < image.height:
                text.append("\n")
        return text


class ImagePreview(ModalScreen):
    BINDINGS = [Binding("escape", "cancel", show=False)]
    CSS = """
    ImagePreview { align: center middle; background: $background 75%; }
    ImagePreview > Container { width: 96%; height: 94%; min-height: 7; border: solid $accent; background: $surface; padding: 1; }
    ImagePreview #image_preview { width: 1fr; height: 1fr; content-align: center middle; overflow: auto auto; }
    ImagePreview #image_status { height: 1; color: $text-muted; }
    """

    def __init__(
        self, client: WebQQClient, title: str, path: str, params: Optional[Mapping[str, str]] = None,
    ):
        super().__init__()
        self.client = client
        self.title = title
        self.path = path
        self.params = dict(params or {})

    def compose(self) -> ComposeResult:
        with Container():
            yield Static(self.title, classes="dialog-title")
            yield Static("Loading preview...", id="image_preview", markup=False)
            yield Static("Esc return", id="image_status")

    def on_mount(self) -> None:
        self.run_worker(self._load())

    async def _load(self) -> None:
        try:
            data, content_type = await self.client.fetch_bytes(self.path, self.params)
            preview = render_image_cells(data, max(2, self.size.width - 6), max(2, self.size.height - 6))
            self.query_one("#image_preview", Static).update(preview)
            self.query_one("#image_status", Static).update("{} - {} bytes - Esc return".format(content_type or "image", len(data)))
        except Exception as error:
            self.query_one("#image_preview", Static).update("Preview unavailable\n{}".format(error))

    def action_cancel(self) -> None:
        self.dismiss(None)
