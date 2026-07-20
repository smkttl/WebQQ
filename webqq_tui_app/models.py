import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional
from urllib.parse import unquote, urlsplit

from rich.text import Text


@dataclass(frozen=True)
class Chat:
    chat_id: str
    name: str
    chat_type: str
    last_time: float = 0
    last_text: str = ""
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "Chat":
        chat_id = str(data.get("chat_id") or "")
        return cls(
            chat_id=chat_id,
            name=str(data.get("name") or chat_id),
            chat_type=str(data.get("type") or ""),
            last_time=_number(data.get("last_time")),
            last_text=str(data.get("last_text") or ""),
            raw=dict(data),
        )


@dataclass(frozen=True)
class Attachment:
    kind: str
    name: str
    size: int
    data: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def downloadable(self) -> bool:
        return any(self.data.get(key) for key in ("url", "file", "id", "file_id", "msgId", "name"))


@dataclass
class Message:
    chat_id: str
    message_id: str
    local_id: str
    timestamp: float
    sender_id: str
    sender_name: str
    content: str
    self_sent: bool
    mentions: Dict[str, str] = field(default_factory=dict)
    attachments: List[Attachment] = field(default_factory=list)
    forwards: List[Mapping[str, Any]] = field(default_factory=list)
    extra_segments: List[Mapping[str, Any]] = field(default_factory=list)
    reactions: List[Mapping[str, Any]] = field(default_factory=list)
    recalled: bool = False
    pending: bool = False
    send_error: str = ""
    raw: Dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "Message":
        raw = dict(data)
        attachments: List[Attachment] = []
        for kind, key in (("image", "images"), ("file", "files"), ("video", "videos"), ("voice", "records")):
            values = data.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                if not isinstance(value, dict):
                    continue
                name = _attachment_name(kind, value)
                attachments.append(Attachment(kind, name, _integer(value.get("size") or value.get("fileSize")), dict(value)))
        mentions = data.get("mentions") if isinstance(data.get("mentions"), dict) else {}
        return cls(
            chat_id=str(data.get("chat_id") or ""),
            message_id=_identifier(data.get("message_id")),
            local_id=_identifier(data.get("local_id")),
            timestamp=_number(data.get("time")),
            sender_id=_identifier(data.get("sender_id")),
            sender_name=str(data.get("sender_name") or data.get("sender_id") or "Unknown"),
            content=str(data.get("content") or ""),
            self_sent=bool(data.get("self")),
            mentions={str(key): str(value) for key, value in mentions.items()},
            attachments=attachments,
            forwards=_dict_list(data.get("forwards")),
            extra_segments=_dict_list(data.get("extra_segments")),
            reactions=_dict_list(data.get("reactions")),
            recalled=bool(data.get("recalled")),
            pending=bool(data.get("pending")),
            send_error=str(data.get("send_error") or ""),
            raw=raw,
        )

    @property
    def stable_id(self) -> str:
        if self.message_id:
            return "message:" + self.message_id
        if self.local_id:
            return "local:" + self.local_id
        return "fallback:{:.6f}:{}:{}".format(self.timestamp, self.sender_id, self.content)

    @property
    def downloadable_attachments(self) -> List[Attachment]:
        return [item for item in self.attachments if item.downloadable]

    def merged(self, update: Mapping[str, Any]) -> "Message":
        raw = dict(self.raw)
        raw.update(update)
        return Message.from_json(raw)


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0


def _integer(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _identifier(value: Any) -> str:
    return "" if value is None else str(value)


def _dict_list(value: Any) -> List[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _attachment_name(kind: str, data: Mapping[str, Any]) -> str:
    for key in ("name", "fileName", "file_name", "filename"):
        if data.get(key):
            return str(data[key])
    file_value = str(data.get("file") or "")
    if file_value and not file_value.startswith(("http://", "https://")):
        return file_value.rsplit("/", 1)[-1]
    url = str(data.get("url") or "")
    if url:
        name = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
        if name:
            return name
    return kind


def human_size(size: int) -> str:
    if size <= 0:
        return ""
    value = float(size)
    units = ("B", "KB", "MB", "GB")
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    digits = 0 if unit == "B" or value >= 10 else 1
    return f"{value:.{digits}f} {unit}"


def format_timestamp(timestamp: float, now: Optional[datetime] = None) -> str:
    if not timestamp:
        return ""
    value = datetime.fromtimestamp(timestamp)
    now = now or datetime.now()
    if value.date() == now.date():
        return value.strftime("%H:%M:%S")
    if value.year == now.year:
        return value.strftime("%m-%d %H:%M")
    return value.strftime("%Y-%m-%d %H:%M")


MENTION_RE = re.compile(r"@\[(\d+)\]")
REPLY_RE = re.compile(r"\[reply:([^\]]+)\]")
MEDIA_TOKEN_RE = re.compile(r"\[(?:image|file|video|voice|forward)\]")


def display_content(message: Message) -> str:
    content = MENTION_RE.sub(lambda match: "@" + message.mentions.get(match.group(1), match.group(1)), message.content)
    content = REPLY_RE.sub(lambda match: "reply to #" + match.group(1), content)
    # Structured attachments are rendered below, so their transport tokens are noise here.
    if message.attachments or message.forwards:
        content = MEDIA_TOKEN_RE.sub("", content)
    return content.strip()


def format_chat(chat: Chat, compact: bool = False) -> Text:
    text = Text(no_wrap=True, overflow="ellipsis")
    prefix = "# " if chat.chat_type == "group" or chat.chat_id.startswith("group_") else "@ "
    text.append(prefix + chat.name, style="bold")
    if not compact:
        stamp = format_timestamp(chat.last_time)
        if stamp:
            text.append("  " + stamp, style="dim")
        if chat.last_text:
            text.append("\n" + chat.last_text.replace("\n", " "), style="dim")
    return text


def format_message(message: Message, compact: bool = False, search: str = "") -> Text:
    text = Text()
    stamp = format_timestamp(message.timestamp)
    sender_style = "bold cyan" if message.self_sent else "bold green"
    text.append(("You" if message.self_sent else message.sender_name) or "Unknown", style=sender_style)
    if stamp:
        text.append("  " + stamp, style="dim")
    if message.recalled:
        text.append("  RECALLED", style="bold red")
    if message.pending:
        text.append("  sending", style="yellow")
    if message.send_error:
        text.append("  SEND FAILED", style="bold red")

    body = display_content(message)
    if body:
        text.append("\n")
        _append_highlighted(text, body, search)

    for attachment in message.attachments:
        suffix = human_size(attachment.size)
        label = "[{}: {}{}]".format(
            attachment.kind,
            attachment.name,
            " ({})".format(suffix) if suffix and not compact else "",
        )
        text.append("\n" + label, style="magenta")
    for forward in message.forwards:
        nodes = forward.get("nodes") if isinstance(forward.get("nodes"), list) else []
        title = str(forward.get("title") or "Forwarded messages")
        text.append("\n[forward: {} - {} messages]".format(title, len(nodes)), style="magenta")
        if not compact:
            for node in nodes[:3]:
                if not isinstance(node, dict):
                    continue
                sender = str(node.get("sender_name") or node.get("sender_id") or "Unknown")
                content = str(node.get("content") or "").replace("\n", " ").strip()
                if content:
                    text.append("\n  {}: {}".format(sender, content), style="dim")
            if len(nodes) > 3:
                text.append("\n  ... {} more".format(len(nodes) - 3), style="dim")
    for segment in message.extra_segments:
        label = str(segment.get("label") or "[{}]".format(segment.get("type") or "unknown"))
        detail = str(segment.get("text") or "")
        if compact and len(detail) > 80:
            detail = detail[:77] + "..."
        text.append("\n{}{}".format(label, " " + detail if detail else ""), style="magenta")

    if message.reactions:
        labels = []
        for reaction in message.reactions:
            emoji_id = str(reaction.get("emoji_id") or "?")
            count = _integer(reaction.get("count"))
            labels.append("[face:{}] x{}".format(emoji_id, count or 1))
        text.append("\n" + "  ".join(labels), style="yellow")
    if message.send_error and not compact:
        text.append("\n" + message.send_error, style="red")
    return text


def _append_highlighted(target: Text, value: str, search: str) -> None:
    if not search:
        target.append(value)
        return
    lower = value.casefold()
    needle = search.casefold()
    start = 0
    while True:
        index = lower.find(needle, start)
        if index < 0:
            target.append(value[start:])
            return
        target.append(value[start:index])
        target.append(value[index:index + len(search)], style="black on yellow")
        start = index + len(search)


def message_matches(message: Message, query: str) -> bool:
    query = query.strip().casefold()
    if not query:
        return False
    haystack = "\n".join((message.sender_name, message.sender_id, display_content(message))).casefold()
    return query in haystack


def deduplicate_messages(messages: Iterable[Message]) -> List[Message]:
    result: List[Message] = []
    positions: Dict[str, int] = {}
    local_positions: Dict[str, int] = {}
    for message in messages:
        keys = [message.stable_id]
        if message.local_id:
            keys.append("local:" + message.local_id)
        existing = next((positions[key] for key in keys if key in positions), None)
        if existing is None and message.message_id and message.local_id:
            existing = local_positions.get(message.local_id)
        if existing is None:
            existing = len(result)
            result.append(message)
        else:
            result[existing] = message
        for key in keys:
            positions[key] = existing
        if message.local_id:
            local_positions[message.local_id] = existing
    result.sort(key=lambda item: (item.timestamp, item.message_id or item.local_id))
    return result
