import asyncio
import json
import os
import re
import time
from pathlib import Path

from webqq_app.common import extract_message_id


COMMAND_RE = re.compile(r"^\[reply:([^\]]+)\]\s*(/prev|/next)\s*$", re.IGNORECASE)
PLAIN_COMMAND_RE = re.compile(r"^(/prev|/next)\s*$", re.IGNORECASE)
REPLY_PREFIX_RE = re.compile(r"^\[reply:[^\]]+\]")
FACE_TOKEN_RE = re.compile(r"\[face:(\d+)\]")
AT_TOKEN_RE = re.compile(r"@\[(\d+|all)\]")

MESSAGE_LABEL = "Message: "
STALE_TEXT = "sorry, the message is too stale"
NO_REVOKED_TEXT = "No revoked message found"
NO_QUOTE_TEXT = "Please reply to a message with /prev or /next."

PLACEHOLDER_TOKENS = {
    "[image]", "[video]", "[voice]", "[file]", "[forward]",
    "[mface]", "[onlinefile]", "[flashtransfer]",
}


def _strip_placeholders(content):
    for token in PLACEHOLDER_TOKENS:
        content = content.replace(token, "")
    return content


def _tokenize_content(content):
    parts = []
    pattern = re.compile(r"\[face:\d+\]|@\[(?:\d+|all)\]")
    position = 0
    for match in pattern.finditer(content):
        if match.start() > position:
            _append_text(parts, content[position:match.start()])
        token = match.group(0)
        if token.startswith("[face:"):
            parts.append({"type": "face", "data": {"id": token[len("[face:"):-1]}})
        else:
            parts.append({"type": "at", "data": {"qq": token[len("@["):-1]}})
        position = match.end()
    _append_text(parts, content[position:])
    return parts


def _append_text(parts, text):
    if not text:
        return
    if parts and parts[-1].get("type") == "text":
        parts[-1]["data"]["text"] += text
    else:
        parts.append({"type": "text", "data": {"text": text}})


def _reproduction_prefix(delay_seconds):
    return "Message Reproduction (Timeout: {}s): ".format(int(delay_seconds))


def _replay_source(item):
    url = str(item.get("url") or "").strip()
    if url.startswith(("http://", "https://", "data:")):
        return url
    file = str(item.get("file") or "").strip()
    if file.startswith(("http://", "https://", "data:")):
        return file
    if file and Path(file).is_file():
        return str(Path(file).resolve())
    return ""


def _build_reproduction_segments(target, delay_seconds):
    content = REPLY_PREFIX_RE.sub("", str(target.get("content") or ""), count=1)
    content = _strip_placeholders(content)
    segments = _tokenize_content(content)
    sender_id = str(target.get("sender_id") or "").strip()
    if sender_id:
        header = [
            {"type": "text", "data": {"text": _reproduction_prefix(delay_seconds) + "Sender: "}},
            {"type": "at", "data": {"qq": sender_id}},
            {"type": "text", "data": {"text": " " + MESSAGE_LABEL}},
        ]
    else:
        header = [{"type": "text", "data": {"text": _reproduction_prefix(delay_seconds) + MESSAGE_LABEL}}]
    segments = header + segments

    for kind, segments_key, segment_type in (
        ("image", "images", "image"),
        ("video", "videos", "video"),
        ("voice", "records", "record"),
    ):
        items = target.get(segments_key) or []
        replayed = False
        for item in items:
            source = _replay_source(item)
            if not source:
                continue
            segments.append({"type": segment_type, "data": {"file": source}})
            replayed = True
        if items and not replayed:
            _append_text(segments, f"[{kind}]")

    files = target.get("files") or []
    if files:
        _append_text(segments, "[file]")
    forwards = target.get("forwards") or []
    if forwards:
        title = str((forwards[0] if isinstance(forwards, list) else {}).get("title") or "Forwarded messages")
        _append_text(segments, f"[forward: {title}]")

    for extra in target.get("extra_segments") or []:
        segment_type = str(extra.get("type") or "unknown")
        if segment_type == "music":
            url = str(extra.get("url") or "").strip()
            audio = str(extra.get("audio") or "").strip()
            if url.startswith(("http://", "https://")) and audio.startswith(("http://", "https://")):
                music_data = {"type": "custom", "url": url, "audio": audio}
                title = str(extra.get("title") or "").strip()
                if title:
                    music_data["title"] = title
                segments.append({"type": "music", "data": music_data})
            else:
                _append_text(segments, str(extra.get("label") or "[music]"))
        elif segment_type == "contact":
            contact_id = str(extra.get("contact_id") or "").strip()
            if contact_id:
                segments.append({
                    "type": "contact",
                    "data": {
                        "type": str(extra.get("contact_type") or "qq"),
                        "id": contact_id,
                    },
                })
            else:
                _append_text(segments, str(extra.get("label") or "[contact]"))
        elif segment_type == "location":
            latitude = extra.get("latitude")
            longitude = extra.get("longitude")
            if latitude is not None and longitude is not None:
                segments.append({
                    "type": "location",
                    "data": {"lat": float(latitude), "lon": float(longitude)},
                })
            else:
                _append_text(segments, str(extra.get("label") or "[location]"))
        elif segment_type in ("dice", "rps"):
            segments.append({"type": segment_type, "data": {"result": "0"}})
        else:
            label = str(extra.get("label") or f"[{segment_type}]")
            title = str(extra.get("title") or extra.get("text") or "").strip()
            _append_text(segments, f"{label}: {title}" if title else label)
    return segments


def _segments_text(segments):
    parts = []
    for segment in segments:
        if segment.get("type") == "text":
            parts.append(str(segment.get("data", {}).get("text") or ""))
        elif segment.get("type") == "at":
            qq = str(segment.get("data", {}).get("qq") or "")
            parts.append(f"@[{qq}]")
    return "".join(parts)


def setup(ctx):
    return AntiRevokePlugin(ctx)


class AntiRevokePlugin:
    def __init__(self, ctx, state_path=None):
        self.ctx = ctx
        self.base_path = Path(__file__).resolve().parent
        self.state_path = Path(state_path) if state_path else self.base_path / "state.json"
        self.state = self._load_state()
        self._rearm()

    def _load_state(self):
        if self.state_path.is_file():
            try:
                with open(self.state_path, encoding="utf-8") as stream:
                    data = json.load(stream)
                if isinstance(data, dict) and isinstance(data.get("pending"), list):
                    return data
            except Exception as error:
                self.ctx.log(f"failed to load state: {error}")
        return {"pending": []}

    def _save_state(self):
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.state_path.with_suffix(".tmp")
            with open(temp_path, "w", encoding="utf-8") as stream:
                json.dump(self.state, stream, ensure_ascii=False, indent=2)
            os.replace(temp_path, self.state_path)
        except Exception as error:
            self.ctx.log(f"failed to save state: {error}")

    def _rearm(self):
        now = time.time()
        kept = []
        for entry in list(self.state.get("pending") or []):
            message_id = str(entry.get("message_id") or "")
            chat_id = str(entry.get("chat_id") or "")
            try:
                deadline = float(entry.get("deadline") or 0)
            except (TypeError, ValueError):
                deadline = 0
            if not message_id or not chat_id:
                continue
            kept.append(entry)
            remaining = deadline - now
            if remaining <= 0:
                self.ctx.create_task(self._recall_overdue(chat_id, message_id))
            else:
                self.ctx.create_task(self._recall_after(chat_id, message_id, remaining))
        self.state["pending"] = kept
        self._save_state()

    def _remove_pending(self, message_id):
        key = str(message_id)
        before = len(self.state.get("pending") or [])
        self.state["pending"] = [
            entry for entry in self.state.get("pending") or []
            if str(entry.get("message_id") or "") != key
        ]
        if len(self.state["pending"]) != before:
            self._save_state()

    def _schedule_recall(self, chat_id, message_id, delay):
        self.state.setdefault("pending", []).append({
            "message_id": str(message_id),
            "chat_id": str(chat_id),
            "deadline": time.time() + delay,
        })
        self._save_state()
        self.ctx.create_task(self._recall_after(chat_id, message_id, delay))

    async def _recall_after(self, chat_id, message_id, delay):
        try:
            await asyncio.sleep(delay)
            await self._delete_message(chat_id, message_id)
            self.ctx.log(f"recalled reproduction {message_id}")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.ctx.log(f"failed to recall reproduction {message_id}: {error}")
        self._remove_pending(message_id)

    async def _recall_overdue(self, chat_id, message_id):
        try:
            await self._delete_message(chat_id, message_id)
            self.ctx.log(f"recalled overdue reproduction {message_id}")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.ctx.log(f"failed to recall overdue reproduction {message_id}: {error}")
        self._remove_pending(message_id)

    async def _delete_message(self, chat_id, message_id):
        await self.ctx.napcat("delete_msg", {"message_id": int(message_id)})

    async def handle_event(self, event, ctx):
        if event.get("type") != "message":
            return
        message = event.get("message") or {}
        if message.get("self") or str(message.get("source") or "").startswith("plugin:"):
            return
        chat_id = str(message.get("chat_id") or "")
        if message.get("type") != "group" and not chat_id.startswith("group_"):
            return
        content = str(message.get("content") or "").strip()
        match = COMMAND_RE.match(content)
        if not match:
            if PLAIN_COMMAND_RE.match(content):
                await self._reply(ctx, chat_id, message, NO_QUOTE_TEXT)
            return
        anchor_id = match.group(1)
        direction = match.group(2).lower().lstrip("/")
        command_id = str(message.get("message_id") or "")
        try:
            limit = max(1, min(int(ctx.config.get("lookup_limit", 200)), 1000))
        except (TypeError, ValueError):
            limit = 200
        messages = sorted(
            ctx.get_messages(chat_id, limit=limit) or [],
            key=lambda item: (item.get("time", 0), str(item.get("message_id") or "")),
        )
        real = [
            item for item in messages
            if not item.get("system") and str(item.get("message_id") or "") != command_id
        ]
        anchor_index = next(
            (index for index, item in enumerate(real) if str(item.get("message_id") or "") == anchor_id),
            None,
        )
        if anchor_index is None:
            await self._reply(ctx, chat_id, message, STALE_TEXT)
            return
        if direction == "prev":
            candidates = reversed(real[:anchor_index])
        else:
            candidates = real[anchor_index + 1:]
        target = next((item for item in candidates if item.get("recalled")), None)
        if target is None:
            await self._reply(ctx, chat_id, message, NO_REVOKED_TEXT)
            return
        await self._reproduce(ctx, chat_id, message, target)

    async def _reply(self, ctx, chat_id, message, text):
        try:
            await ctx.send_message(chat_id, text, reply_to=str(message.get("message_id") or ""))
        except Exception as error:
            ctx.log(f"send failed: {error}")

    async def _reproduce(self, ctx, chat_id, command_message, target):
        try:
            delay = float(ctx.config.get("recall_delay_seconds", 110))
        except (TypeError, ValueError):
            delay = 110
        if delay <= 0:
            delay = 110
        segments = _build_reproduction_segments(target, delay)
        text = _segments_text(segments)
        reply_to = str(command_message.get("message_id") or "") or None
        try:
            result = await ctx.send_segments(chat_id, segments, text=text, reply_to=reply_to)
        except Exception as error:
            ctx.log(f"reproduction failed: {error}")
            return
        message_id = extract_message_id(result)
        if message_id is None:
            ctx.log("reproduction sent but its message id is unavailable")
            return
        self._schedule_recall(chat_id, str(message_id), delay)
