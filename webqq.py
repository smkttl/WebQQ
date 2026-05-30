#!/usr/bin/env python3
import asyncio
import json
import os
import time
import uuid
import hmac
import tempfile
import mimetypes
from urllib.parse import quote, urlparse
from collections import defaultdict, deque
from pathlib import Path

from aiohttp import web
import aiohttp

CONFIG_PATH = Path(__file__).parent / "config.json"
STATIC_DIR = Path(__file__).parent / "static"
DATA_DIR = Path(__file__).parent / "data"
AVATAR_DIR = DATA_DIR / "avatars"
MAX_MESSAGES = 1000
MAX_FILE_UPLOAD = 100 * 1024 * 1024
AVATAR_CACHE_TTL = 7 * 24 * 60 * 60
REACTION_EMOJI_IDS = tuple(str(i) for i in (
    4, 5, 8, 9, 10, 12, 14, 16, 21, 23, 24, 25, 26, 27, 28, 29, 30, 32,
    33, 34, 38, 39, 41, 42, 43, 49, 53, 60, 63, 66, 74, 75, 76, 78, 79,
    85, 89, 96, 97, 98, 99, 100, 101, 102, 103, 104, 106, 109, 111, 116,
    118, 120, 122, 123, 124, 125, 129, 144, 147, 171, 173, 174, 175, 176,
    179, 180, 181, 182, 183, 201, 203, 212, 214, 219, 222, 227, 232, 240,
    243, 246, 262, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 277,
    278, 281, 282, 284, 285, 287, 289, 290, 293, 294, 297, 298, 299, 305,
    306, 307, 314, 315, 318, 319, 320, 322, 324, 326,
))
REACTION_FETCH_EMOJI_IDS = ("14", "1", "4", "5", "8", "9", "21", "23", "24", "66")

DEFAULT_CONFIG = {
    "ws_url": "ws://localhost:49341/?message_post_format=array",
    "napcat_token": "",
    "web_port": 8080,
    "web_token": "",
    "flush_interval": 15,
}


def parse_chat_id(chat_id):
    if not isinstance(chat_id, str):
        return None
    parts = chat_id.split("_")
    if len(parts) == 2 and parts[0] in ("group", "private") and parts[1].isdigit():
        return {"type": parts[0], f"{parts[0]}_id": int(parts[1])}
    if len(parts) == 3 and parts[0] == "temp" and parts[1].isdigit() and parts[2].isdigit():
        return {"type": "temp", "group_id": int(parts[1]), "user_id": int(parts[2])}
    return None


def avatar_url_for(avatar_type, avatar_id):
    if str(avatar_id).isdigit() and avatar_type in ("user", "group"):
        return f"/api/avatar?type={avatar_type}&id={avatar_id}"
    return ""


def chat_avatar_url(chat_id, chat_type="", user_id=None, group_id=None):
    parsed = parse_chat_id(chat_id)
    if parsed:
        if parsed["type"] == "group":
            return avatar_url_for("group", parsed["group_id"])
        if parsed["type"] == "private":
            return avatar_url_for("user", parsed["private_id"])
        if parsed["type"] == "temp":
            return avatar_url_for("user", parsed["user_id"])
    if chat_type == "group" and group_id:
        return avatar_url_for("group", group_id)
    if chat_type in ("private", "temp") and user_id:
        return avatar_url_for("user", user_id)
    return ""


def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = dict(DEFAULT_CONFIG)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
        print(f"[config] created {CONFIG_PATH} with defaults")
    return cfg


class MessageStore:
    def __init__(self, maxlen=1000, data_dir=DATA_DIR):
        self.maxlen = maxlen
        self._data = defaultdict(lambda: deque(maxlen=maxlen))
        self._chat_meta = {}
        self._data_dir = data_dir
        self._dirty = set()
        self._nicknames = {}  # uid -> nickname
        self._group_members = defaultdict(dict)  # chat_id -> {uid: nickname}
        self._group_member_details = defaultdict(list)  # chat_id -> [{user_id, names...}]
        self._message_chat_index = {}  # message_id -> chat_id
        self._pending_local_reactions = {}  # (message_id, emoji_id) -> user
        self._self_user = {"user_id": "self", "name": "You"}
        data_dir.mkdir(exist_ok=True)

    def _chat_path(self, chat_id):
        return self._data_dir / f"{chat_id}.json"

    def load_all(self):
        for fp in self._data_dir.glob("*.json"):
            chat_id = fp.stem
            try:
                with open(fp, encoding="utf-8") as f:
                    msgs = json.load(f)
                if isinstance(msgs, list):
                    self._data[chat_id] = deque(msgs[-self.maxlen:], maxlen=self.maxlen)
                    self._reindex_chat(chat_id)
                    if msgs:
                        last = msgs[-1]
                        self._chat_meta[chat_id] = {
                            "chat_id": chat_id,
                            "name": last.get("chat_name", chat_id),
                            "type": last.get("type", ""),
                            "last_time": last.get("time", 0),
                            "last_text": (last.get("content", "") or "")[:50],
                            "avatar_url": chat_avatar_url(chat_id, last.get("type", ""), last.get("user_id"), last.get("group_id")),
                        }
            except Exception:
                pass

    def flush(self, chat_id=None):
        targets = [chat_id] if chat_id else list(self._dirty)
        for cid in targets:
            if cid not in self._data:
                continue
            try:
                with open(self._chat_path(cid), "w", encoding="utf-8") as f:
                    json.dump(list(self._data[cid]), f, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                pass
        self._dirty -= set(targets)

    def chat_key(self, msg):
        mt = msg.get("message_type")
        if mt == "group":
            return f"group_{msg['group_id']}"
        if mt == "private":
            if msg.get("sub_type") == "group" and msg.get("group_id"):
                return f"temp_{msg['group_id']}_{msg['user_id']}"
            return f"private_{msg['user_id']}"
        return None

    def add(self, msg):
        key = self.chat_key(msg)
        if not key:
            return
        simplified = self._simplify(msg)
        self.append_simplified(key, simplified)
        if key not in self._chat_meta:
            self._chat_meta[key] = {
                "chat_id": key,
                "name": simplified.get("chat_name", key),
                "type": simplified.get("type", ""),
                "last_time": 0,
                "last_text": "",
            }
        self._chat_meta[key]["name"] = simplified.get("chat_name") or self._chat_meta[key]["name"]
        self._chat_meta[key]["avatar_url"] = chat_avatar_url(key, simplified.get("type", ""), simplified.get("user_id"), simplified.get("group_id"))
        self._chat_meta[key]["last_time"] = simplified["time"]
        self._chat_meta[key]["last_text"] = simplified["content"][:50]
        self._dirty.add(key)
        return simplified

    def append_simplified(self, chat_id, simplified):
        self._data[chat_id].append(simplified)
        self._reindex_chat(chat_id)

    def _reindex_chat(self, chat_id):
        for message_id, indexed_chat_id in list(self._message_chat_index.items()):
            if indexed_chat_id == chat_id:
                self._message_chat_index.pop(message_id, None)
        for msg in self._data.get(chat_id, []):
            message_id = msg.get("message_id")
            if message_id is not None:
                self._message_chat_index[str(message_id)] = chat_id

    def remember_local_reaction(self, message_id, emoji_id, chat_id=None):
        key = (str(message_id), str(emoji_id))
        self._pending_local_reactions[key] = dict(self._self_user)
        if chat_id and chat_id in self._data:
            self._message_chat_index.setdefault(str(message_id), chat_id)

    def add_local_reaction(self, message_id, emoji_id, chat_id=None):
        key = str(message_id)
        emoji_id = str(emoji_id)
        known_chat_id = self._message_chat_index.get(key) or self.find_chat_by_message_id(key)
        if not known_chat_id and chat_id in self._data:
            known_chat_id = chat_id
        if not known_chat_id:
            return None
        self_user = dict(self._self_user)
        self_uid = str(self_user.get("user_id") or "")
        for msg in reversed(self._data.get(known_chat_id, [])):
            if str(msg.get("message_id")) != key:
                continue
            reactions = [
                dict(item)
                for item in (msg.get("reactions") or [])
                if isinstance(item, dict) and item.get("emoji_id") is not None
            ]
            target = None
            for reaction in reactions:
                if str(reaction.get("emoji_id")) == emoji_id:
                    target = reaction
                    break
            if target is None:
                target = {"emoji_id": emoji_id, "count": 0, "users": []}
                reactions.append(target)
            users = target.get("users") if isinstance(target.get("users"), list) else []
            users = self._dedupe_reaction_users(users)
            already_reacted = self_uid and any(
                str(user.get("user_id")) == self_uid
                for user in users
                if isinstance(user, dict)
            )
            if not already_reacted:
                users.append(self_user)
            users = self._dedupe_reaction_users(users)
            prior_count = reaction_count(target)
            target["emoji_id"] = emoji_id
            target["users"] = users
            target["count"] = max(prior_count + (0 if already_reacted else 1), len(users), 1)
            msg["reactions"] = reactions
            self._message_chat_index[key] = known_chat_id
            self._dirty.add(known_chat_id)
            return {"chat_id": known_chat_id, "reactions": reactions}
        self._message_chat_index.pop(key, None)
        return None

    def set_self_user(self, user_id, name=""):
        if user_id is None:
            return
        uid = str(user_id)
        display = name or self._nicknames.get(uid) or uid
        self._self_user = {"user_id": uid, "name": display}
        self._nicknames[uid] = display

    def apply_reactions(self, message_id, reactions, chat_id=None, notice_user=None):
        key = str(message_id)
        known_chat_id = self._message_chat_index.get(key) or self.find_chat_by_message_id(key)
        if not known_chat_id and chat_id in self._data:
            known_chat_id = chat_id
        if not known_chat_id:
            return None
        normalized = reactions if isinstance(reactions, list) else []
        for msg in reversed(self._data.get(known_chat_id, [])):
            if str(msg.get("message_id")) == key:
                merged = self._merge_reactions(key, normalized, msg.get("reactions") or [], notice_user=notice_user)
                msg["reactions"] = merged
                self._message_chat_index[key] = known_chat_id
                self._dirty.add(known_chat_id)
                return {"chat_id": known_chat_id, "reactions": merged}
        self._message_chat_index.pop(key, None)
        return None

    def _merge_reactions(self, message_id, reactions, previous_reactions, notice_user=None):
        previous = {
            str(item.get("emoji_id")): item
            for item in previous_reactions
            if isinstance(item, dict) and item.get("emoji_id") is not None
        }
        updates = self._merge_reaction_users(message_id, reactions, previous, notice_user=notice_user)
        merged = dict(previous)
        for reaction in updates:
            emoji_id = str(reaction.get("emoji_id"))
            if reaction_count(reaction) > 0:
                merged[emoji_id] = reaction
            else:
                merged.pop(emoji_id, None)
        return list(merged.values())

    def _merge_reaction_users(self, message_id, reactions, previous, notice_user=None):
        merged = []
        for reaction in reactions:
            if not isinstance(reaction, dict):
                continue
            reaction = dict(reaction)
            emoji_id = str(reaction.get("emoji_id"))
            users = reaction.get("users") if isinstance(reaction.get("users"), list) else []
            if not users:
                prior_users = previous.get(emoji_id, {}).get("users", [])
                if isinstance(prior_users, list) and prior_users:
                    users = prior_users
            users = self._dedupe_reaction_users(users)
            if notice_user and not any(str(u.get("user_id")) == str(notice_user["user_id"]) for u in users if isinstance(u, dict)):
                users = users + [notice_user]
            pending_key = (str(message_id), emoji_id)
            pending_user = self._pending_local_reactions.pop(pending_key, None)
            if pending_user and not any(str(u.get("user_id")) == str(pending_user["user_id"]) for u in users if isinstance(u, dict)):
                users = users + [pending_user]
            users = self._dedupe_reaction_users(users)
            reaction["users"] = users
            merged.append(reaction)
        return merged

    def _dedupe_reaction_users(self, users):
        if not isinstance(users, list):
            return []
        normalized = []
        seen = set()
        self_id = str(self._self_user.get("user_id") or "")
        for user in users:
            if not isinstance(user, dict):
                continue
            uid = str(user.get("user_id") or "")
            if uid == "self" and self_id and self_id != "self":
                uid = self_id
                user = {"user_id": self_id, "name": self._self_user.get("name") or user.get("name") or self_id}
            elif uid:
                user = {**user, "user_id": uid}
            if uid and uid in seen:
                continue
            if uid:
                seen.add(uid)
            normalized.append(user)
        return normalized

    def find_chat_by_message_id(self, message_id):
        key = str(message_id)
        for chat_id, msgs in self._data.items():
            for msg in msgs:
                if str(msg.get("message_id")) == key:
                    self._message_chat_index[key] = chat_id
                    return chat_id
        return None

    def _simplify(self, msg):
        sender = msg.get("sender") or {}
        chat_name = ""
        mt = msg.get("message_type", "")
        chat_type = mt
        if mt == "group":
            chat_name = msg.get("group_name", "")
        if mt == "private":
            if msg.get("sub_type") == "group" and msg.get("group_id"):
                chat_type = "temp"
                sender_name = sender.get("card") or sender.get("nickname") or str(msg.get("user_id", ""))
                group_name = msg.get("group_name", "")
                chat_name = f"{group_name} / {sender_name}" if group_name else f"群临时会话: {sender_name}"
            else:
                chat_name = sender.get("nickname", str(msg.get("user_id", "")))
        if sender.get("user_id"):
            nick = sender.get("card") or sender.get("nickname") or ""
            if nick:
                self._nicknames[str(sender["user_id"])] = nick
        content, mentions, images, forwards, files = self._extract_text(msg)
        return {
            "message_id": msg.get("message_id"),
            "time": msg.get("time", int(time.time())),
            "sender_id": msg.get("user_id"),
            "sender_name": sender.get("card", "") or sender.get("nickname", "") or str(msg.get("user_id", "")),
            "sender_avatar_url": avatar_url_for("user", msg.get("user_id")),
            "content": content,
            "mentions": mentions,
            "images": images,
            "forwards": forwards,
            "files": files,
            "reactions": normalize_emoji_likes(msg.get("reactions") or msg.get("likes") or msg.get("like") or []),
            "chat_id": self.chat_key(msg),
            "type": chat_type,
            "group_id": msg.get("group_id"),
            "user_id": msg.get("user_id"),
            "chat_name": chat_name,
            "self": (
                (msg.get("sub_type") == "friend" and msg.get("target_id") == msg.get("self_user_id"))
                or (msg.get("self_user_id") is not None and msg.get("user_id") == msg.get("self_user_id"))
            ),
        }

    def _extract_text(self, msg):
        segments = msg.get("message", [])
        mentions = {}
        images = []
        forwards = []
        files = []
        if isinstance(segments, list):
            parts = []
            for seg in segments:
                if not isinstance(seg, dict):
                    parts.append(str(seg))
                    continue
                t = seg.get("type", "")
                d = seg.get("data", {})
                if not isinstance(d, dict):
                    d = {}
                if t == "text":
                    parts.append(d.get("text", ""))
                elif t == "image":
                    url = d.get("url", "")
                    file = d.get("file", "")
                    if not url and isinstance(file, str) and file.startswith(("http://", "https://", "data:")):
                        url = file
                    image = {
                        "url": url,
                        "thumbnail": d.get("thumbnail") or d.get("preview") or d.get("thumb") or d.get("url", ""),
                        "file": file,
                        "summary": d.get("summary", ""),
                        "sub_type": d.get("sub_type", ""),
                        "width": first_positive_int(d.get("width"), d.get("w"), d.get("image_width"), d.get("imageWidth")),
                        "height": first_positive_int(d.get("height"), d.get("h"), d.get("image_height"), d.get("imageHeight")),
                    }
                    images.append({k: v for k, v in image.items() if v})
                    parts.append("[image]")
                elif t == "face":
                    parts.append(f"[face:{d.get('id', '')}]")
                elif t == "at":
                    qq = d.get("qq", "")
                    nick = self._nicknames.get(qq, qq)
                    mentions[qq] = nick
                    parts.append(f"@[{qq}]")
                elif t == "reply":
                    parts.append(f"[reply:{d.get('id', '')}]")
                elif t == "forward":
                    forward = self._simplify_forward_segment(d)
                    forwards.append(forward)
                    parts.append("[forward]")
                elif t == "file":
                    file_item = self._simplify_file_segment(d)
                    if file_item:
                        files.append(file_item)
                    parts.append("[file]")
                else:
                    parts.append(f"[{t}]")
            return "".join(parts), mentions, images, forwards, files
        if isinstance(segments, str):
            return segments, mentions, images, forwards, files
        return str(msg.get("raw_message", "")), mentions, images, forwards, files

    @staticmethod
    def _simplify_file_segment(data):
        if not isinstance(data, dict):
            return None
        file_item = {
            "id": data.get("id") or data.get("file_id") or data.get("fileId"),
            "name": data.get("name") or data.get("file_name") or data.get("filename") or data.get("file"),
            "size": data.get("size") or data.get("file_size") or data.get("fileSize"),
            "url": data.get("url"),
            "file": data.get("file"),
            "busid": data.get("busid") or data.get("bus_id") or data.get("busId"),
        }
        return {k: v for k, v in file_item.items() if v is not None and v != ""}

    def _simplify_forward_segment(self, data):
        forward = {
            "id": data.get("id") or data.get("message_id") or data.get("resid") or "",
            "title": data.get("title") or data.get("name") or "Forwarded messages",
            "summary": data.get("summary") or data.get("desc") or "",
            "status": "unavailable" if data.get("error") else "pending",
            "error": data.get("error", ""),
            "nodes": [],
        }
        nodes = self._extract_forward_nodes(data.get("content") or data.get("nodes") or data.get("messages"))
        if nodes:
            forward["nodes"] = nodes
            forward["status"] = "ok"
        return {k: v for k, v in forward.items() if v or k in ("nodes", "status")}

    def _extract_forward_nodes(self, payload):
        if not payload:
            return []
        if isinstance(payload, dict):
            if payload.get("type") == "node" and isinstance(payload.get("data"), dict):
                return [self._simplify_forward_node(payload["data"])]
            node_keys = ("sender", "nickname", "name", "user_id", "uin", "time", "timestamp")
            container_keys = ("messages", "nodes")
            if not any(key in payload for key in node_keys):
                container_keys = container_keys + ("message", "content")
            for key in container_keys:
                nodes = self._extract_forward_nodes(payload.get(key))
                if nodes:
                    return nodes
            return [self._simplify_forward_node(payload)]
        if isinstance(payload, list):
            nodes = []
            for item in payload:
                if isinstance(item, dict) and item.get("type") == "node" and isinstance(item.get("data"), dict):
                    nodes.append(self._simplify_forward_node(item["data"]))
                elif isinstance(item, dict):
                    nodes.append(self._simplify_forward_node(item))
                else:
                    node = self._simplify_forward_node({"content": str(item)})
                    if node.get("content"):
                        nodes.append(node)
            return nodes
        if isinstance(payload, str):
            return [self._simplify_forward_node({"content": payload})]
        return []

    def _simplify_forward_node(self, node):
        sender = node.get("sender") if isinstance(node.get("sender"), dict) else {}
        content = node.get("content", node.get("message", ""))
        fake_msg = {
            "message": content,
            "raw_message": content if isinstance(content, str) else "",
        }
        text, mentions, images, forwards, files = self._extract_text(fake_msg)
        return {
            "sender_id": node.get("user_id") or node.get("uin") or sender.get("user_id") or "",
            "sender_name": node.get("nickname") or node.get("name") or sender.get("nickname") or sender.get("card") or "",
            "time": node.get("time") or node.get("timestamp") or 0,
            "content": text,
            "mentions": mentions,
            "images": images,
            "forwards": forwards,
            "files": files,
        }

    def get_messages(self, chat_id, limit=50, before=None):
        msgs = list(self._data.get(chat_id, []))
        if before:
            msgs = [m for m in msgs if m["time"] < before]
        return msgs[-limit:]

    def get_chats(self):
        return sorted(self._chat_meta.values(), key=lambda c: c.get("last_time", 0), reverse=True)

    def ensure_chat(self, chat_id, name, chat_type, **extra):
        if chat_id in self._chat_meta:
            self._chat_meta[chat_id]["name"] = name
            self._chat_meta[chat_id]["type"] = chat_type
            self._chat_meta[chat_id].update(extra)
        else:
            self._chat_meta[chat_id] = {
                "chat_id": chat_id,
                "name": name,
                "type": chat_type,
                "last_time": 0,
                "last_text": "",
                "avatar_url": chat_avatar_url(chat_id, chat_type, extra.get("user_id"), extra.get("group_id")),
                **extra,
            }
        self._chat_meta[chat_id]["avatar_url"] = chat_avatar_url(
            chat_id,
            self._chat_meta[chat_id].get("type", chat_type),
            self._chat_meta[chat_id].get("user_id"),
            self._chat_meta[chat_id].get("group_id"),
        )


class NapCatConnection:
    def __init__(self, ws_url, token, store):
        self.ws_url = ws_url
        self.token = token
        self.store = store
        self.session = None
        self.ws = None
        self._pending = {}
        self._subscribers = []

    async def start(self):
        self.session = aiohttp.ClientSession()
        while True:
            try:
                await self._connect_and_listen()
            except Exception as e:
                print(f"[napcat] error: {e}, reconnecting in 5s...")
                self.ws = None
                await asyncio.sleep(5)

    async def _connect_and_listen(self):
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        async with self.session.ws_connect(self.ws_url, headers=headers) as ws:
            self.ws = ws
            print("[napcat] connected")
            asyncio.create_task(self._fetch_contacts())
            async for raw_msg in ws:
                if raw_msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(raw_msg.data)
                    except Exception:
                        continue
                    await self._handle(data)
                elif raw_msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    break
            self.ws = None

    async def _fetch_contacts(self):
        try:
            login = await self._request("get_login_info", {})
            if login and login.get("status") == "ok":
                info = login.get("data") or {}
                self.store.set_self_user(info.get("user_id"), info.get("nickname") or info.get("name") or "")
        except Exception:
            pass
        try:
            friends = await self._request("get_friend_list", {})
            if friends and friends.get("status") == "ok":
                for f in (friends.get("data") or []):
                    uid = f.get("user_id")
                    if uid:
                        name = f.get("nickname", "") or f.get("remark", "") or str(uid)
                        self.store.ensure_chat(f"private_{uid}", name, "private")
                        self.store._nicknames[str(uid)] = name
        except Exception:
            pass
        try:
            groups = await self._request("get_group_list", {})
            if groups and groups.get("status") == "ok":
                for g in (groups.get("data") or []):
                    gid = g.get("group_id")
                    if gid:
                        name = g.get("group_name", "") or str(gid)
                        self.store.ensure_chat(f"group_{gid}", name, "group")
                        asyncio.create_task(self._fetch_group_members(gid))
        except Exception:
            pass

    async def _fetch_group_members(self, group_id):
        try:
            resp = await self._request("get_group_member_list", {"group_id": group_id})
            if resp and resp.get("status") == "ok":
                members = {}
                details = []
                for m in (resp.get("data") or []):
                    uid = m.get("user_id")
                    if uid:
                        uid = str(uid)
                        detail = simplify_group_member(m, fallback_name=self.store._nicknames.get(uid))
                        nick = detail["display_name"]
                        members[uid] = nick
                        details.append(detail)
                        if nick != uid:
                            self.store._nicknames[uid] = nick
                self.store._group_members[f"group_{group_id}"] = members
                self.store._group_member_details[f"group_{group_id}"] = details
        except Exception:
            pass

    async def _request(self, action, params, timeout=10):
        if not self.ws:
            return None
        echo = f"{action}-{uuid.uuid4().hex[:10]}"
        fut = asyncio.get_event_loop().create_future()
        self._pending[echo] = fut
        await self.ws.send_json({"action": action, "params": params, "echo": echo})
        try:
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            self._pending.pop(echo, None)
            return None

    async def _handle(self, data):
        echo = data.get("echo")
        if echo and echo in self._pending:
            fut = self._pending.pop(echo)
            if not fut.done():
                fut.set_result(data)
            return
        post_type = data.get("post_type")
        if post_type == "message":
            await self._resolve_forward_segments(data)
            simplified = self.store.add(data)
            if simplified:
                await self._broadcast({"type": "new_message", "data": simplified})
        elif post_type == "notice" and data.get("notice_type") == "group_msg_emoji_like":
            message_id = data.get("message_id")
            if message_id is not None:
                chat_id = f"group_{data['group_id']}" if data.get("group_id") else None
                notice_user = extract_notice_user(data, chat_id=chat_id, store=self.store)
                reactions = normalize_emoji_likes(data.get("likes") or data.get("like") or [], message_id=message_id)
                applied = self.store.apply_reactions(message_id, reactions, chat_id=chat_id, notice_user=notice_user)
                if applied:
                    reactions = applied["reactions"]
                payload = {
                    "message_id": str(message_id),
                    "reactions": reactions,
                }
                if applied:
                    payload["chat_id"] = applied["chat_id"]
                await self._broadcast({
                    "type": "emoji_like",
                    "data": payload,
                })

    async def _resolve_forward_segments(self, msg):
        segments = msg.get("message", [])
        if not isinstance(segments, list):
            return
        for seg in segments:
            if not isinstance(seg, dict) or seg.get("type") != "forward":
                continue
            data = seg.setdefault("data", {})
            if not isinstance(data, dict):
                continue
            if data.get("content") or data.get("nodes") or data.get("messages"):
                continue
            forward_id = data.get("id") or data.get("message_id") or data.get("resid")
            if not forward_id:
                data["error"] = "missing forward id"
                continue
            resp = await self._request("get_forward_msg", {"id": forward_id}, timeout=10)
            if not resp or resp.get("status") != "ok":
                data["error"] = (resp or {}).get("wording") or (resp or {}).get("message") or "forward unavailable"
                continue
            content = self._extract_forward_response_payload(resp.get("data"))
            if content:
                data["content"] = content
            else:
                data["error"] = "empty forward content"

    @staticmethod
    def _extract_forward_response_payload(payload):
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            for key in ("messages", "message", "content", "nodes"):
                value = payload.get(key)
                if value:
                    return value
            return payload
        return payload

    async def send_message(self, chat_id, text, reply_to=None):
        if not self.ws:
            raise RuntimeError("not connected to NapCat")
        message = self._parse_message(text, reply_to=reply_to)
        parsed = parse_chat_id(chat_id)
        if not parsed:
            raise ValueError(f"unknown chat_id: {chat_id}")
        if parsed["type"] == "group":
            return await self._request("send_group_msg", {"group_id": parsed["group_id"], "message": message})
        if parsed["type"] == "private":
            return await self._request("send_private_msg", {"user_id": parsed["private_id"], "message": message})
        return await self._request("send_private_msg", {
            "user_id": parsed["user_id"],
            "group_id": parsed["group_id"],
            "message": message,
        })

    async def upload_file(self, chat_id, path, name):
        if not self.ws:
            raise RuntimeError("not connected to NapCat")
        parsed = parse_chat_id(chat_id)
        if not parsed:
            raise ValueError(f"unknown chat_id: {chat_id}")
        if parsed["type"] == "group":
            return await self._request("upload_group_file", {
                "group_id": parsed["group_id"],
                "file": path,
                "name": name,
            }, timeout=60)
        if parsed["type"] == "private":
            return await self._request("upload_private_file", {
                "user_id": parsed["private_id"],
                "file": path,
                "name": name,
            }, timeout=60)
        raise ValueError("file sending is not supported for temporary chats")

    async def resolve_file_urls(self, file_id="", file_path="", busid="", url="", chat_id=""):
        urls = []
        if file_url_allowed(url):
            urls.append(url)
        candidates = []
        if file_id:
            candidates.append({"file_id": file_id})
        if file_path:
            candidates.append({"file": file_path})
        if file_id and busid:
            candidates.append({"file_id": file_id, "busid": busid})
        parsed = parse_chat_id(chat_id)
        for params in candidates:
            if parsed and parsed["type"] == "group":
                params = {**params, "group_id": parsed["group_id"]}
            actions = ["get_file"]
            if parsed and parsed["type"] == "group":
                actions.append("get_group_file_url")
            for action in actions:
                resp = await self._request(action, params, timeout=10)
                data = resp.get("data") if resp and resp.get("status") == "ok" else {}
                for candidate in extract_file_urls(data):
                    if file_url_allowed(candidate):
                        urls.append(candidate)
        return list(dict.fromkeys(urls))

    async def set_msg_emoji_like(self, message_id, emoji_id, enabled=True):
        return await self._request("set_msg_emoji_like", {
            "message_id": int(message_id),
            "emoji_id": str(emoji_id),
            "set": bool(enabled),
        })

    async def fetch_emoji_likes(self, message_id, chat_id="", emoji_ids=None):
        reactions = []
        fetch_ids = tuple(dict.fromkeys(str(eid) for eid in (emoji_ids or REACTION_FETCH_EMOJI_IDS) if str(eid).isdigit()))
        for emoji_id in fetch_ids:
            params = {
                "message_id": int(message_id),
                "emojiId": str(emoji_id),
                "emojiType": 1,
                "count": 20,
            }
            if chat_id.startswith("group_"):
                params["group_id"] = int(chat_id.split("_", 1)[1])
            resp = await self._request("fetch_emoji_like", params, timeout=5)
            if not resp or resp.get("status") != "ok":
                continue
            reaction = normalize_emoji_like_response(emoji_id, resp.get("data"))
            if reaction and reaction.get("count", 0) > 0:
                reactions.append(reaction)
        return reactions

    @staticmethod
    def _parse_message(text, reply_to=None):
        import re
        prefix = []
        if reply_to:
            prefix.append({"type": "reply", "data": {"id": str(reply_to)}})

        token_re = re.compile(r"@\[(\d+)\]|\[face:(\d+)\]")
        if not token_re.search(text):
            return prefix + [{"type": "text", "data": {"text": text}}] if prefix else text

        result = list(prefix)
        pos = 0
        for match in token_re.finditer(text):
            if match.start() > pos:
                result.append({"type": "text", "data": {"text": text[pos:match.start()]}})
            if match.group(1) is not None:
                result.append({"type": "at", "data": {"qq": match.group(1)}})
            else:
                result.append({"type": "face", "data": {"id": match.group(2)}})
            pos = match.end()
        if pos < len(text):
            result.append({"type": "text", "data": {"text": text[pos:]}})
        return result

    async def _broadcast(self, obj):
        dead = []
        for ws in self._subscribers:
            try:
                await ws.send_json(obj)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._subscribers.remove(ws)

    def subscribe(self, ws):
        self._subscribers.append(ws)

    def unsubscribe(self, ws):
        if ws in self._subscribers:
            self._subscribers.remove(ws)


def check_auth(request):
    cfg = request.app["config"]
    auth_token = cfg.get("web_token", "")
    if not auth_token:
        return True
    req_token = request.query.get("token") or request.cookies.get("token") or ""
    return hmac.compare_digest(req_token, auth_token)


async def read_json_body(request):
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"error":"invalid JSON"}', content_type="application/json")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error":"JSON body must be an object"}', content_type="application/json")
    return body


async def handle_login(request):
    cfg = request.app["config"]
    auth_token = cfg.get("web_token", "")
    if not auth_token:
        return web.json_response({"ok": True})
    body = await read_json_body(request)
    token = body.get("token", "")
    if not isinstance(token, str):
        return web.json_response({"ok": False, "error": "token must be a string"}, status=400)
    if hmac.compare_digest(token, auth_token):
        resp = web.json_response({"ok": True})
        resp.set_cookie("token", auth_token, max_age=86400 * 30, httponly=True)
        return resp
    return web.json_response({"ok": False, "error": "invalid token"}, status=401)


async def handle_chats(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    return web.json_response({"chats": request.app["store"].get_chats()})


async def handle_messages(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    store = request.app["store"]
    chat_id = request.query.get("chat_id", "")
    try:
        limit = max(1, min(int(request.query.get("limit", "50")), 200))
    except (TypeError, ValueError):
        return web.json_response({"error": "limit must be an integer"}, status=400)
    before_raw = request.query.get("before")
    try:
        before = float(before_raw) if before_raw else None
    except (TypeError, ValueError):
        return web.json_response({"error": "before must be a timestamp"}, status=400)
    return web.json_response({"messages": store.get_messages(chat_id, limit=limit, before=before)})


async def handle_temp_chat(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    group_id = str(body.get("group_id", "")).strip()
    user_id = str(body.get("user_id", "")).strip()
    if not group_id.isdigit() or not user_id.isdigit():
        return web.json_response({"ok": False, "error": "group_id and user_id are required"}, status=400)
    name = str(body.get("name", "")).strip()
    group_name = str(body.get("group_name", "")).strip()
    if not name:
        sender_name = str(body.get("sender_name", "")).strip() or user_id
        name = f"{group_name} / {sender_name}" if group_name else f"群临时会话: {sender_name}"
    chat_id = f"temp_{group_id}_{user_id}"
    store = request.app["store"]
    store.ensure_chat(chat_id, name, "temp", group_id=int(group_id), user_id=int(user_id), group_name=group_name)
    return web.json_response({"ok": True, "chat_id": chat_id, "name": name})


async def handle_send(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = body.get("chat_id")
    text = body.get("text")
    reply_to = body.get("reply_to")
    if not isinstance(chat_id, str) or not chat_id:
        return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
    if not isinstance(text, str):
        return web.json_response({"ok": False, "error": "text is required"}, status=400)
    parsed_chat = parse_chat_id(chat_id)
    if not parsed_chat:
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    if reply_to is not None:
        reply_to = str(reply_to).strip()
        if not reply_to.isdigit():
            return web.json_response({"ok": False, "error": "invalid reply_to"}, status=400)
    napcat = request.app["napcat"]
    store = request.app["store"]
    try:
        result = await napcat.send_message(chat_id, text, reply_to=reply_to)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "send failed")) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)
        now = int(time.time())
        message_id = extract_message_id(result)
        simplified = {
            "message_id": message_id,
            "time": now,
            "sender_id": "self",
            "sender_name": "You",
            "sender_avatar_url": avatar_url_for("user", store._self_user.get("user_id")),
            "content": f"[reply:{reply_to}]{text}" if reply_to else text,
            "mentions": {},
            "images": [],
            "forwards": [],
            "files": [],
            "reactions": [],
            "chat_id": chat_id,
            "type": parsed_chat["type"],
            "group_id": parsed_chat.get("group_id"),
            "user_id": parsed_chat.get("user_id") or parsed_chat.get("private_id"),
            "chat_name": "",
            "self": True,
        }
        store.append_simplified(chat_id, simplified)
        if chat_id in store._chat_meta:
            store._chat_meta[chat_id]["last_time"] = now
            store._chat_meta[chat_id]["last_text"] = text[:50]
        else:
            store.ensure_chat(chat_id, chat_id, parsed_chat["type"])
            store._chat_meta[chat_id]["last_time"] = now
            store._chat_meta[chat_id]["last_text"] = text[:50]
        store._dirty.add(chat_id)
        await napcat._broadcast({"type": "new_message", "data": simplified})
        return web.json_response({"ok": True, "data": result})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


async def handle_send_file(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = ""
    filename = "file"
    temp_path = None
    size = 0
    too_large = False
    try:
        reader = await request.multipart()
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "chat_id":
                chat_id = (await part.text()).strip()
            elif part.name == "file":
                filename = safe_download_name(part.filename or "file")
                fd, temp_path = tempfile.mkstemp(prefix="webqq-upload-")
                with os.fdopen(fd, "wb") as f:
                    while True:
                        chunk = await part.read_chunk(size=1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > MAX_FILE_UPLOAD:
                            too_large = True
                        else:
                            f.write(chunk)
            else:
                await part.release()
        parsed_chat = parse_chat_id(chat_id)
        if not chat_id:
            return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
        if not parsed_chat:
            return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
        if parsed_chat["type"] not in ("group", "private"):
            return web.json_response({"ok": False, "error": "file sending is not supported for temporary chats"}, status=400)
        if not temp_path:
            return web.json_response({"ok": False, "error": "file is required"}, status=400)
        if too_large:
            return web.json_response({"ok": False, "error": "file is larger than 100 MB"}, status=413)
        if size <= 0:
            return web.json_response({"ok": False, "error": "file is empty"}, status=400)
        napcat = request.app["napcat"]
        result = await napcat.upload_file(chat_id, temp_path, filename)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "file upload failed")) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)

        now = int(time.time())
        message_id = extract_message_id(result)
        simplified = {
            "message_id": message_id,
            "time": now,
            "sender_id": "self",
            "sender_name": "You",
            "sender_avatar_url": avatar_url_for("user", store._self_user.get("user_id")),
            "content": "[file]",
            "mentions": {},
            "images": [],
            "forwards": [],
            "files": [{"name": filename, "size": size}],
            "reactions": [],
            "chat_id": chat_id,
            "type": parsed_chat["type"],
            "group_id": parsed_chat.get("group_id"),
            "user_id": parsed_chat.get("user_id") or parsed_chat.get("private_id"),
            "chat_name": "",
            "self": True,
        }
        store = request.app["store"]
        store.append_simplified(chat_id, simplified)
        if chat_id not in store._chat_meta:
            store.ensure_chat(chat_id, chat_id, parsed_chat["type"])
        store._chat_meta[chat_id]["last_time"] = now
        store._chat_meta[chat_id]["last_text"] = "[file]"
        store._dirty.add(chat_id)
        await napcat._broadcast({"type": "new_message", "data": simplified})
        return web.json_response({"ok": True, "data": result})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def handle_message_emoji_like(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    message_id = str(body.get("message_id", "")).strip()
    emoji_id = str(body.get("emoji_id", "")).strip()
    chat_id = str(body.get("chat_id", "")).strip()
    if not is_int_string(message_id):
        return web.json_response({"ok": False, "error": "message_id is required"}, status=400)
    if not emoji_id.isdigit():
        return web.json_response({"ok": False, "error": "emoji_id is required"}, status=400)
    result = await request.app["napcat"].set_msg_emoji_like(message_id, emoji_id)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "reaction failed")) if result else "not connected"
        return web.json_response({"ok": False, "error": err}, status=500)
    data = result.get("data")
    if isinstance(data, dict) and data.get("result") is False:
        err = data.get("errMsg") or data.get("message") or result.get("wording") or "reaction was not accepted by QQ"
        return web.json_response({"ok": False, "error": err}, status=500)
    request.app["store"].remember_local_reaction(message_id, emoji_id, chat_id=chat_id)
    applied = request.app["store"].add_local_reaction(message_id, emoji_id, chat_id=chat_id)
    payload = {
        "message_id": message_id,
        "reactions": applied["reactions"] if applied else [{
            "emoji_id": emoji_id,
            "count": 1,
            "users": [dict(request.app["store"]._self_user)],
        }],
    }
    if applied:
        payload["chat_id"] = applied["chat_id"]
        await request.app["napcat"]._broadcast({"type": "emoji_like", "data": payload})
    return web.json_response({"ok": True, **payload})


async def handle_message_emoji_likes(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    message_id = str(request.query.get("message_id", "")).strip()
    chat_id = request.query.get("chat_id", "")
    emoji_id = str(request.query.get("emoji_id", "")).strip()
    if not is_int_string(message_id):
        return web.json_response({"error": "message_id is required"}, status=400)
    if emoji_id and not emoji_id.isdigit():
        return web.json_response({"error": "emoji_id must be numeric"}, status=400)
    fetched_emoji_ids = [emoji_id] if emoji_id else list(REACTION_FETCH_EMOJI_IDS)
    reactions = await request.app["napcat"].fetch_emoji_likes(message_id, chat_id=chat_id, emoji_ids=fetched_emoji_ids)
    return web.json_response({"message_id": message_id, "reactions": reactions, "fetched_emoji_ids": fetched_emoji_ids})


def extract_message_id(result):
    if not isinstance(result, dict):
        return None
    if result.get("message_id") is not None:
        return result.get("message_id")
    data = result.get("data")
    if isinstance(data, dict):
        return data.get("message_id")
    return None


def is_int_string(value):
    return bool(value) and value.lstrip("-").isdigit()


def reaction_count(reaction):
    try:
        return int(reaction.get("count", 0))
    except (TypeError, ValueError, AttributeError):
        return 0


def extract_notice_user(data, chat_id=None, store=None):
    uid = (
        data.get("user_id")
        or data.get("operator_id")
        or data.get("sender_id")
        or data.get("uin")
        or data.get("uid")
    )
    if uid is None:
        sender = data.get("sender")
        if isinstance(sender, dict):
            uid = sender.get("user_id") or sender.get("uin") or sender.get("uid")
    if uid is None:
        return None
    uid = str(uid)
    name = ""
    sender = data.get("sender")
    if isinstance(sender, dict):
        name = sender.get("card") or sender.get("nickname") or sender.get("name") or ""
    name = (
        name
        or data.get("card")
        or data.get("nickname")
        or data.get("name")
        or ((store._group_members.get(chat_id, {}) if store and chat_id else {}).get(uid))
        or ((store._nicknames.get(uid) if store else None))
        or uid
    )
    return {"user_id": uid, "name": name}


def normalize_emoji_like_response(emoji_id, data):
    if data is None:
        return None
    if isinstance(data, list):
        users = normalize_emoji_like_users(data)
        return {"emoji_id": str(emoji_id), "count": len(users), "users": users}
    if not isinstance(data, dict):
        return None
    users = normalize_emoji_like_users(
        data.get("userList")
        or data.get("emojiLikesList")
        or data.get("users")
        or data.get("likes")
        or data.get("items")
        or data.get("list")
        or []
    )
    count = data.get("count") or data.get("likeCount") or data.get("total_count") or data.get("total") or len(users)
    try:
        count = int(count)
    except (TypeError, ValueError):
        count = len(users)
    return {"emoji_id": str(data.get("emoji_id") or data.get("emojiId") or emoji_id), "count": count, "users": users}


def normalize_emoji_likes(payload, message_id=None):
    if isinstance(payload, dict):
        payload = payload.get("likes") or payload.get("items") or payload.get("list") or [payload]
    if not isinstance(payload, list):
        return []
    reactions = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        item_message_id = item.get("message_id") or item.get("msg_id") or item.get("msgId")
        if message_id is not None and item_message_id is not None and str(item_message_id) != str(message_id):
            continue
        emoji_id = item.get("emoji_id") or item.get("emojiId") or item.get("id")
        if emoji_id is None:
            continue
        reaction = normalize_emoji_like_response(emoji_id, item)
        if reaction:
            reactions.append(reaction)
    return reactions


def normalize_emoji_like_users(users):
    if not isinstance(users, list):
        return []
    result = []
    for user in users:
        if isinstance(user, dict):
            uid = user.get("user_id") or user.get("uin") or user.get("uid") or user.get("tiny_id")
            name = user.get("nickname") or user.get("name") or user.get("nick") or ""
            result.append({k: v for k, v in {"user_id": uid, "name": name}.items() if v})
        elif user:
            result.append({"user_id": user})
    return result


def first_text(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def first_positive_int(*values):
    for value in values:
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return None


def simplify_group_member(member, fallback_name=""):
    uid = first_text(member.get("user_id"), member.get("uin"), member.get("uid"))
    card = first_text(member.get("card"), member.get("card_name"))
    nickname = first_text(member.get("nickname"), member.get("nick"))
    remark = first_text(member.get("remark"), member.get("remarks"))
    name = first_text(member.get("name"), member.get("user_name"), member.get("username"))
    nick = first_text(member.get("nick"), member.get("nickname"))
    qid = first_text(member.get("qid"), member.get("q_id"))
    display = first_text(card, remark, name, nickname, nick, fallback_name, uid)
    return {
        "user_id": uid,
        "uid": uid,
        "display_name": display,
        "card": card,
        "remark": remark,
        "name": name,
        "nick": nick,
        "nickname": nickname,
        "qid": qid,
    }


async def handle_status(request):
    napcat = request.app["napcat"]
    return web.json_response({
        "napcat_connected": napcat.ws is not None,
        "chats_count": len(request.app["store"]._data),
    })


async def handle_nicknames(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    store = request.app["store"]
    chat_id = request.query.get("chat_id", "")
    if chat_id.startswith("group_"):
        group_id = chat_id.split("_", 1)[1]
        if chat_id not in store._group_members and group_id.isdigit():
            await request.app["napcat"]._fetch_group_members(int(group_id))
        return web.json_response(store._group_members.get(chat_id, {}))
    return web.json_response(store._nicknames)


async def handle_group_members(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    store = request.app["store"]
    chat_id = request.query.get("chat_id", "")
    if not chat_id.startswith("group_"):
        return web.json_response({"members": []})
    group_id = chat_id.split("_", 1)[1]
    if group_id.isdigit():
        await request.app["napcat"]._fetch_group_members(int(group_id))
    return web.json_response({"members": store._group_member_details.get(chat_id, [])})


def image_url_allowed(url):
    parsed = urlparse(url)
    host = parsed.hostname or ""
    allowed_hosts = (
        "multimedia.nt.qq.com.cn",
        "gchat.qpic.cn",
        "c2cpicdw.qpic.cn",
        "thirdqq.qlogo.cn",
        "gxh.vip.qq.com",
    )
    return parsed.scheme in ("http", "https") and host in allowed_hosts


def file_url_allowed(url):
    parsed = urlparse(url)
    return parsed.scheme in ("http", "https") and bool(parsed.hostname)


def avatar_cache_paths(avatar_type, avatar_id):
    base = AVATAR_DIR / avatar_type
    return base / f"{avatar_id}.img", base / f"{avatar_id}.json"


def avatar_source_url(avatar_type, avatar_id):
    if avatar_type == "user":
        return f"https://q1.qlogo.cn/g?b=qq&nk={avatar_id}&s=100"
    if avatar_type == "group":
        return f"https://p.qlogo.cn/gh/{avatar_id}/{avatar_id}/100"
    return ""


def avatar_cache_fresh(meta):
    try:
        fetched_at = float(meta.get("fetched_at", 0))
    except (TypeError, ValueError):
        return False
    return time.time() - fetched_at < AVATAR_CACHE_TTL


def read_avatar_meta(meta_path):
    try:
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        return meta if isinstance(meta, dict) else {}
    except Exception:
        return {}


def serve_cached_avatar(image_path, meta_path, stale=False):
    meta = read_avatar_meta(meta_path)
    content_type = meta.get("content_type") or mimetypes.guess_type(str(image_path))[0] or "image/jpeg"
    headers = {"Cache-Control": "private, max-age=86400", "Content-Type": content_type}
    if stale:
        headers["Warning"] = '110 - "avatar cache is stale"'
    return web.FileResponse(image_path, headers=headers)


def avatar_placeholder_svg(label):
    text = (str(label or "?").strip() or "?")[:2]
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100">'
        '<rect width="100" height="100" rx="50" fill="#0f3460"/>'
        f'<text x="50" y="57" text-anchor="middle" font-family="Arial,sans-serif" font-size="34" fill="#e0e0e0">{text}</text>'
        "</svg>"
    )


async def fetch_and_cache_avatar(avatar_type, avatar_id, image_path, meta_path):
    url = avatar_source_url(avatar_type, avatar_id)
    if not url:
        return False
    image_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"}) as resp:
            if resp.status != 200:
                return False
            content_type = resp.headers.get("Content-Type", "image/jpeg").split(";", 1)[0]
            if not content_type.startswith("image/"):
                return False
            body = await resp.read()
            if not body:
                return False
    fd, tmp_image = tempfile.mkstemp(prefix=f"{avatar_id}.", dir=str(image_path.parent))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(body)
        os.replace(tmp_image, image_path)
        tmp_meta = meta_path.with_suffix(".json.tmp")
        with open(tmp_meta, "w", encoding="utf-8") as f:
            json.dump({"content_type": content_type, "fetched_at": time.time(), "source_url": url}, f, separators=(",", ":"))
        os.replace(tmp_meta, meta_path)
        return True
    finally:
        try:
            if os.path.exists(tmp_image):
                os.unlink(tmp_image)
        except OSError:
            pass


async def handle_avatar(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    avatar_type = request.query.get("type", "")
    avatar_id = request.query.get("id", "")
    if avatar_type not in ("user", "group") or not avatar_id.isdigit():
        return web.json_response({"error": "type=user|group and numeric id are required"}, status=400)
    image_path, meta_path = avatar_cache_paths(avatar_type, avatar_id)
    if image_path.exists():
        meta = read_avatar_meta(meta_path)
        if avatar_cache_fresh(meta):
            return serve_cached_avatar(image_path, meta_path)
    try:
        if await fetch_and_cache_avatar(avatar_type, avatar_id, image_path, meta_path):
            return serve_cached_avatar(image_path, meta_path)
    except Exception:
        pass
    if image_path.exists():
        return serve_cached_avatar(image_path, meta_path, stale=True)
    return web.Response(
        text=avatar_placeholder_svg(avatar_id[-2:]),
        content_type="image/svg+xml",
        headers={"Cache-Control": "private, max-age=300"},
    )


def safe_download_name(name):
    name = os.path.basename(str(name or "file")).replace("\x00", "").strip()
    return name or "file"


def content_disposition(filename):
    safe_ascii = safe_download_name(filename).encode("ascii", "ignore").decode("ascii") or "file"
    safe_ascii = safe_ascii.replace("\\", "_").replace('"', "_")
    encoded = quote(safe_download_name(filename))
    return f'attachment; filename="{safe_ascii}"; filename*=UTF-8\'\'{encoded}'


def extract_file_urls(payload):
    urls = []
    if isinstance(payload, dict):
        for key in ("url", "file", "download_url", "downloadUrl"):
            value = payload.get(key)
            if isinstance(value, str):
                urls.append(value)
        for key in ("data", "file_info", "fileInfo"):
            urls.extend(extract_file_urls(payload.get(key)))
    elif isinstance(payload, list):
        for item in payload:
            urls.extend(extract_file_urls(item))
    return urls


async def fetch_first_file(urls, filename):
    async with aiohttp.ClientSession() as session:
        last_status = 502
        for file_url in urls:
            try:
                async with session.get(file_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    last_status = resp.status
                    if resp.status != 200:
                        continue
                    content_type = resp.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0]
                    headers = {
                        "Cache-Control": "private, max-age=300",
                        "Content-Disposition": content_disposition(filename),
                    }
                    length = resp.headers.get("Content-Length")
                    if length:
                        headers["Content-Length"] = length
                    return web.Response(body=await resp.read(), content_type=content_type, headers=headers)
            except Exception:
                continue
        return web.json_response({"error": "file fetch failed"}, status=last_status)


async def handle_file_proxy(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    filename = safe_download_name(request.query.get("name", "file"))
    urls = await request.app["napcat"].resolve_file_urls(
        file_id=request.query.get("id", ""),
        file_path=request.query.get("file", ""),
        busid=request.query.get("busid", ""),
        url=request.query.get("url", ""),
        chat_id=request.query.get("chat_id", ""),
    )
    if not urls:
        return web.json_response({"error": "file url unavailable"}, status=400)
    return await fetch_first_file(urls, filename)


async def resolve_image_urls(request, url, file, refresh=False):
    urls = []
    if image_url_allowed(url):
        urls.append(url)
    if refresh and file:
        image_info = await request.app["napcat"]._request("get_image", {"file": file}, timeout=10)
        data = image_info.get("data") if image_info and image_info.get("status") == "ok" else {}
        for candidate in (data.get("url"), data.get("file")):
            if image_url_allowed(candidate or ""):
                urls.append(candidate)
    return list(dict.fromkeys(urls))


async def fetch_first_image(urls):
    async with aiohttp.ClientSession() as session:
        last_status = 502
        for image_url in urls:
            try:
                async with session.get(image_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    last_status = resp.status
                    if resp.status != 200:
                        continue
                    content_type = resp.headers.get("Content-Type", "application/octet-stream").split(";", 1)[0]
                    if not (content_type.startswith("image/") or content_type == "application/octet-stream"):
                        continue
                    return web.Response(
                        body=await resp.read(),
                        content_type=content_type,
                        headers={"Cache-Control": "private, max-age=300"},
                    )
            except Exception:
                continue
        return web.json_response({"error": "image fetch failed"}, status=last_status)


async def handle_image_proxy(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    url = request.query.get("url", "")
    file = request.query.get("file", "")
    refresh = request.query.get("refresh") == "1"
    urls = await resolve_image_urls(request, url, file, refresh=refresh)
    if not urls:
        return web.json_response({"error": "invalid image url"}, status=400)
    return await fetch_first_image(urls)


async def handle_image_full(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    url = request.query.get("url", "")
    file = request.query.get("file", "")
    urls = await resolve_image_urls(request, url, file, refresh=True)
    if not urls:
        return web.json_response({"error": "invalid image url"}, status=400)
    return await fetch_first_image(urls)


async def handle_ws_browser(request):
    if not check_auth(request):
        await request.writer.drain()
        raise web.HTTPUnauthorized()
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    napcat = request.app["napcat"]
    napcat.subscribe(ws)
    try:
        async for msg in ws:
            pass
    finally:
        napcat.unsubscribe(ws)
    return ws


async def flush_loop(store, interval):
    while True:
        await asyncio.sleep(interval)
        store.flush()


async def flush_on_shutdown(app):
    app["store"].flush()


async def main():
    config = load_config()
    store = MessageStore(maxlen=MAX_MESSAGES)
    store.load_all()
    napcat = NapCatConnection(config["ws_url"], config.get("napcat_token", ""), store)

    app = web.Application(client_max_size=MAX_FILE_UPLOAD + 1024 * 1024)
    app["config"] = config
    app["store"] = store
    app["napcat"] = napcat
    app.on_shutdown.append(flush_on_shutdown)

    app.router.add_post("/api/login", handle_login)
    app.router.add_get("/api/chats", handle_chats)
    app.router.add_get("/api/messages", handle_messages)
    app.router.add_post("/api/temp-chat", handle_temp_chat)
    app.router.add_post("/api/send", handle_send)
    app.router.add_post("/api/send-file", handle_send_file)
    app.router.add_post("/api/message/emoji-like", handle_message_emoji_like)
    app.router.add_get("/api/message/emoji-likes", handle_message_emoji_likes)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/nicknames", handle_nicknames)
    app.router.add_get("/api/group-members", handle_group_members)
    app.router.add_get("/api/avatar", handle_avatar)
    app.router.add_get("/api/image", handle_image_proxy)
    app.router.add_get("/api/image/full", handle_image_full)
    app.router.add_get("/api/file", handle_file_proxy)
    app.router.add_get("/ws", handle_ws_browser)
    app.router.add_get("/", lambda r: web.FileResponse(STATIC_DIR / "index.html"))
    app.router.add_static("/", path=str(STATIC_DIR), name="static")

    asyncio.create_task(napcat.start())
    asyncio.create_task(flush_loop(store, config.get("flush_interval", 15)))

    port = config.get("web_port", 8080)
    print(f"WebQQ running at http://localhost:{port}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
