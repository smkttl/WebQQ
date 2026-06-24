#!/usr/bin/env python3
import asyncio
import json
import os
import time
import uuid
import hmac
import tempfile
import mimetypes
import base64
import importlib.util
import traceback
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from collections import defaultdict, deque
from pathlib import Path

from aiohttp import web
import aiohttp

CONFIG_PATH = Path(__file__).parent / "config.json"
STATIC_DIR = Path(__file__).parent / "static"
DATA_DIR = Path(__file__).parent / "data"
AVATAR_DIR = DATA_DIR / "avatars"
PLUGIN_DIR = Path(__file__).parent / "plugins"
MAX_MESSAGES = 1000
MAX_FILE_UPLOAD = 100 * 1024 * 1024
AVATAR_CACHE_TTL = 7 * 24 * 60 * 60
REVOKE_WINDOW_SECONDS = 2 * 60
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
    "fail2ban_max_failures": 5,
    "fail2ban_window_seconds": 300,
    "fail2ban_ban_seconds": 1800,
    "plugins": {"enabled": {}},
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


def canonical_chat_id(chat_id):
    parsed = parse_chat_id(chat_id)
    if not parsed:
        return chat_id
    if parsed["type"] == "temp":
        return f"private_{parsed['user_id']}"
    return chat_id


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


def save_config(cfg):
    tmp = CONFIG_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(tmp, CONFIG_PATH)


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
        self._known_private_users = set()
        self._private_temp_contexts = {}  # uid -> {group_id, group_name}
        self._message_chat_index = {}  # message_id -> chat_id
        self._pending_local_reactions = {}  # (message_id, emoji_id) -> user
        self._pending_local_messages = defaultdict(list)  # chat_id -> optimistic messages awaiting message_sent
        self._self_user = {"user_id": "self", "name": "You"}
        data_dir.mkdir(exist_ok=True)

    def _chat_path(self, chat_id):
        return self._data_dir / f"{chat_id}.json"

    def load_all(self):
        for fp in self._data_dir.glob("*.json"):
            source_chat_id = fp.stem
            chat_id = canonical_chat_id(source_chat_id)
            try:
                with open(fp, encoding="utf-8") as f:
                    msgs = json.load(f)
                if isinstance(msgs, list):
                    for msg in msgs[-self.maxlen:]:
                        if isinstance(msg, dict):
                            normalized = self._normalize_loaded_message(source_chat_id, msg)
                            self._append_dedup(chat_id, normalized)
                    self._sort_and_trim_chat(chat_id)
                    self._reindex_chat(chat_id)
                    self._refresh_chat_meta(chat_id)
            except Exception:
                pass

    def _normalize_loaded_message(self, source_chat_id, msg):
        normalized = dict(msg)
        source_parsed = parse_chat_id(source_chat_id)
        original_chat_id = normalized.get("chat_id") or source_chat_id
        parsed = parse_chat_id(original_chat_id) or source_parsed
        if parsed and parsed["type"] == "temp":
            normalized.setdefault("temp_group_id", parsed["group_id"])
            if normalized.get("group_name"):
                normalized.setdefault("temp_group_name", normalized.get("group_name"))
            self.remember_temp_context(parsed["user_id"], parsed["group_id"], normalized.get("group_name") or normalized.get("temp_group_name") or "")
            normalized["chat_id"] = f"private_{parsed['user_id']}"
            normalized["type"] = "private"
            normalized["user_id"] = parsed["user_id"]
        elif parsed and parsed["type"] == "private":
            normalized["chat_id"] = f"private_{parsed['private_id']}"
            normalized["type"] = "private"
            normalized["user_id"] = normalized.get("user_id") or parsed["private_id"]
        else:
            normalized["chat_id"] = canonical_chat_id(original_chat_id)
        return normalized

    @staticmethod
    def _dedup_key(msg):
        message_id = msg.get("message_id")
        if message_id is not None:
            return ("id", str(message_id))
        return (
            "fallback",
            str(msg.get("time", "")),
            str(msg.get("sender_id", "")),
            str(msg.get("content", "")),
        )

    def _append_dedup(self, chat_id, msg):
        key = self._dedup_key(msg)
        for existing in self._data.get(chat_id, []):
            if self._dedup_key(existing) == key:
                return False
        self._data[chat_id].append(msg)
        return True

    def _sort_and_trim_chat(self, chat_id):
        items = sorted(self._data.get(chat_id, []), key=lambda item: (item.get("time", 0), str(item.get("message_id", ""))))
        self._data[chat_id] = deque(items[-self.maxlen:], maxlen=self.maxlen)

    def _refresh_chat_meta(self, chat_id):
        msgs = self._data.get(chat_id)
        if not msgs:
            return
        last = msgs[-1]
        self._chat_meta[chat_id] = {
            "chat_id": chat_id,
            "name": last.get("chat_name") or self._chat_meta.get(chat_id, {}).get("name") or chat_id,
            "type": last.get("type", ""),
            "last_time": last.get("time", 0),
            "last_text": (last.get("content", "") or "")[:50],
            "avatar_url": chat_avatar_url(chat_id, last.get("type", ""), last.get("user_id"), last.get("group_id")),
        }

    def remember_private_user(self, user_id):
        if user_id is not None:
            self._known_private_users.add(str(user_id))

    def remember_temp_context(self, user_id, group_id, group_name=""):
        if user_id is None or group_id is None:
            return
        uid = str(user_id)
        context = {"group_id": int(group_id)}
        if group_name:
            context["group_name"] = str(group_name)
        self._private_temp_contexts[uid] = context

    def private_send_context(self, user_id):
        uid = str(user_id)
        if uid in self._known_private_users:
            return {}
        return dict(self._private_temp_contexts.get(uid) or {})

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
            user_id = msg.get("target_id") if msg.get("post_type") == "message_sent" and msg.get("target_id") else msg.get("user_id")
            if msg.get("sub_type") == "group" and msg.get("group_id"):
                self.remember_temp_context(user_id, msg.get("group_id"), msg.get("group_name", ""))
            else:
                self.remember_private_user(user_id)
            return f"private_{user_id}"
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

    def add_history_messages(self, messages):
        added = []
        for msg in messages:
            key = self.chat_key(msg)
            if not key:
                continue
            simplified = self._simplify(msg)
            message_id = simplified.get("message_id")
            if message_id is not None and self.find_chat_by_message_id(message_id):
                continue
            self._data[key].appendleft(simplified)
            added.append(simplified)
            if key not in self._chat_meta:
                self.ensure_chat(key, simplified.get("chat_name") or key, simplified.get("type", ""))
            self._dirty.add(key)
            self._reindex_chat(key)
        return added

    def register_pending_local_message(self, chat_id, simplified):
        local_id = f"local-{uuid.uuid4().hex}"
        simplified["local_id"] = local_id
        simplified["pending"] = True
        self._pending_local_messages[chat_id].append(simplified)
        self.append_simplified(chat_id, simplified)
        return simplified

    def reconcile_self_message(self, simplified):
        chat_id = simplified.get("chat_id")
        if self._is_ambiguous_self_private_echo(simplified):
            chat_id = self._matching_pending_chat_id(simplified)
            if not chat_id:
                return {"message": simplified, "replaced": False, "ignored": True}
            self._copy_chat_identity(simplified, self._pending_local_messages[chat_id][0])
        if not chat_id:
            return {"message": simplified, "replaced": False}
        pending = self._pending_local_messages.get(chat_id, [])
        match = None
        for item in list(pending):
            if self._pending_matches(item, simplified):
                match = item
                break
        if not match:
            self.append_simplified(chat_id, simplified)
            self._update_chat_meta_from_message(chat_id, simplified)
            self._dirty.add(chat_id)
            return {"message": simplified, "replaced": False}
        pending.remove(match)
        local_id = match.get("local_id")
        merged = {**match, **simplified}
        merged.pop("pending", None)
        if local_id:
            merged["local_id"] = local_id
        for index, msg in enumerate(self._data.get(chat_id, [])):
            if msg is match or (local_id and msg.get("local_id") == local_id):
                self._data[chat_id][index] = merged
                break
        self._reindex_chat(chat_id)
        self._update_chat_meta_from_message(chat_id, merged)
        self._dirty.add(chat_id)
        return {"message": merged, "replaced": True, "local_id": local_id}

    def _is_ambiguous_self_private_echo(self, simplified):
        self_id = str(self._self_user.get("user_id") or "")
        chat_id = str(simplified.get("chat_id") or "")
        sender_id = str(simplified.get("sender_id") or "")
        user_id = str(simplified.get("user_id") or "")
        return (
            bool(self_id)
            and simplified.get("type") == "private"
            and chat_id == f"private_{self_id}"
            and sender_id == self_id
            and user_id == self_id
        )

    def _matching_pending_chat_id(self, simplified):
        for chat_id, pending in self._pending_local_messages.items():
            for item in list(pending):
                if self._pending_matches(item, simplified):
                    return chat_id
        return None

    @staticmethod
    def _copy_chat_identity(target, source):
        for key in ("chat_id", "type", "group_id", "user_id", "chat_name"):
            target[key] = source.get(key)

    def _pending_matches(self, pending, incoming):
        try:
            if abs(float(incoming.get("time", 0)) - float(pending.get("time", 0))) > 180:
                return False
        except (TypeError, ValueError):
            pass
        if pending.get("content") != incoming.get("content"):
            return False
        pending_files = pending.get("files") or []
        incoming_files = incoming.get("files") or []
        if pending_files or incoming_files:
            pending_names = [str(f.get("name") or f.get("file") or "") for f in pending_files if isinstance(f, dict)]
            incoming_names = [str(f.get("name") or f.get("file") or "") for f in incoming_files if isinstance(f, dict)]
            return bool(set(pending_names) & set(incoming_names)) or pending.get("content") == incoming.get("content")
        return True

    def _update_chat_meta_from_message(self, chat_id, simplified):
        chat_id = canonical_chat_id(chat_id)
        if chat_id not in self._chat_meta:
            self.ensure_chat(chat_id, simplified.get("chat_name") or chat_id, simplified.get("type", ""))
        current_name = self._chat_meta[chat_id].get("name", chat_id)
        is_known_private_temp = (
            chat_id.startswith("private_")
            and str(simplified.get("user_id") or "") in self._known_private_users
            and simplified.get("temp_group_id")
        )
        self._chat_meta[chat_id]["name"] = current_name if is_known_private_temp else (simplified.get("chat_name") or current_name)
        self._chat_meta[chat_id]["avatar_url"] = chat_avatar_url(chat_id, simplified.get("type", ""), simplified.get("user_id"), simplified.get("group_id"))
        self._chat_meta[chat_id]["last_time"] = simplified.get("time", 0)
        self._chat_meta[chat_id]["last_text"] = (simplified.get("content", "") or "")[:50]

    def add_system_message(self, chat_id, text, notice_type="", sub_type="", **extra):
        chat_id = canonical_chat_id(chat_id)
        parsed = parse_chat_id(chat_id)
        if not parsed or not text:
            return None
        now = int(time.time())
        simplified = {
            "message_id": f"system-{now}-{uuid.uuid4().hex[:8]}",
            "time": now,
            "sender_id": "system",
            "sender_name": "System",
            "sender_avatar_url": "",
            "content": text,
            "mentions": {},
            "images": [],
            "forwards": [],
            "files": [],
            "videos": [],
            "records": [],
            "extra_segments": [],
            "reactions": [],
            "chat_id": chat_id,
            "type": parsed["type"],
            "group_id": parsed.get("group_id"),
            "user_id": parsed.get("user_id") or parsed.get("private_id"),
            "chat_name": self._chat_meta.get(chat_id, {}).get("name", chat_id),
            "self": False,
            "system": True,
            "notice_type": notice_type,
            "sub_type": sub_type,
            **{k: v for k, v in extra.items() if v is not None},
        }
        self.append_simplified(chat_id, simplified)
        self._update_chat_meta_from_message(chat_id, simplified)
        self._dirty.add(chat_id)
        return simplified

    def mark_recalled(self, message_id, chat_id=None, operator_id=None, recalled_at=None):
        chat_id = canonical_chat_id(chat_id)
        key = str(message_id)
        indexed_chat_id = self._message_chat_index.get(key) or self.find_chat_by_message_id(key)
        known_chat_id = chat_id if chat_id in self._data else indexed_chat_id
        if chat_id and indexed_chat_id and chat_id != indexed_chat_id:
            known_chat_id = indexed_chat_id
        if not known_chat_id:
            return None
        for msg in self._data.get(known_chat_id, []):
            if str(msg.get("message_id")) == key:
                already_recalled = bool(msg.get("recalled"))
                msg["recalled"] = True
                msg["recalled_at"] = recalled_at or int(time.time())
                if operator_id is not None:
                    msg["recall_operator_id"] = operator_id
                self._dirty.add(known_chat_id)
                return {"chat_id": known_chat_id, "message": msg, "already_recalled": already_recalled}
        return None

    def oldest_message_id(self, chat_id, before=None):
        chat_id = canonical_chat_id(chat_id)
        candidates = list(self._data.get(chat_id, []))
        if before:
            candidates = [m for m in candidates if m.get("time", 0) < before]
        for msg in candidates:
            message_id = msg.get("message_id")
            if message_id is not None:
                return message_id
        return None

    def _reindex_chat(self, chat_id):
        for message_id, indexed_chat_id in list(self._message_chat_index.items()):
            if indexed_chat_id == chat_id:
                self._message_chat_index.pop(message_id, None)
        for msg in self._data.get(chat_id, []):
            message_id = msg.get("message_id")
            if message_id is not None:
                self._message_chat_index[str(message_id)] = chat_id

    def remember_local_reaction(self, message_id, emoji_id, chat_id=None):
        chat_id = canonical_chat_id(chat_id)
        key = (str(message_id), str(emoji_id))
        self._pending_local_reactions[key] = dict(self._self_user)
        if chat_id and chat_id in self._data:
            self._message_chat_index.setdefault(str(message_id), chat_id)

    def add_local_reaction(self, message_id, emoji_id, chat_id=None):
        chat_id = canonical_chat_id(chat_id)
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

    def find_message(self, message_id, chat_id=None):
        chat_id = canonical_chat_id(chat_id)
        key = str(message_id)
        chat_ids = [chat_id] if chat_id in self._data else []
        indexed = self._message_chat_index.get(key) or self.find_chat_by_message_id(key)
        if indexed and indexed not in chat_ids:
            chat_ids.append(indexed)
        for cid in chat_ids or list(self._data.keys()):
            for msg in self._data.get(cid, []):
                if str(msg.get("message_id")) == key:
                    return {"chat_id": cid, "message": msg}
        return None

    def _simplify(self, msg):
        sender = msg.get("sender") or {}
        chat_name = ""
        mt = msg.get("message_type", "")
        chat_type = mt
        temp_group_id = None
        temp_group_name = ""
        if mt == "group":
            chat_name = msg.get("group_name", "")
        if mt == "private":
            if msg.get("sub_type") == "group" and msg.get("group_id"):
                chat_type = "private"
                temp_group_id = msg.get("group_id")
                temp_group_name = msg.get("group_name", "")
                sender_name = sender.get("card") or sender.get("nickname") or str(msg.get("user_id", ""))
                group_name = msg.get("group_name", "")
                chat_name = f"{group_name} / {sender_name}" if group_name else f"群临时会话: {sender_name}"
            else:
                chat_name = sender.get("nickname", str(msg.get("user_id", "")))
        if sender.get("user_id"):
            nick = sender.get("card") or sender.get("nickname") or ""
            if nick:
                self._nicknames[str(sender["user_id"])] = nick
        content, mentions, images, forwards, files, videos, records, extra_segments = self._extract_text(msg)
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
            "videos": videos,
            "records": records,
            "extra_segments": extra_segments,
            "reactions": normalize_emoji_likes(msg.get("reactions") or msg.get("likes") or msg.get("like") or []),
            "chat_id": self.chat_key(msg),
            "type": chat_type,
            "group_id": msg.get("group_id"),
            "temp_group_id": temp_group_id,
            "temp_group_name": temp_group_name,
            "user_id": msg.get("user_id"),
            "chat_name": chat_name,
            "self": (
                msg.get("post_type") == "message_sent"
                or msg.get("message_sent_type") == "self"
                or
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
        videos = []
        records = []
        extra_segments = []
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
                elif t == "video":
                    video_item = self._simplify_media_segment(d, "video")
                    if video_item:
                        videos.append(video_item)
                    parts.append("[video]")
                elif t == "record":
                    record_item = self._simplify_media_segment(d, "record")
                    if record_item:
                        records.append(record_item)
                    parts.append("[voice]")
                elif t == "mface":
                    image = self._simplify_mface_segment(d)
                    if image:
                        images.append(image)
                    parts.append(d.get("summary") or "[mface]")
                elif t in ("onlinefile", "flashtransfer"):
                    file_item = self._simplify_file_segment(d)
                    if file_item:
                        file_item["kind"] = t
                        files.append(file_item)
                    parts.append(f"[{t}]")
                elif t in ("json", "markdown", "music", "xml", "poke", "dice", "rps", "miniapp", "contact", "location"):
                    extra = self._simplify_extra_segment(t, d)
                    extra_segments.append(extra)
                    parts.append(extra["label"])
                else:
                    extra_segments.append(self._simplify_extra_segment(t, d))
                    parts.append(f"[{t}]")
            return "".join(parts), mentions, images, forwards, files, videos, records, extra_segments
        if isinstance(segments, str):
            return segments, mentions, images, forwards, files, videos, records, extra_segments
        return str(msg.get("raw_message", "")), mentions, images, forwards, files, videos, records, extra_segments

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
            "kind": data.get("kind"),
        }
        return {k: v for k, v in file_item.items() if v is not None and v != ""}

    @staticmethod
    def _simplify_media_segment(data, kind):
        if not isinstance(data, dict):
            return None
        item = {
            "kind": kind,
            "name": data.get("name") or data.get("file_name") or data.get("filename") or data.get("file"),
            "file": data.get("file") or data.get("path"),
            "url": data.get("url"),
            "size": data.get("size") or data.get("file_size") or data.get("fileSize"),
            "thumb": data.get("thumb") or data.get("thumbnail"),
        }
        return {k: v for k, v in item.items() if v is not None and v != ""}

    @staticmethod
    def _simplify_mface_segment(data):
        if not isinstance(data, dict):
            return None
        emoji_id = data.get("emoji_id") or data.get("emojiId")
        package_id = data.get("emoji_package_id") or data.get("emojiPackageId")
        url = data.get("url")
        if not url and emoji_id:
            directory = str(emoji_id)[:2]
            url = f"https://gxh.vip.qq.com/club/item/parcel/item/{directory}/{emoji_id}/raw300.gif"
        return {
            "url": url,
            "file": data.get("file") or (f"mface-{emoji_id}.gif" if emoji_id else ""),
            "summary": data.get("summary") or data.get("name") or "[mface]",
            "emoji_id": emoji_id,
            "emoji_package_id": package_id,
        }

    @staticmethod
    def _simplify_extra_segment(segment_type, data):
        label_map = {
            "json": "[json card]",
            "markdown": "[markdown]",
            "music": "[music]",
            "xml": "[xml]",
            "poke": "[poke]",
            "dice": "[dice]",
            "rps": "[rps]",
            "miniapp": "[mini app]",
            "contact": "[contact]",
            "location": "[location]",
        }
        text = ""
        if isinstance(data, dict):
            text = first_text(data.get("summary"), data.get("title"), data.get("content"), data.get("text"), data.get("data"))
        return {
            "type": segment_type,
            "label": label_map.get(segment_type, f"[{segment_type}]"),
            "text": text[:500],
        }

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
        text, mentions, images, forwards, files, videos, records, extra_segments = self._extract_text(fake_msg)
        return {
            "sender_id": node.get("user_id") or node.get("uin") or sender.get("user_id") or "",
            "sender_name": node.get("nickname") or node.get("name") or sender.get("nickname") or sender.get("card") or "",
            "time": node.get("time") or node.get("timestamp") or 0,
            "content": text,
            "mentions": mentions,
            "images": images,
            "forwards": forwards,
            "files": files,
            "videos": videos,
            "records": records,
            "extra_segments": extra_segments,
        }

    def get_messages(self, chat_id, limit=50, before=None):
        chat_id = canonical_chat_id(chat_id)
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


class PluginContext:
    def __init__(self, manager, plugin_id, config):
        self.manager = manager
        self.plugin_id = plugin_id
        self.config = config

    def log(self, message):
        print(f"[plugin:{self.plugin_id}] {message}")

    async def send_message(self, chat_id, text, reply_to=None):
        sent = await send_text_and_register(
            self.manager.napcat,
            self.manager.store,
            chat_id,
            text,
            reply_to=reply_to,
            source=f"plugin:{self.plugin_id}",
            optimistic=True,
        )
        return sent["result"]

    async def upload_file(self, chat_id, path, name=None):
        return await self.manager.napcat.upload_file(chat_id, path, name or os.path.basename(path))

    async def set_msg_emoji_like(self, message_id, emoji_id, enabled=True):
        return await self.manager.napcat.set_msg_emoji_like(message_id, emoji_id, enabled=enabled)

    async def mark_chat_read(self, chat_id):
        return await self.manager.napcat.mark_chat_read(chat_id)

    async def fetch_history(self, chat_id, before_message_id=None, count=50):
        return await self.manager.napcat.fetch_history(chat_id, before_message_id=before_message_id, count=count)

    def get_messages(self, chat_id, limit=50, before=None):
        return self.manager.store.get_messages(chat_id, limit=limit, before=before)

    def get_chats(self):
        return self.manager.store.get_chats()

    def get_self_user(self):
        return dict(self.manager.store._self_user)

    async def napcat(self, action, params=None, timeout=10):
        return await self.manager.napcat._request(action, params or {}, timeout=timeout)


class PluginManager:
    def __init__(self, plugin_dir, config, store, napcat=None):
        self.plugin_dir = Path(plugin_dir)
        self.config = config
        self.store = store
        self.napcat = napcat
        self._plugins = {}
        self._enabled = self.config.setdefault("plugins", {}).setdefault("enabled", {})
        self.plugin_dir.mkdir(exist_ok=True)

    def set_napcat(self, napcat):
        self.napcat = napcat

    def scan(self):
        discovered = {}
        for manifest_path in sorted(self.plugin_dir.glob("*/plugin.json")):
            plugin_id = manifest_path.parent.name
            state = self._plugins.get(plugin_id, {})
            state.update({
                "id": plugin_id,
                "path": manifest_path.parent,
                "manifest_path": manifest_path,
                "manifest": {},
                "module": state.get("module"),
                "handler": state.get("handler"),
                "portal_handler": state.get("portal_handler"),
                "ctx": state.get("ctx"),
                "loaded": False,
                "error": "",
                "config_error": "",
            })
            try:
                with open(manifest_path, encoding="utf-8") as f:
                    manifest = json.load(f)
                if not isinstance(manifest, dict):
                    raise ValueError("plugin.json must be an object")
                manifest_id = str(manifest.get("id") or plugin_id)
                if manifest_id != plugin_id:
                    raise ValueError("plugin.json id must match folder name")
                state["manifest"] = manifest
            except Exception as e:
                state["error"] = str(e)
            discovered[plugin_id] = state
        self._plugins = discovered
        return self.list_plugins()

    def list_plugins(self):
        if not self._plugins:
            self.scan()
        return [self._public_state(state) for state in self._plugins.values()]

    def _public_state(self, state):
        manifest = state.get("manifest") or {}
        plugin_id = state.get("id", "")
        enabled = self.is_enabled(plugin_id)
        return {
            "id": plugin_id,
            "name": manifest.get("name") or plugin_id,
            "version": manifest.get("version", ""),
            "description": manifest.get("description", ""),
            "enabled": enabled,
            "loaded": bool(state.get("loaded")) and enabled,
            "portal_receiver": bool(enabled and state.get("loaded") and callable(state.get("portal_handler"))),
            "error": state.get("error", ""),
            "config_error": state.get("config_error", ""),
        }

    def is_enabled(self, plugin_id):
        if plugin_id in self._enabled:
            return bool(self._enabled[plugin_id])
        manifest = (self._plugins.get(plugin_id) or {}).get("manifest") or {}
        return bool(manifest.get("enabled_by_default", False))

    def set_enabled(self, plugin_id, enabled):
        self._require_plugin(plugin_id)
        self._enabled[plugin_id] = bool(enabled)
        save_config(self.config)
        if enabled:
            self.load_plugin(plugin_id)
        else:
            self.unload_plugin(plugin_id)
        return self._public_state(self._plugins[plugin_id])

    def restart_plugin(self, plugin_id):
        self._require_plugin(plugin_id)
        self.unload_plugin(plugin_id)
        if self.is_enabled(plugin_id):
            self.load_plugin(plugin_id)
        return self._public_state(self._plugins[plugin_id])

    def load_enabled(self):
        self.scan()
        for plugin_id in list(self._plugins):
            if self.is_enabled(plugin_id):
                self.load_plugin(plugin_id)

    def unload_plugin(self, plugin_id):
        state = self._plugins.get(plugin_id)
        if not state:
            return
        state["module"] = None
        state["handler"] = None
        state["portal_handler"] = None
        state["ctx"] = None
        state["loaded"] = False

    def load_plugin(self, plugin_id):
        state = self._require_plugin(plugin_id)
        manifest = state.get("manifest") or {}
        entry = manifest.get("entry") or "main.py"
        entry_path = (state["path"] / entry).resolve()
        try:
            if not str(entry_path).startswith(str(state["path"].resolve())):
                raise ValueError("entry must stay inside plugin folder")
            if not entry_path.is_file():
                raise FileNotFoundError(f"entry not found: {entry}")
            config = self.read_plugin_config(plugin_id)["config"]
            module_name = f"webqq_plugin_{plugin_id}_{uuid.uuid4().hex}"
            spec = importlib.util.spec_from_file_location(module_name, entry_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            ctx = PluginContext(self, plugin_id, config)
            handler = getattr(module, "handle_event", None)
            portal_handler = getattr(module, "handle_portal_message", None)
            setup = getattr(module, "setup", None)
            if setup:
                instance = setup(ctx)
                if callable(instance):
                    handler = instance
                if hasattr(instance, "handle_event"):
                    handler = instance.handle_event
                if hasattr(instance, "handle_portal_message"):
                    portal_handler = instance.handle_portal_message
            if not callable(handler):
                raise ValueError("plugin must expose handle_event(event, ctx) or setup(ctx)")
            state.update({
                "module": module,
                "handler": handler,
                "portal_handler": portal_handler if callable(portal_handler) else None,
                "ctx": ctx,
                "loaded": True,
                "error": "",
                "config_error": "",
            })
        except Exception as e:
            state.update({"module": None, "handler": None, "portal_handler": None, "ctx": None, "loaded": False, "error": str(e)})
            print(f"[plugin:{plugin_id}] load failed: {e}")
        return self._public_state(state)

    async def dispatch_portal_message(self, plugin_id, message):
        state = self._require_plugin(plugin_id)
        if not self.is_enabled(plugin_id):
            raise ValueError("plugin is disabled")
        if not state.get("loaded"):
            self.load_plugin(plugin_id)
            state = self._require_plugin(plugin_id)
        handler = state.get("portal_handler")
        if not state.get("loaded") or not callable(handler):
            raise ValueError("plugin does not accept portal messages")
        try:
            result = handler(message, state["ctx"])
            if asyncio.iscoroutine(result):
                await result
            state["error"] = ""
        except Exception:
            state["error"] = traceback.format_exc(limit=5)
            print(f"[plugin:{plugin_id}] portal handler failed:\n{state['error']}")
            raise

    async def dispatch(self, event_type, payload, raw=None):
        if not self.napcat:
            return
        event = {"type": event_type, **payload, "raw": raw}
        tasks = []
        for plugin_id, state in list(self._plugins.items()):
            if not self.is_enabled(plugin_id):
                continue
            if not state.get("loaded"):
                self.load_plugin(plugin_id)
            if state.get("loaded") and callable(state.get("handler")):
                tasks.append(asyncio.create_task(self._run_handler(plugin_id, state, event)))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_handler(self, plugin_id, state, event):
        try:
            result = state["handler"](event, state["ctx"])
            if asyncio.iscoroutine(result):
                await result
            state["error"] = ""
        except Exception:
            state["error"] = traceback.format_exc(limit=5)
            print(f"[plugin:{plugin_id}] handler failed:\n{state['error']}")

    def read_plugin_config_text(self, plugin_id):
        state = self._require_plugin(plugin_id)
        path = state["path"] / "config.json"
        if not path.exists():
            return "{}"
        with open(path, encoding="utf-8") as f:
            return f.read()

    def read_plugin_config(self, plugin_id):
        state = self._require_plugin(plugin_id)
        text = self.read_plugin_config_text(plugin_id)
        try:
            config = json.loads(text or "{}")
            if not isinstance(config, dict):
                raise ValueError("config.json must be an object")
            state["config_error"] = ""
            return {"text": text, "config": config, "error": ""}
        except Exception as e:
            state["config_error"] = str(e)
            raise

    def write_plugin_config(self, plugin_id, text):
        state = self._require_plugin(plugin_id)
        try:
            parsed = json.loads(text or "{}")
            if not isinstance(parsed, dict):
                raise ValueError("config JSON must be an object")
        except Exception as e:
            state["config_error"] = str(e)
            raise
        path = state["path"] / "config.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(parsed, f, indent=2, ensure_ascii=False)
            f.write("\n")
        state["config_error"] = ""
        return self.restart_plugin(plugin_id)

    def _require_plugin(self, plugin_id):
        if not self._plugins:
            self.scan()
        state = self._plugins.get(plugin_id)
        if not state:
            raise KeyError(f"unknown plugin: {plugin_id}")
        return state


class NapCatConnection:
    def __init__(self, ws_url, token, store, plugins=None):
        self.ws_url = ws_url
        self.token = token
        self.store = store
        self.plugins = plugins
        self.session = None
        self.ws = None
        self._pending = {}
        self._stream_pending = {}
        self._subscribers = []
        self._plugin_tasks = set()

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
                        self.store.remember_private_user(uid)
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

    async def _stream_file(self, file_ref, timeout=120):
        if not self.ws or not file_ref:
            return None
        echo = f"download_file_stream-{uuid.uuid4().hex[:10]}"
        queue = asyncio.Queue()
        self._stream_pending[echo] = queue
        await self.ws.send_json({
            "action": "download_file_stream",
            "params": {"file": file_ref, "chunk_size": 256 * 1024},
            "echo": echo,
        })
        chunks = {}
        info = {}
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    return None
                payload = data.get("data") or {}
                if data.get("status") != "ok" or not isinstance(payload, dict):
                    return None
                data_type = payload.get("data_type")
                if data_type == "file_info":
                    info = payload
                elif data_type == "file_chunk":
                    try:
                        index = int(payload.get("index", len(chunks)))
                        chunks[index] = base64.b64decode(payload.get("data", ""))
                    except Exception:
                        return None
                elif data_type == "file_complete":
                    return {
                        "name": info.get("file_name") or "",
                        "size": info.get("file_size") or payload.get("total_bytes"),
                        "body": b"".join(chunks[i] for i in sorted(chunks)),
                    }
                elif payload.get("type") == "error":
                    return None
        finally:
            self._stream_pending.pop(echo, None)

    async def stream_file_candidates(self, candidates):
        for candidate in candidates:
            result = await self._stream_file(candidate)
            if result:
                return result
        return None

    async def _handle(self, data):
        echo = data.get("echo")
        if echo and echo in self._stream_pending:
            await self._stream_pending[echo].put(data)
            return
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
                self._dispatch_plugins("message", {"message": simplified}, raw=data)
        elif post_type == "message_sent":
            await self._resolve_forward_segments(data)
            simplified = self.store._simplify(data)
            reconciled = self.store.reconcile_self_message(simplified)
            if reconciled.get("ignored"):
                return
            if reconciled.get("replaced"):
                await self._broadcast({
                    "type": "message_update",
                    "data": {
                        "chat_id": reconciled["message"].get("chat_id"),
                        "message_id": reconciled["message"].get("message_id"),
                        "local_id": reconciled.get("local_id"),
                        "message": reconciled["message"],
                    },
                })
            else:
                await self._broadcast({"type": "new_message", "data": reconciled["message"]})
            self._dispatch_plugins("message", {"message": reconciled["message"]}, raw=data)
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
            self._dispatch_plugins("notice", {"notice": data}, raw=data)
        elif post_type == "notice" and data.get("notice_type") in ("friend_recall", "group_recall"):
            message_id = data.get("message_id")
            if message_id is not None:
                chat_id = None
                if data.get("notice_type") == "group_recall" and data.get("group_id"):
                    chat_id = f"group_{data['group_id']}"
                elif data.get("notice_type") == "friend_recall" and data.get("user_id"):
                    chat_id = f"private_{data['user_id']}"
                recalled = self.store.mark_recalled(
                    message_id,
                    chat_id=chat_id,
                    operator_id=data.get("operator_id"),
                    recalled_at=data.get("time"),
                )
                if recalled:
                    msg = recalled["message"]
                    await self._broadcast({
                        "type": "message_update",
                        "data": {
                            "chat_id": recalled["chat_id"],
                            "message_id": msg.get("message_id"),
                            "message": msg,
                            "patch": {
                                "recalled": True,
                                "recalled_at": msg.get("recalled_at"),
                                "recall_operator_id": msg.get("recall_operator_id"),
                            },
                        },
                    })
                    if not recalled.get("already_recalled"):
                        system = self.store.add_system_message(
                            recalled["chat_id"],
                            recall_notice_text(data),
                            notice_type=data.get("notice_type", ""),
                            operator_id=data.get("operator_id"),
                            target_id=data.get("user_id"),
                        )
                        if system:
                            await self._broadcast({"type": "new_message", "data": system})
            self._dispatch_plugins("notice", {"notice": data}, raw=data)
        elif post_type == "notice":
            system = self._notice_to_system_message(data)
            if system:
                await self._broadcast({"type": "new_message", "data": system})
            self._dispatch_plugins("notice", {"notice": data, "system_message": system}, raw=data)
        elif post_type == "request":
            await self._handle_request(data)
            self._dispatch_plugins("request", {"request": data}, raw=data)

    def _dispatch_plugins(self, event_type, payload, raw=None):
        if not self.plugins:
            return
        task = asyncio.create_task(self.plugins.dispatch(event_type, payload, raw=raw))
        self._plugin_tasks.add(task)
        task.add_done_callback(self._plugin_tasks.discard)

    def _notice_to_system_message(self, data):
        chat_id = notice_chat_id(data)
        if not chat_id:
            return None
        text = notice_text(data)
        if not text:
            return None
        return self.store.add_system_message(
            chat_id,
            text,
            notice_type=data.get("notice_type", ""),
            sub_type=data.get("sub_type", ""),
            operator_id=data.get("operator_id"),
            target_id=data.get("user_id") or data.get("target_id"),
        )

    async def _handle_request(self, data):
        request_type = data.get("request_type")
        flag = data.get("flag")
        if not flag:
            return
        if request_type == "friend":
            resp = await self._request("set_friend_add_request", {"flag": str(flag), "approve": True}, timeout=10)
            if not resp or resp.get("status") != "ok":
                print(f"[napcat] failed to auto-approve friend request: {resp}")
            return
        if request_type == "group" and data.get("sub_type") == "invite":
            resp = await self._request("set_group_add_request", {"flag": str(flag), "approve": True}, timeout=10)
            if not resp or resp.get("status") != "ok":
                print(f"[napcat] failed to auto-approve group invite: {resp}")

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
            params = {"user_id": parsed["private_id"], "message": message}
            context = self.store.private_send_context(parsed["private_id"])
            if context.get("group_id"):
                params["group_id"] = context["group_id"]
            return await self._request("send_private_msg", params)
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
            params = {
                "user_id": parsed["private_id"],
                "file": path,
                "name": name,
            }
            context = self.store.private_send_context(parsed["private_id"])
            if context.get("group_id"):
                params["group_id"] = context["group_id"]
            return await self._request("upload_private_file", params, timeout=60)
        return await self._request("upload_private_file", {
            "user_id": parsed["user_id"],
            "group_id": parsed["group_id"],
            "file": path,
            "name": name,
        }, timeout=60)

    async def resolve_file_locations(self, file_id="", file_path="", busid="", url="", chat_id="", filename=""):
        urls = []
        paths = []
        normalized_url = normalize_file_url(url, filename)
        if file_url_allowed(normalized_url):
            urls.append(normalized_url)
        candidates = []
        if file_id:
            candidates.append({"file_id": file_id})
            candidates.append({"file_hash": file_id})
            candidates.append({"file_uuid": file_id})
        if file_path:
            candidates.append({"file": file_path})
            candidates.append({"file_name": file_path})
        if file_id and busid:
            candidates.append({"file_id": file_id, "busid": busid})
            candidates.append({"file_hash": file_id, "busid": busid})
            candidates.append({"file_uuid": file_id, "busid": busid})
        parsed = parse_chat_id(chat_id)
        for params in candidates:
            if parsed and parsed["type"] == "group":
                params = {**params, "group_id": parsed["group_id"]}
            actions = ["get_file"]
            if parsed and parsed["type"] == "group":
                actions.append("get_group_file_url")
            for action in actions:
                resp = await self._request(action, params, timeout=10)
                if not resp or resp.get("status") != "ok":
                    continue
                data = resp.get("data") or {}
                for candidate in extract_file_urls(data):
                    candidate = normalize_file_url(candidate, filename)
                    if file_url_allowed(candidate):
                        urls.append(candidate)
                paths.extend(extract_file_paths(data))
        return {
            "urls": list(dict.fromkeys(urls)),
            "paths": list(dict.fromkeys(paths)),
        }

    async def set_msg_emoji_like(self, message_id, emoji_id, enabled=True):
        return await self._request("set_msg_emoji_like", {
            "message_id": int(message_id),
            "emoji_id": str(emoji_id),
            "set": bool(enabled),
        })

    async def delete_msg(self, message_id):
        return await self._request("delete_msg", {"message_id": int(message_id)}, timeout=10)

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

    async def fetch_history(self, chat_id, before_message_id=None, count=50):
        parsed = parse_chat_id(chat_id)
        if not parsed:
            return []
        if parsed["type"] == "temp":
            parsed = {"type": "private", "private_id": parsed["user_id"]}
        params = {
            "count": max(1, min(int(count or 50), 50)),
            "parse_mult_msg": True,
            "disable_get_url": False,
        }
        if before_message_id:
            params["message_seq"] = str(before_message_id)
        if parsed["type"] == "group":
            action = "get_group_msg_history"
            params["group_id"] = str(parsed["group_id"])
        else:
            action = "get_friend_msg_history"
            params["user_id"] = str(parsed["private_id"])
        resp = await self._request(action, params, timeout=15)
        if not resp or resp.get("status") != "ok":
            return []
        data = resp.get("data") or {}
        messages = data.get("messages") if isinstance(data, dict) else []
        return messages if isinstance(messages, list) else []

    async def mark_chat_read(self, chat_id):
        parsed = parse_chat_id(chat_id)
        if not parsed:
            return None
        if parsed["type"] == "temp":
            parsed = {"type": "private", "private_id": parsed["user_id"]}
        if parsed["type"] == "group":
            return await self._request("mark_group_msg_as_read", {"group_id": str(parsed["group_id"])}, timeout=5)
        return await self._request("mark_private_msg_as_read", {"user_id": str(parsed["private_id"])}, timeout=5)

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


class BanTracker:
    def __init__(self, max_failures=5, window_seconds=300, ban_seconds=1800):
        self.max_failures = max(1, int(max_failures or 5))
        self.window_seconds = max(1, int(window_seconds or 300))
        self.ban_seconds = max(1, int(ban_seconds or 1800))
        self._failures = defaultdict(deque)
        self._banned_until = {}

    def is_banned(self, ip, now=None):
        if not ip:
            return False
        now = now or time.time()
        banned_until = self._banned_until.get(ip, 0)
        if banned_until > now:
            return True
        self._banned_until.pop(ip, None)
        return False

    def record_failure(self, ip, now=None):
        if not ip:
            return False
        now = now or time.time()
        failures = self._failures[ip]
        cutoff = now - self.window_seconds
        while failures and failures[0] < cutoff:
            failures.popleft()
        failures.append(now)
        if len(failures) >= self.max_failures:
            self._banned_until[ip] = now + self.ban_seconds
            failures.clear()
            print(f"[security] banned {ip} for {self.ban_seconds}s after failed auth attempts")
            return True
        return False

    def clear(self, ip):
        if not ip:
            return
        self._failures.pop(ip, None)
        self._banned_until.pop(ip, None)


def client_ip(request):
    for header in ("CF-Connecting-IP", "X-Real-IP"):
        value = request.headers.get(header, "").strip()
        if value:
            return value
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",", 1)[0].strip()
    return request.remote or ""


def record_auth_failure(request):
    tracker = request.app.get("ban_tracker")
    if tracker:
        tracker.record_failure(client_ip(request))


@web.middleware
async def ban_middleware(request, handler):
    tracker = request.app.get("ban_tracker")
    if tracker and tracker.is_banned(client_ip(request)):
        return web.json_response({"error": "too many failed auth attempts"}, status=429)
    return await handler(request)


def check_auth(request):
    cfg = request.app["config"]
    auth_token = cfg.get("web_token", "")
    if not auth_token:
        return True
    req_token = request.query.get("token") or request.cookies.get("token") or ""
    ok = hmac.compare_digest(req_token, auth_token)
    if not ok and req_token:
        record_auth_failure(request)
    return ok


async def read_json_body(request):
    try:
        body = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='{"error":"invalid JSON"}', content_type="application/json")
    if not isinstance(body, dict):
        raise web.HTTPBadRequest(text='{"error":"JSON body must be an object"}', content_type="application/json")
    return body


def dispatch_plugin_message_later(napcat, message, raw=None):
    if napcat.plugins:
        asyncio.create_task(napcat.plugins.dispatch("message", {"message": message}, raw=raw))


def make_local_self_message(store, parsed_chat, chat_id, text, reply_to=None, message_id=None, source="user"):
    now = int(time.time())
    return {
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
        "videos": [],
        "records": [],
        "extra_segments": [],
        "reactions": [],
        "chat_id": chat_id,
        "type": parsed_chat["type"],
        "group_id": parsed_chat.get("group_id"),
        "user_id": parsed_chat.get("user_id") or parsed_chat.get("private_id"),
        "chat_name": "",
        "self": True,
        "source": source,
    }


def update_chat_after_local_send(store, chat_id, parsed_chat, text, now=None):
    now = now or int(time.time())
    if chat_id in store._chat_meta:
        store._chat_meta[chat_id]["last_time"] = now
        store._chat_meta[chat_id]["last_text"] = text[:50]
    else:
        store.ensure_chat(chat_id, chat_id, parsed_chat["type"])
        store._chat_meta[chat_id]["last_time"] = now
        store._chat_meta[chat_id]["last_text"] = text[:50]
    store._dirty.add(chat_id)


async def send_text_and_register(napcat, store, chat_id, text, reply_to=None, source="user", optimistic=False):
    upstream_chat_id = chat_id
    chat_id = canonical_chat_id(chat_id)
    parsed_chat = parse_chat_id(chat_id)
    if not parsed_chat:
        raise ValueError("invalid chat_id")
    simplified = None
    if optimistic:
        simplified = make_local_self_message(store, parsed_chat, chat_id, text, reply_to=reply_to, source=source)
        store.register_pending_local_message(chat_id, simplified)
        update_chat_after_local_send(store, chat_id, parsed_chat, text, now=simplified["time"])
        await napcat._broadcast({"type": "new_message", "data": simplified})
        dispatch_plugin_message_later(napcat, simplified, raw=None)
    result = await napcat.send_message(upstream_chat_id, text, reply_to=reply_to)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "send failed")) if result else "not connected"
        if simplified:
            simplified["pending"] = False
            simplified["send_error"] = err
            store._dirty.add(chat_id)
            await napcat._broadcast({
                "type": "message_update",
                "data": {
                    "chat_id": chat_id,
                    "local_id": simplified.get("local_id"),
                    "message": simplified,
                    "patch": {"pending": False, "send_error": err},
                },
            })
            if napcat.plugins:
                asyncio.create_task(napcat.plugins.dispatch(
                    "message_send_failed",
                    {"message": simplified, "error": err},
                    raw=None,
                ))
        raise RuntimeError(err)
    message_id = extract_message_id(result)
    if optimistic:
        if message_id is not None:
            simplified["message_id"] = message_id
            store._reindex_chat(chat_id)
            store._dirty.add(chat_id)
            await napcat._broadcast({
                "type": "message_update",
                "data": {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "local_id": simplified.get("local_id"),
                    "message": simplified,
                },
            })
    else:
        simplified = make_local_self_message(store, parsed_chat, chat_id, text, reply_to=reply_to, message_id=message_id, source=source)
        store.register_pending_local_message(chat_id, simplified)
        update_chat_after_local_send(store, chat_id, parsed_chat, text, now=simplified["time"])
        await napcat._broadcast({"type": "new_message", "data": simplified})
        dispatch_plugin_message_later(napcat, simplified, raw=None)
    return {"result": result, "message": simplified}


async def handle_login(request):
    cfg = request.app["config"]
    auth_token = cfg.get("web_token", "")
    if not auth_token:
        return web.json_response({"ok": True})
    body = await read_json_body(request)
    token = body.get("token", "")
    if not isinstance(token, str):
        record_auth_failure(request)
        return web.json_response({"ok": False, "error": "token must be a string"}, status=400)
    if hmac.compare_digest(token, auth_token):
        tracker = request.app.get("ban_tracker")
        if tracker:
            tracker.clear(client_ip(request))
        resp = web.json_response({"ok": True})
        resp.set_cookie("token", auth_token, max_age=86400 * 30, httponly=True)
        return resp
    record_auth_failure(request)
    return web.json_response({"ok": False, "error": "invalid token"}, status=401)


async def handle_chats(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    return web.json_response({"chats": request.app["store"].get_chats()})


async def handle_messages(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    store = request.app["store"]
    chat_id = canonical_chat_id(request.query.get("chat_id", ""))
    try:
        limit = max(1, min(int(request.query.get("limit", "50")), 200))
    except (TypeError, ValueError):
        return web.json_response({"error": "limit must be an integer"}, status=400)
    before_raw = request.query.get("before")
    try:
        before = float(before_raw) if before_raw else None
    except (TypeError, ValueError):
        return web.json_response({"error": "before must be a timestamp"}, status=400)
    messages = store.get_messages(chat_id, limit=limit, before=before)
    if len(messages) < limit and request.app["napcat"].ws is not None:
        before_message_id = store.oldest_message_id(chat_id, before=before)
        try:
            history = await request.app["napcat"].fetch_history(chat_id, before_message_id=before_message_id, count=limit)
            added = store.add_history_messages(history)
            if added:
                messages = store.get_messages(chat_id, limit=limit, before=before)
        except Exception:
            pass
    return web.json_response({"messages": messages})


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
    chat_id = f"private_{user_id}"
    store = request.app["store"]
    store.remember_temp_context(user_id, group_id, group_name)
    display_name = store._chat_meta.get(chat_id, {}).get("name") or store._nicknames.get(user_id) or name
    store.ensure_chat(chat_id, display_name, "private", user_id=int(user_id), temp_group_id=int(group_id), temp_group_name=group_name)
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
        sent = await send_text_and_register(napcat, store, chat_id, text, reply_to=reply_to)
        return web.json_response({"ok": True, "data": sent["result"]})
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
        store = request.app["store"]
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
        canonical_id = canonical_chat_id(chat_id)
        canonical_parsed = parse_chat_id(canonical_id)
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
        simplified = {
            "message_id": None,
            "time": now,
            "sender_id": "self",
            "sender_name": "You",
            "sender_avatar_url": avatar_url_for("user", store._self_user.get("user_id")),
            "content": "[file]",
            "mentions": {},
            "images": [],
            "forwards": [],
            "files": [{"name": filename, "size": size}],
            "videos": [],
            "records": [],
            "extra_segments": [],
            "reactions": [],
            "chat_id": canonical_id,
            "type": canonical_parsed["type"],
            "group_id": canonical_parsed.get("group_id"),
            "user_id": canonical_parsed.get("user_id") or canonical_parsed.get("private_id"),
            "chat_name": "",
            "self": True,
        }
        store.register_pending_local_message(canonical_id, simplified)
        if canonical_id not in store._chat_meta:
            store.ensure_chat(canonical_id, canonical_id, canonical_parsed["type"])
        store._chat_meta[canonical_id]["last_time"] = now
        store._chat_meta[canonical_id]["last_text"] = "[file]"
        store._dirty.add(canonical_id)
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
    chat_id = canonical_chat_id(chat_id)
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


async def handle_message_revoke(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    message_id = str(body.get("message_id", "")).strip()
    chat_id = str(body.get("chat_id", "")).strip()
    chat_id = canonical_chat_id(chat_id)
    if not is_int_string(message_id):
        return web.json_response({"ok": False, "error": "message_id is required"}, status=400)
    found = request.app["store"].find_message(message_id, chat_id=chat_id if parse_chat_id(chat_id) else None)
    if not found:
        return web.json_response({"ok": False, "error": "message is not in local history"}, status=404)
    message = found["message"]
    if not message.get("self"):
        return web.json_response({"ok": False, "error": "only your own messages can be revoked"}, status=400)
    try:
        age = time.time() - float(message.get("time", 0))
    except (TypeError, ValueError):
        age = REVOKE_WINDOW_SECONDS + 1
    if age > REVOKE_WINDOW_SECONDS:
        return web.json_response({"ok": False, "error": "message is too old to revoke"}, status=400)
    result = await request.app["napcat"].delete_msg(message_id)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "revoke failed")) if result else "not connected"
        return web.json_response({"ok": False, "error": err}, status=500)
    recalled = request.app["store"].mark_recalled(
        message_id,
        chat_id=chat_id if parse_chat_id(chat_id) else None,
        operator_id=request.app["store"]._self_user.get("user_id"),
        recalled_at=int(time.time()),
    )
    payload = {"message_id": message_id}
    if recalled:
        msg = recalled["message"]
        payload.update({
            "chat_id": recalled["chat_id"],
            "message": msg,
            "patch": {
                "recalled": True,
                "recalled_at": msg.get("recalled_at"),
                "recall_operator_id": msg.get("recall_operator_id"),
            },
        })
        await request.app["napcat"]._broadcast({"type": "message_update", "data": payload})
    return web.json_response({"ok": True, **payload})


async def handle_message_emoji_likes(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    message_id = str(request.query.get("message_id", "")).strip()
    chat_id = canonical_chat_id(request.query.get("chat_id", ""))
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


def notice_chat_id(data):
    notice_type = data.get("notice_type")
    if notice_type in ("group_increase", "group_decrease", "group_admin", "group_card", "group_ban"):
        return f"group_{data['group_id']}" if data.get("group_id") else None
    if notice_type == "notify" and data.get("sub_type") in ("group_name", "title") and data.get("group_id"):
        return f"group_{data['group_id']}"
    return None


def recall_notice_text(data):
    operator = first_text(data.get("operator_id"), data.get("user_id"), "Someone")
    if data.get("notice_type") == "group_recall":
        target = first_text(data.get("user_id"))
        return f"Message recalled by {operator}" + (f" for {target}." if target and target != operator else ".")
    return f"Message recalled by {operator}."


def notice_text(data):
    notice_type = data.get("notice_type")
    sub_type = data.get("sub_type", "")
    user = first_text(data.get("user_id"), "Someone")
    operator = first_text(data.get("operator_id"))
    if notice_type == "group_increase":
        if sub_type == "invite" and operator:
            return f"{user} joined the group by invitation from {operator}."
        return f"{user} joined the group."
    if notice_type == "group_decrease":
        if sub_type == "leave":
            return f"{user} left the group."
        if sub_type == "kick_me":
            return "You were removed from the group."
        if sub_type == "disband":
            return "The group was disbanded."
        return f"{user} was removed from the group" + (f" by {operator}." if operator else ".")
    if notice_type == "group_admin":
        return f"{user} is now an admin." if sub_type == "set" else f"{user} is no longer an admin."
    if notice_type == "group_card":
        old = first_text(data.get("card_old"), "(empty)")
        new = first_text(data.get("card_new"), "(empty)")
        return f"{user} changed group card: {old} -> {new}."
    if notice_type == "notify" and sub_type == "group_name":
        return f"Group name changed to {first_text(data.get('name_new'), 'a new name')}."
    if notice_type == "notify" and sub_type == "title":
        return f"{user} received title: {first_text(data.get('title'), '(empty)')}."
    if notice_type == "group_ban":
        duration = first_text(data.get("duration"))
        if sub_type == "lift_ban" or duration == "0":
            return f"{user} was unmuted" + (f" by {operator}." if operator else ".")
        if duration == "-1":
            return "All members were muted" + (f" by {operator}." if operator else ".")
        return f"{user} was muted" + (f" by {operator}" if operator else "") + (f" for {format_duration(duration)}." if duration else ".")
    return ""


def format_duration(seconds):
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return str(seconds)
    if total < 0:
        return "an unknown duration"
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m"
    if total < 86400:
        return f"{total // 3600}h"
    return f"{total // 86400}d"


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


def plugin_error_response(error, status=400):
    return web.json_response({"ok": False, "error": str(error)}, status=status)


async def handle_plugins(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    return web.json_response({"plugins": request.app["plugins"].list_plugins()})


async def handle_plugins_refresh(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    request.app["plugins"].scan()
    request.app["plugins"].load_enabled()
    return web.json_response({"ok": True, "plugins": request.app["plugins"].list_plugins()})


async def handle_plugin_enable(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    try:
        plugin = request.app["plugins"].set_enabled(plugin_id, True)
        return web.json_response({"ok": True, "plugin": plugin})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


async def handle_plugin_disable(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    try:
        plugin = request.app["plugins"].set_enabled(plugin_id, False)
        return web.json_response({"ok": True, "plugin": plugin})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


async def handle_plugin_restart(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    try:
        plugin = request.app["plugins"].restart_plugin(plugin_id)
        return web.json_response({"ok": True, "plugin": plugin})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


async def handle_plugin_config_get(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    try:
        manager = request.app["plugins"]
        text = manager.read_plugin_config_text(plugin_id)
        error = ""
        parsed = None
        try:
            parsed = json.loads(text or "{}")
            if not isinstance(parsed, dict):
                raise ValueError("config.json must be an object")
            manager._plugins[plugin_id]["config_error"] = ""
        except Exception as e:
            error = str(e)
            manager._plugins[plugin_id]["config_error"] = error
        return web.json_response({"ok": not error, "text": text, "config": parsed, "error": error})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


async def handle_plugin_config_put(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    body = await read_json_body(request)
    text = body.get("text")
    if not isinstance(text, str):
        return web.json_response({"ok": False, "error": "text is required"}, status=400)
    try:
        plugin = request.app["plugins"].write_plugin_config(plugin_id, text)
        return web.json_response({"ok": True, "plugin": plugin})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


async def handle_plugin_portal_message(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    plugin_id = request.match_info.get("plugin_id", "")
    body = await read_json_body(request)
    chat_id = body.get("chat_id")
    text = body.get("text")
    reply_to = body.get("reply_to")
    if not isinstance(chat_id, str) or not chat_id:
        return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
    parsed_chat = parse_chat_id(chat_id)
    if not parsed_chat:
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    chat_id = canonical_chat_id(chat_id)
    parsed_chat = parse_chat_id(chat_id)
    if not isinstance(text, str):
        return web.json_response({"ok": False, "error": "text is required"}, status=400)
    if reply_to is not None:
        reply_to = str(reply_to).strip()
        if not reply_to.isdigit():
            return web.json_response({"ok": False, "error": "invalid reply_to"}, status=400)
    message = {
        "chat_id": chat_id,
        "chat_type": parsed_chat["type"],
        "text": text,
        "reply_to": reply_to,
        "source": "ui_portal",
        "self_user": dict(request.app["store"]._self_user),
    }
    try:
        await request.app["plugins"].dispatch_portal_message(plugin_id, message)
        return web.json_response({"ok": True})
    except KeyError as e:
        return plugin_error_response(e, status=404)
    except Exception as e:
        return plugin_error_response(e, status=400)


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


async def handle_mark_read(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = str(body.get("chat_id", "")).strip()
    if not parse_chat_id(chat_id):
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    result = await request.app["napcat"].mark_chat_read(chat_id)
    if result and result.get("status") not in (None, "ok"):
        return web.json_response({"ok": False, "error": result.get("message") or result.get("wording") or "mark read failed"}, status=500)
    return web.json_response({"ok": True})


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


def normalize_file_url(url, filename=""):
    if not isinstance(url, str) or not url:
        return ""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return ""
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if filename and any(key == "fname" and value == "" for key, value in query):
        query = [(key, safe_download_name(filename) if key == "fname" and value == "" else value) for key, value in query]
        return urlunparse(parsed._replace(query=urlencode(query)))
    return url


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
        for key in (
            "url", "file", "download_url", "downloadUrl", "downloadURL",
            "download_addr", "downloadAddr", "download", "link", "download_link",
            "downloadLink", "dlink", "dLink",
        ):
            value = payload.get(key)
            if isinstance(value, str):
                urls.append(value)
        for key in ("data", "file_info", "fileInfo", "info", "item", "items", "list"):
            urls.extend(extract_file_urls(payload.get(key)))
    elif isinstance(payload, list):
        for item in payload:
            urls.extend(extract_file_urls(item))
    return urls


def extract_file_paths(payload):
    paths = []
    if isinstance(payload, dict):
        for key in ("file", "path", "downloadPath", "download_path"):
            value = payload.get(key)
            if isinstance(value, str) and value.startswith("/") and "\x00" not in value:
                paths.append(value)
        for key in ("data", "file_info", "fileInfo", "info", "item", "items", "list"):
            paths.extend(extract_file_paths(payload.get(key)))
    elif isinstance(payload, list):
        for item in payload:
            paths.extend(extract_file_paths(item))
    return paths


async def fetch_first_file(urls, filename):
    async with aiohttp.ClientSession() as session:
        last_status = 502
        for file_url in urls:
            try:
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": "https://im.qq.com/",
                    "Accept": "*/*",
                }
                async with session.get(file_url, timeout=60, headers=headers, allow_redirects=True) as resp:
                    last_status = resp.status
                    if resp.status != 200:
                        print(f"[file] upstream returned {resp.status} for {urlparse(file_url).hostname}")
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
                print(f"[file] upstream fetch failed for {urlparse(file_url).hostname}")
                continue
        return web.json_response({"error": "file fetch failed"}, status=last_status)


async def serve_first_local_file(paths, filename):
    for path in paths:
        try:
            if not os.access(path, os.R_OK):
                continue
            real_path = Path(path).resolve()
            if not real_path.is_file():
                continue
            content_type = mimetypes.guess_type(safe_download_name(filename))[0] or "application/octet-stream"
            return web.FileResponse(
                real_path,
                headers={
                    "Cache-Control": "private, max-age=300",
                    "Content-Disposition": content_disposition(filename),
                    "Content-Type": content_type,
                },
            )
        except Exception:
            print(f"[file] local file fallback failed for {Path(path).name}")
            continue
    return None


def stream_file_response(streamed, filename):
    if not streamed or not streamed.get("body"):
        return None
    response_name = safe_download_name(filename or streamed.get("name") or "file")
    content_type = mimetypes.guess_type(response_name)[0] or "application/octet-stream"
    headers = {
        "Cache-Control": "private, max-age=300",
        "Content-Disposition": content_disposition(response_name),
        "Content-Length": str(len(streamed["body"])),
    }
    return web.Response(body=streamed["body"], content_type=content_type, headers=headers)


async def handle_file_proxy(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    filename = safe_download_name(request.query.get("name", "file"))
    locations = await request.app["napcat"].resolve_file_locations(
        file_id=request.query.get("id", ""),
        file_path=request.query.get("file", ""),
        busid=request.query.get("busid", ""),
        url=request.query.get("url", ""),
        chat_id=request.query.get("chat_id", ""),
        filename=filename,
    )
    urls = locations["urls"]
    stream_candidates = [
        value for value in (
            request.query.get("id", ""),
            request.query.get("file", ""),
            request.query.get("name", ""),
        ) if value
    ]
    if not urls:
        local_response = await serve_first_local_file(locations["paths"], filename)
        if local_response:
            return local_response
        streamed = await request.app["napcat"].stream_file_candidates(stream_candidates)
        stream_response = stream_file_response(streamed, filename)
        if stream_response:
            return stream_response
        print(f"[file] no download url for chat={request.query.get('chat_id', '')} id={request.query.get('id', '')} file={request.query.get('file', '')}")
        return web.json_response({"error": "file url unavailable"}, status=400)
    response = await fetch_first_file(urls, filename)
    if response.status < 400:
        return response
    local_response = await serve_first_local_file(locations["paths"], filename)
    if local_response:
        return local_response
    streamed = await request.app["napcat"].stream_file_candidates(stream_candidates)
    return stream_file_response(streamed, filename) or response


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
    plugins = PluginManager(PLUGIN_DIR, config, store)
    napcat = NapCatConnection(config["ws_url"], config.get("napcat_token", ""), store, plugins=plugins)
    plugins.set_napcat(napcat)
    plugins.load_enabled()

    app = web.Application(client_max_size=MAX_FILE_UPLOAD + 1024 * 1024, middlewares=[ban_middleware])
    app["config"] = config
    app["store"] = store
    app["napcat"] = napcat
    app["plugins"] = plugins
    app["ban_tracker"] = BanTracker(
        max_failures=config.get("fail2ban_max_failures", DEFAULT_CONFIG["fail2ban_max_failures"]),
        window_seconds=config.get("fail2ban_window_seconds", DEFAULT_CONFIG["fail2ban_window_seconds"]),
        ban_seconds=config.get("fail2ban_ban_seconds", DEFAULT_CONFIG["fail2ban_ban_seconds"]),
    )
    app.on_shutdown.append(flush_on_shutdown)

    app.router.add_post("/api/login", handle_login)
    app.router.add_get("/api/chats", handle_chats)
    app.router.add_get("/api/messages", handle_messages)
    app.router.add_post("/api/temp-chat", handle_temp_chat)
    app.router.add_post("/api/send", handle_send)
    app.router.add_post("/api/send-file", handle_send_file)
    app.router.add_post("/api/message/revoke", handle_message_revoke)
    app.router.add_post("/api/message/emoji-like", handle_message_emoji_like)
    app.router.add_get("/api/message/emoji-likes", handle_message_emoji_likes)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/plugins", handle_plugins)
    app.router.add_post("/api/plugins/refresh", handle_plugins_refresh)
    app.router.add_post("/api/plugins/{plugin_id}/enable", handle_plugin_enable)
    app.router.add_post("/api/plugins/{plugin_id}/disable", handle_plugin_disable)
    app.router.add_post("/api/plugins/{plugin_id}/restart", handle_plugin_restart)
    app.router.add_get("/api/plugins/{plugin_id}/config", handle_plugin_config_get)
    app.router.add_put("/api/plugins/{plugin_id}/config", handle_plugin_config_put)
    app.router.add_post("/api/plugins/{plugin_id}/portal-message", handle_plugin_portal_message)
    app.router.add_get("/api/nicknames", handle_nicknames)
    app.router.add_get("/api/group-members", handle_group_members)
    app.router.add_post("/api/mark-read", handle_mark_read)
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
