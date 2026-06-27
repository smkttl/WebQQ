from .common import *

class MessageStore:
    def __init__(self, maxlen=1000, data_dir=DATA_DIR):
        self.maxlen = maxlen
        self._data = defaultdict(lambda: deque(maxlen=maxlen))
        self._chat_meta = {}
        self._data_dir = Path(data_dir)
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
        self._data_dir.mkdir(exist_ok=True)

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
            if normalized.get("temp_group_id"):
                self.remember_temp_context(
                    parsed["private_id"],
                    normalized.get("temp_group_id"),
                    normalized.get("temp_group_name") or "",
                )
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
            "name": self._message_chat_display_name(chat_id, last),
            "type": last.get("type", ""),
            "last_time": last.get("time", 0),
            "last_text": (last.get("content", "") or "")[:50],
            "avatar_url": chat_avatar_url(chat_id, last.get("type", ""), last.get("user_id"), last.get("group_id")),
            **{
                k: last.get(k)
                for k in ("user_id", "group_id", "temp_group_id", "temp_group_name")
                if last.get(k) is not None
            },
        }

    def refresh_private_temp_names_for_group(self, group_id):
        group_id = int(group_id)
        for chat_id, meta in list(self._chat_meta.items()):
            parsed = parse_chat_id(chat_id)
            if not parsed or parsed["type"] != "private":
                continue
            uid = str(parsed["private_id"])
            context = self._private_temp_contexts.get(uid) or {}
            meta_group_id = meta.get("temp_group_id")
            if context.get("group_id") != group_id and meta_group_id != group_id:
                continue
            name = self._temp_group_member_name(uid, {"temp_group_id": group_id})
            if name:
                meta["name"] = name

    def _message_chat_display_name(self, chat_id, message):
        parsed = parse_chat_id(chat_id)
        if parsed and parsed["type"] == "private":
            uid = str(parsed["private_id"])
            chat_name = str(message.get("chat_name") or "")
            sender_name = str(message.get("sender_name") or "")
            if chat_name.startswith("群临时会话") or chat_name.endswith(" / 临时会话") or chat_name.endswith(" / 群临时会话"):
                chat_name = ""
            if is_placeholder_name(sender_name):
                sender_name = ""
            group_member_name = self._temp_group_member_name(uid, message)
            return self._nicknames.get(uid) or group_member_name or chat_name or sender_name or uid
        return message.get("chat_name") or self._chat_meta.get(chat_id, {}).get("name") or chat_id

    def _temp_group_member_name(self, user_id, message=None):
        uid = str(user_id)
        group_id = None
        if isinstance(message, dict):
            group_id = message.get("temp_group_id") or message.get("group_id")
        if not group_id:
            group_id = (self._private_temp_contexts.get(uid) or {}).get("group_id")
        if group_id:
            name = self._group_members.get(f"group_{group_id}", {}).get(uid)
            if name and not is_placeholder_name(name):
                return name
        return ""

    def resolve_display_name(self, user_id, fallback="", group_id=None):
        uid = str(user_id or "")
        if not uid:
            return fallback if not is_placeholder_name(fallback) else ""
        if uid == str(self._self_user.get("user_id") or ""):
            name = self._self_user.get("name")
            if name and not is_placeholder_name(name):
                return name
        if group_id:
            name = self._group_members.get(f"group_{group_id}", {}).get(uid)
            if name and not is_placeholder_name(name):
                return name
        name = self._nicknames.get(uid)
        if name and not is_placeholder_name(name):
            return name
        return "" if is_placeholder_name(fallback) else fallback

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
            self.ensure_chat(chat_id, self._message_chat_display_name(chat_id, simplified), simplified.get("type", ""))
        current_name = self._chat_meta[chat_id].get("name", chat_id)
        is_known_private_temp = (
            chat_id.startswith("private_")
            and str(simplified.get("user_id") or "") in self._known_private_users
            and simplified.get("temp_group_id")
        )
        display_name = self._message_chat_display_name(chat_id, simplified)
        self._chat_meta[chat_id]["name"] = current_name if is_known_private_temp else (display_name or current_name)
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
                if is_placeholder_name(sender_name):
                    chat_name = str(msg.get("user_id", ""))
                elif temp_group_name:
                    chat_name = f"{temp_group_name} / {sender_name}"
                else:
                    chat_name = sender_name
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
                    qq = str(d.get("qq", ""))
                    nick = self.resolve_display_name(qq, d.get("name") or qq, group_id=msg.get("group_id"))
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
