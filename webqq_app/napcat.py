from .common import *

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
                self.store.refresh_private_temp_names_for_group(group_id)
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
