#!/usr/bin/env python3
import asyncio
import json
import os
import time
import uuid
import hmac
from collections import defaultdict, deque
from pathlib import Path

from aiohttp import web
import aiohttp

CONFIG_PATH = Path(__file__).parent / "config.json"
STATIC_DIR = Path(__file__).parent / "static"
DATA_DIR = Path(__file__).parent / "data"
MAX_MESSAGES = 1000

DEFAULT_CONFIG = {
    "ws_url": "ws://localhost:49341/?message_post_format=array",
    "napcat_token": "",
    "web_port": 8080,
    "web_token": "",
    "flush_interval": 15,
}


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
                    if msgs:
                        last = msgs[-1]
                        self._chat_meta[chat_id] = {
                            "chat_id": chat_id,
                            "name": last.get("chat_name", chat_id),
                            "type": last.get("type", ""),
                            "last_time": last.get("time", 0),
                            "last_text": (last.get("content", "") or "")[:50],
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
            return f"private_{msg['user_id']}"
        return None

    def add(self, msg):
        key = self.chat_key(msg)
        if not key:
            return
        simplified = self._simplify(msg)
        self._data[key].append(simplified)
        if key not in self._chat_meta:
            self._chat_meta[key] = {
                "chat_id": key,
                "name": simplified.get("chat_name", key),
                "type": msg.get("message_type", ""),
                "last_time": 0,
                "last_text": "",
            }
        self._chat_meta[key]["name"] = simplified.get("chat_name") or self._chat_meta[key]["name"]
        self._chat_meta[key]["last_time"] = simplified["time"]
        self._chat_meta[key]["last_text"] = simplified["content"][:50]
        self._dirty.add(key)
        return simplified

    def _simplify(self, msg):
        sender = msg.get("sender") or {}
        chat_name = ""
        mt = msg.get("message_type", "")
        if mt == "group":
            chat_name = msg.get("group_name", "")
        if mt == "private":
            chat_name = sender.get("nickname", str(msg.get("user_id", "")))
        if sender.get("user_id"):
            nick = sender.get("card") or sender.get("nickname") or ""
            if nick:
                self._nicknames[str(sender["user_id"])] = nick
        content, mentions = self._extract_text(msg)
        return {
            "message_id": msg.get("message_id"),
            "time": msg.get("time", int(time.time())),
            "sender_id": msg.get("user_id"),
            "sender_name": sender.get("nickname", "") or sender.get("card", "") or str(msg.get("user_id", "")),
            "content": content,
            "mentions": mentions,
            "chat_id": self.chat_key(msg),
            "type": mt,
            "chat_name": chat_name,
            "self": msg.get("sub_type") == "friend" and msg.get("target_id") == msg.get("self_user_id"),
        }

    def _extract_text(self, msg):
        segments = msg.get("message", [])
        mentions = {}
        if isinstance(segments, list):
            parts = []
            for seg in segments:
                t = seg.get("type", "")
                d = seg.get("data", {})
                if t == "text":
                    parts.append(d.get("text", ""))
                elif t == "image":
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
                else:
                    parts.append(f"[{t}]")
            return "".join(parts), mentions
        return str(msg.get("raw_message", "")), mentions

    def get_messages(self, chat_id, limit=50, before=None):
        msgs = list(self._data.get(chat_id, []))
        if before:
            msgs = [m for m in msgs if m["time"] < before]
        return msgs[-limit:]

    def get_chats(self):
        return sorted(self._chat_meta.values(), key=lambda c: c.get("last_time", 0), reverse=True)

    def ensure_chat(self, chat_id, name, chat_type):
        if chat_id in self._chat_meta:
            self._chat_meta[chat_id]["name"] = name
        else:
            self._chat_meta[chat_id] = {
                "chat_id": chat_id,
                "name": name,
                "type": chat_type,
                "last_time": 0,
                "last_text": "",
            }


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
                for m in (resp.get("data") or []):
                    uid = m.get("user_id")
                    if uid:
                        uid = str(uid)
                        nick = m.get("card") or m.get("nickname") or str(uid)
                        members[uid] = nick
                        self.store._nicknames[uid] = nick
                self.store._group_members[f"group_{group_id}"] = members
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
            simplified = self.store.add(data)
            if simplified:
                await self._broadcast({"type": "new_message", "data": simplified})

    async def send_message(self, chat_id, text):
        if not self.ws:
            raise RuntimeError("not connected to NapCat")
        message = self._parse_message(text)
        if chat_id.startswith("group_"):
            group_id = int(chat_id.split("_", 1)[1])
            return await self._request("send_group_msg", {"group_id": group_id, "message": message})
        elif chat_id.startswith("private_"):
            user_id = int(chat_id.split("_", 1)[1])
            return await self._request("send_private_msg", {"user_id": user_id, "message": message})
        else:
            raise ValueError(f"unknown chat_id: {chat_id}")

    @staticmethod
    def _parse_message(text):
        import re
        parts = re.split(r"@\[(\d+)\]", text)
        if len(parts) == 1:
            return text
        result = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                if part:
                    result.append({"type": "text", "data": {"text": part}})
            else:
                result.append({"type": "at", "data": {"qq": part}})
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


async def handle_send(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = body.get("chat_id")
    text = body.get("text")
    if not isinstance(chat_id, str) or not chat_id:
        return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
    if not isinstance(text, str):
        return web.json_response({"ok": False, "error": "text is required"}, status=400)
    if chat_id.startswith("group_"):
        chat_num = chat_id.split("_", 1)[1]
    elif chat_id.startswith("private_"):
        chat_num = chat_id.split("_", 1)[1]
    else:
        return web.json_response({"ok": False, "error": "unknown chat_id"}, status=400)
    if not chat_num.isdigit():
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    napcat = request.app["napcat"]
    store = request.app["store"]
    try:
        result = await napcat.send_message(chat_id, text)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "send failed")) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)
        now = int(time.time())
        simplified = {
            "message_id": (result or {}).get("message_id"),
            "time": now,
            "sender_id": "self",
            "sender_name": "You",
            "content": text,
            "chat_id": chat_id,
            "type": chat_id.startswith("group_") and "group" or "private",
            "chat_name": "",
            "self": True,
        }
        store._data[chat_id].append(simplified)
        if chat_id in store._chat_meta:
            store._chat_meta[chat_id]["last_time"] = now
            store._chat_meta[chat_id]["last_text"] = text[:50]
        store._dirty.add(chat_id)
        await napcat._broadcast({"type": "new_message", "data": simplified})
        return web.json_response({"ok": True, "data": result})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)


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


async def main():
    config = load_config()
    store = MessageStore(maxlen=MAX_MESSAGES)
    store.load_all()
    napcat = NapCatConnection(config["ws_url"], config.get("napcat_token", ""), store)

    app = web.Application()
    app["config"] = config
    app["store"] = store
    app["napcat"] = napcat

    app.router.add_post("/api/login", handle_login)
    app.router.add_get("/api/chats", handle_chats)
    app.router.add_get("/api/messages", handle_messages)
    app.router.add_post("/api/send", handle_send)
    app.router.add_get("/api/status", handle_status)
    app.router.add_get("/api/nicknames", handle_nicknames)
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
