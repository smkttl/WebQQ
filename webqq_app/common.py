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

PACKAGE_DIR = Path(__file__).resolve().parent
ROOT_DIR = PACKAGE_DIR.parent
CONFIG_PATH = ROOT_DIR / "config.json"
STATIC_DIR = ROOT_DIR / "static"
DATA_DIR = ROOT_DIR / "data"
AVATAR_DIR = DATA_DIR / "avatars"
PLUGIN_DIR = ROOT_DIR / "plugins"

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


def is_placeholder_name(value):
    return str(value or "").strip() in ("临时会话", "群临时会话")


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
