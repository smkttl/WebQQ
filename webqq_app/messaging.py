from .common import *

MAX_FORWARD_NODES = 100
MAX_FORWARD_SENDER_NAME = 100
MAX_FORWARD_NODE_CONTENT = 20_000
MAX_FORWARD_TOTAL_CONTENT = 100_000

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


def normalize_forward_nodes(nodes):
    if not isinstance(nodes, list):
        raise ValueError("nodes must be a list")
    if not 1 <= len(nodes) <= MAX_FORWARD_NODES:
        raise ValueError(f"combined forwards require 1 to {MAX_FORWARD_NODES} nodes")
    normalized = []
    total_content = 0
    for index, node in enumerate(nodes, 1):
        if not isinstance(node, dict):
            raise ValueError(f"forward node {index} must be an object")
        message_id = str(node.get("message_id") or "").strip()
        has_custom_fields = any(key in node for key in ("sender_id", "sender_name", "content"))
        if message_id:
            if has_custom_fields:
                raise ValueError(f"forward node {index} cannot mix a message reference with custom content")
            if not message_id.isdigit() or int(message_id) <= 0:
                raise ValueError(f"forward node {index} has an invalid message_id")
            normalized.append({"message_id": message_id})
            continue
        sender_id = str(node.get("sender_id") or "").strip()
        sender_name = node.get("sender_name")
        content = node.get("content")
        if not sender_id.isdigit() or int(sender_id) <= 0 or len(sender_id) > 15:
            raise ValueError(f"forward node {index} has an invalid sender_id")
        if not isinstance(sender_name, str) or not sender_name.strip():
            raise ValueError(f"forward node {index} requires sender_name")
        sender_name = sender_name.strip()
        if len(sender_name) > MAX_FORWARD_SENDER_NAME:
            raise ValueError(f"forward node {index} sender_name is too long")
        if not isinstance(content, str) or not content.strip():
            raise ValueError(f"forward node {index} requires content")
        if len(content) > MAX_FORWARD_NODE_CONTENT:
            raise ValueError(f"forward node {index} content is too long")
        total_content += len(content)
        if total_content > MAX_FORWARD_TOTAL_CONTENT:
            raise ValueError("combined forward content is too long")
        normalized.append({
            "sender_id": sender_id,
            "sender_name": sender_name,
            "content": content,
        })
    return normalized


def _local_forward_node(store, napcat, node, now):
    if "message_id" in node:
        found = store.find_message(node["message_id"])
        if found:
            message = found["message"]
            return {
                key: message.get(key)
                for key in (
                    "sender_id", "sender_name", "time", "content", "mentions", "images",
                    "forwards", "files", "videos", "records", "extra_segments",
                )
            }
        return {
            "sender_id": "",
            "sender_name": "Message {}".format(node["message_id"]),
            "time": now,
            "content": "[message unavailable locally]",
            "mentions": {},
            "images": [],
            "forwards": [],
            "files": [],
            "videos": [],
            "records": [],
            "extra_segments": [],
        }
    return store._simplify_forward_node({
        "user_id": node["sender_id"],
        "nickname": node["sender_name"],
        "time": now,
        "message": napcat._parse_message(node["content"]),
    })


def make_local_self_forward_message(store, napcat, parsed_chat, chat_id, nodes, result, source="user"):
    message_id = extract_message_id(result)
    data = result.get("data") if isinstance(result, dict) and isinstance(result.get("data"), dict) else {}
    forward_id = data.get("forward_id") or data.get("res_id") or ""
    now = int(time.time())
    message = make_local_self_message(
        store,
        parsed_chat,
        chat_id,
        "[forward]",
        message_id=message_id,
        source=source,
    )
    message["time"] = now
    message["forwards"] = [{
        "id": str(forward_id),
        "title": "Forwarded messages",
        "summary": "{} messages".format(len(nodes)),
        "status": "ok",
        "nodes": [_local_forward_node(store, napcat, node, now) for node in nodes],
    }]
    return message


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
        simplified["pending"] = False
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
                "patch": {"pending": False},
            },
        })
    else:
        existing = store.find_message(message_id, chat_id=chat_id) if message_id is not None else None
        if existing:
            return {"result": result, "message": existing["message"]}
        simplified = make_local_self_message(store, parsed_chat, chat_id, text, reply_to=reply_to, message_id=message_id, source=source)
        store.register_pending_local_message(chat_id, simplified)
        if message_id is not None:
            simplified["pending"] = False
        update_chat_after_local_send(store, chat_id, parsed_chat, text, now=simplified["time"])
        await napcat._broadcast({"type": "new_message", "data": simplified})
        dispatch_plugin_message_later(napcat, simplified, raw=None)
    return {"result": result, "message": simplified}


async def send_forward_and_register(napcat, store, chat_id, nodes, source="user"):
    upstream_chat_id = chat_id
    chat_id = canonical_chat_id(chat_id)
    parsed_chat = parse_chat_id(chat_id)
    if not parsed_chat:
        raise ValueError("invalid chat_id")
    nodes = normalize_forward_nodes(nodes)
    result = await napcat.send_forward(upstream_chat_id, nodes)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "send failed")) if result else "not connected"
        raise RuntimeError(err)
    message_id = extract_message_id(result)
    existing = store.find_message(message_id, chat_id=chat_id) if message_id is not None else None
    if existing:
        return {"result": result, "message": existing["message"]}
    simplified = make_local_self_forward_message(
        store, napcat, parsed_chat, chat_id, nodes, result, source=source,
    )
    store.register_pending_local_message(chat_id, simplified)
    if message_id is not None:
        simplified["pending"] = False
    update_chat_after_local_send(store, chat_id, parsed_chat, "[forward]", now=simplified["time"])
    await napcat._broadcast({"type": "new_message", "data": simplified})
    dispatch_plugin_message_later(napcat, simplified, raw=None)
    return {"result": result, "message": simplified}
