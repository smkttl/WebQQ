from .common import *

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
