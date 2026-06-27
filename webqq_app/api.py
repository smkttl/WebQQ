from .common import *
from .auth import check_auth, record_auth_failure, client_ip, read_json_body
from .messaging import send_text_and_register

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
    sender_name = str(body.get("sender_name", "")).strip()
    if name.startswith("群临时会话") or name.endswith(" / 临时会话") or name.endswith(" / 群临时会话"):
        name = ""
    if is_placeholder_name(sender_name):
        sender_name = ""
    if not name:
        name = sender_name or user_id
    chat_id = f"private_{user_id}"
    store = request.app["store"]
    store.remember_temp_context(user_id, group_id, group_name)
    display_name = store.resolve_display_name(user_id, group_id=group_id) or name
    current_name = store._chat_meta.get(chat_id, {}).get("name")
    if current_name and current_name != user_id and not is_placeholder_name(current_name):
        display_name = current_name
    store.ensure_chat(chat_id, display_name, "private", user_id=int(user_id), temp_group_id=int(group_id), temp_group_name=group_name)
    return web.json_response({"ok": True, "chat_id": chat_id, "name": display_name})


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
