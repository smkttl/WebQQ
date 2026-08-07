from .common import *
import hashlib
import imghdr
import shutil
from .auth import check_auth, record_auth_failure, client_ip, read_json_body
from .messaging import send_forward_and_register, send_text_and_register

QZONE_UGC_RIGHTS = {1, 4, 16, 64, 128}
QZONE_MAX_IMAGES = 9
BACKGROUND_UPLOAD_PREFIX = "web_background_image"
BACKGROUND_UPLOAD_LIMIT = 100 * 1024 * 1024
BACKGROUND_IMAGE_EXTENSIONS = {
    "jpeg": ".jpg",
    "png": ".png",
    "gif": ".gif",
    "webp": ".webp",
    "bmp": ".bmp",
}
MUSIC_PLATFORMS = {"qq", "163", "kugou", "migu", "kuwo"}


def _qzone_error(result, fallback):
    if not result:
        return "not connected"
    if not isinstance(result, dict):
        return str(result)
    return result.get("wording", result.get("message", fallback))


def _parse_qzone_right(value):
    try:
        right = int(value)
    except (TypeError, ValueError):
        raise ValueError("ugc_right must be one of 1, 4, 16, 64, or 128")
    if right not in QZONE_UGC_RIGHTS:
        raise ValueError("ugc_right must be one of 1, 4, 16, 64, or 128")
    return right


def _parse_qzone_targets(value):
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            raise ValueError("target_uins must be a JSON array")
    if not isinstance(value, list):
        raise ValueError("target_uins must be an array")
    targets = []
    for uid in value:
        if isinstance(uid, str) and uid.lstrip().startswith("["):
            try:
                nested = json.loads(uid)
            except (TypeError, ValueError):
                raise ValueError("target_uins must be a JSON array")
            if not isinstance(nested, list):
                raise ValueError("target_uins must be an array")
            targets.extend(_parse_qzone_targets(nested))
            continue
        text = str(uid).strip()
        if not text or not text.isdigit():
            raise ValueError("target_uins must contain numeric QQ ids")
        targets.append(text)
    return targets


def _validate_qzone_post(content, images, ugc_right, target_uins):
    if not isinstance(content, str):
        raise ValueError("content must be a string")
    if not content.strip() and not images:
        raise ValueError("content or at least one image is required")
    if len(images) > QZONE_MAX_IMAGES:
        raise ValueError("a maximum of 9 images is allowed")
    if ugc_right in (16, 128) and not target_uins:
        raise ValueError("target_uins is required for this ugc_right")


async def handle_qzone_post(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    temp_paths = []
    images = []
    try:
        content = ""
        raw_right = 1
        targets_value = []
        if getattr(request, "content_type", "") == "multipart/form-data":
            reader = await request.multipart()
            while True:
                part = await reader.next()
                if part is None:
                    break
                if part.name == "content":
                    content = await part.text()
                elif part.name == "ugc_right":
                    raw_right = await part.text()
                elif part.name == "target_uins":
                    targets_value.append(await part.text())
                elif part.name == "images":
                    if len(images) >= QZONE_MAX_IMAGES:
                        await part.release()
                        return web.json_response({"ok": False, "error": "a maximum of 9 images is allowed"}, status=400)
                    fd, path = tempfile.mkstemp(prefix="webqq-qzone-")
                    size = 0
                    try:
                        with os.fdopen(fd, "wb") as image_file:
                            while True:
                                chunk = await part.read_chunk(size=1024 * 1024)
                                if not chunk:
                                    break
                                size += len(chunk)
                                if size > MAX_FILE_UPLOAD:
                                    continue
                                image_file.write(chunk)
                    finally:
                        temp_paths.append(path)
                    if size > MAX_FILE_UPLOAD:
                        return web.json_response({"ok": False, "error": "image is larger than 100 MB"}, status=413)
                    if size <= 0:
                        return web.json_response({"ok": False, "error": "image is empty"}, status=400)
                    images.append(Path(path).resolve().as_uri())
                else:
                    await part.release()
            if len(targets_value) == 1:
                targets_value = targets_value[0]
        else:
            body = await read_json_body(request)
            content = body.get("content", "")
            raw_right = body.get("ugc_right", 1)
            targets_value = body.get("target_uins", [])
            supplied_images = body.get("images", [])
            if supplied_images:
                if not isinstance(supplied_images, list):
                    raise ValueError("images must be an array")
                images = [str(image).strip() for image in supplied_images if str(image).strip()]
        right = _parse_qzone_right(raw_right)
        targets = _parse_qzone_targets(targets_value)
        _validate_qzone_post(content, images, right, targets)
        result = await request.app["napcat"].send_qzone_post(content, images, right, targets)
        if not result or result.get("status") != "ok":
            return web.json_response({"ok": False, "error": _qzone_error(result, "Qzone post failed")}, status=500)
        data = result.get("data")
        tid = data.get("tid") if isinstance(data, dict) else result.get("tid")
        if not tid:
            return web.json_response({"ok": False, "error": "Qzone response did not include tid"}, status=500)
        return web.json_response({"ok": True, "tid": str(tid)})
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    finally:
        for path in temp_paths:
            try:
                os.unlink(path)
            except OSError:
                pass


async def handle_delete_qzone_post(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    tid = str(request.match_info.get("tid", "")).strip()
    if not tid:
        return web.json_response({"ok": False, "error": "tid is required"}, status=400)
    try:
        result = await request.app["napcat"].delete_qzone_post(tid)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        return web.json_response({"ok": False, "error": _qzone_error(result, "Qzone deletion failed")}, status=500)
    return web.json_response({"ok": True})

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


async def handle_send_forward(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = body.get("chat_id")
    if not isinstance(chat_id, str) or not chat_id:
        return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
    try:
        sent = await send_forward_and_register(
            request.app["napcat"], request.app["store"], chat_id, body.get("nodes"),
        )
        return web.json_response({"ok": True, "data": sent["result"]})
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)


async def handle_poke(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = body.get("chat_id")
    user_id = str(body.get("user_id", "")).strip()
    parsed_chat = parse_chat_id(chat_id)
    if not parsed_chat:
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    if not user_id.isdigit():
        return web.json_response({"ok": False, "error": "user_id is required"}, status=400)

    group_id = None
    if parsed_chat["type"] == "group":
        group_id = parsed_chat["group_id"]
    elif parsed_chat["type"] == "private":
        if user_id != str(parsed_chat["private_id"]):
            return web.json_response({"ok": False, "error": "user_id does not match private chat"}, status=400)
    else:
        if user_id != str(parsed_chat["user_id"]):
            return web.json_response({"ok": False, "error": "user_id does not match temporary chat"}, status=400)
        group_id = parsed_chat["group_id"]

    self_id = str(request.app["store"]._self_user.get("user_id") or "")
    if user_id == self_id:
        return web.json_response({"ok": False, "error": "cannot poke yourself"}, status=400)
    try:
        result = await request.app["napcat"].send_poke(user_id, group_id=group_id)
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "poke failed")) if result else "not connected"
        return web.json_response({"ok": False, "error": err}, status=500)
    data = result.get("data")
    if isinstance(data, dict) and data.get("result") is False:
        err = data.get("errMsg") or data.get("message") or result.get("wording") or "poke was not accepted by QQ"
        return web.json_response({"ok": False, "error": err}, status=500)
    return web.json_response({"ok": True, "data": data})


async def handle_forward(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    forward_id = str(request.query.get("id", "")).strip()
    if not forward_id or len(forward_id) > 256:
        return web.json_response({"ok": False, "error": "forward id is required"}, status=400)
    try:
        result = await request.app["napcat"].fetch_forward(forward_id)
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "forward unavailable")) if result else "not connected"
        return web.json_response({"ok": False, "error": err}, status=500)
    forward = request.app["store"]._simplify_forward_segment({
        "id": forward_id,
        "content": result.get("data"),
    })
    if not forward.get("nodes"):
        return web.json_response({"ok": False, "error": "forward content is empty"}, status=500)
    request.app["store"].remember_forward(forward_id, forward)
    return web.json_response({"ok": True, "forward": forward})


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


async def handle_send_image(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = ""
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
                fd, temp_path = tempfile.mkstemp(prefix="webqq-image-")
                with os.fdopen(fd, "wb") as image_file:
                    while True:
                        chunk = await part.read_chunk(size=1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > MAX_FILE_UPLOAD:
                            too_large = True
                        else:
                            image_file.write(chunk)
            else:
                await part.release()
        if not chat_id:
            return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
        if not parse_chat_id(chat_id):
            return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
        if not temp_path:
            return web.json_response({"ok": False, "error": "image is required"}, status=400)
        if too_large:
            return web.json_response({"ok": False, "error": "image is larger than 100 MB"}, status=413)
        if size <= 0:
            return web.json_response({"ok": False, "error": "image is empty"}, status=400)
        result = await request.app["napcat"].send_image(chat_id, temp_path)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "image send failed")) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)
        return web.json_response({"ok": True, "data": result})
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def _handle_send_media_upload(request, kind):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = ""
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
                fd, temp_path = tempfile.mkstemp(prefix="webqq-{}-".format(kind))
                with os.fdopen(fd, "wb") as media_file:
                    while True:
                        chunk = await part.read_chunk(size=1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > MAX_FILE_UPLOAD:
                            too_large = True
                        else:
                            media_file.write(chunk)
            else:
                await part.release()
        if not chat_id:
            return web.json_response({"ok": False, "error": "chat_id is required"}, status=400)
        if not parse_chat_id(chat_id):
            return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
        if not temp_path:
            return web.json_response({"ok": False, "error": "{} is required".format(kind)}, status=400)
        if too_large:
            return web.json_response({"ok": False, "error": "{} is larger than 100 MB".format(kind)}, status=413)
        if size <= 0:
            return web.json_response({"ok": False, "error": "{} is empty".format(kind)}, status=400)
        sender = getattr(request.app["napcat"], "send_{}".format(kind))
        result = await sender(chat_id, temp_path)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "{} send failed".format(kind))) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)
        return web.json_response({"ok": True, "data": result.get("data")})
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def handle_send_video(request):
    return await _handle_send_media_upload(request, "video")


async def handle_send_voice(request):
    return await _handle_send_media_upload(request, "voice")


async def handle_send_contact(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = str(body.get("chat_id", "")).strip()
    contact_type = str(body.get("type", "")).strip().lower()
    contact_id = str(body.get("id", "")).strip()
    if not parse_chat_id(chat_id):
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    if contact_type not in {"qq", "group"}:
        return web.json_response({"ok": False, "error": "contact type must be qq or group"}, status=400)
    if not contact_id.isdigit():
        return web.json_response({"ok": False, "error": "contact id must be numeric"}, status=400)
    try:
        result = await request.app["napcat"].send_contact(chat_id, contact_type, contact_id)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        return web.json_response({"ok": False, "error": _qzone_error(result, "contact send failed")}, status=500)
    return web.json_response({"ok": True, "data": result.get("data")})


async def handle_send_music(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    chat_id = str(body.get("chat_id", "")).strip()
    music_type = str(body.get("type", "")).strip().lower()
    if not parse_chat_id(chat_id):
        return web.json_response({"ok": False, "error": "invalid chat_id"}, status=400)
    if music_type in MUSIC_PLATFORMS:
        music_id = str(body.get("id", "")).strip()
        if not music_id:
            return web.json_response({"ok": False, "error": "music id is required"}, status=400)
        music = {"type": music_type, "id": music_id}
    elif music_type == "custom":
        music = {
            "type": "custom",
            "url": str(body.get("url", "")).strip(),
            "audio": str(body.get("audio", "")).strip(),
            "title": str(body.get("title", "")).strip(),
            "image": str(body.get("image", "")).strip(),
            "content": str(body.get("content", "")).strip(),
        }
        if not music["url"] or not music["audio"] or not music["title"]:
            return web.json_response({"ok": False, "error": "custom music requires url, audio, and title"}, status=400)
    else:
        return web.json_response({"ok": False, "error": "unsupported music type"}, status=400)
    try:
        result = await request.app["napcat"].send_music(chat_id, music)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        return web.json_response({"ok": False, "error": _qzone_error(result, "music send failed")}, status=500)
    return web.json_response({"ok": True, "data": result.get("data")})


async def handle_message_transcribe(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await read_json_body(request)
    message_id = str(body.get("message_id", "")).strip()
    chat_id = canonical_chat_id(str(body.get("chat_id", "")).strip())
    if not is_int_string(message_id):
        return web.json_response({"ok": False, "error": "message_id is required"}, status=400)
    store = request.app["store"]
    found = store.find_message(message_id, chat_id=chat_id if parse_chat_id(chat_id) else None)
    if not found:
        return web.json_response({"ok": False, "error": "message is not in local history"}, status=404)
    records = found["message"].get("records")
    if not isinstance(records, list) or not records or not isinstance(records[0], dict):
        return web.json_response({"ok": False, "error": "message does not contain voice"}, status=400)
    transcript = str(records[0].get("transcript") or "").strip()
    if not transcript:
        try:
            result = await request.app["napcat"].fetch_ptt_text(message_id)
        except Exception as error:
            return web.json_response({"ok": False, "error": str(error)}, status=500)
        if not result or result.get("status") != "ok":
            err = result.get("wording", result.get("message", "voice transcription failed")) if result else "not connected"
            return web.json_response({"ok": False, "error": err}, status=500)
        data = result.get("data")
        transcript = str(data.get("text") if isinstance(data, dict) else data or "").strip()
        if not transcript:
            return web.json_response({"ok": False, "error": "voice transcription returned no text"}, status=500)
        found = store.set_voice_transcript(message_id, transcript, chat_id=found["chat_id"])
        store.flush(found["chat_id"])
        payload = {
            "chat_id": found["chat_id"],
            "message_id": message_id,
            "message": found["message"],
            "patch": {"records": found["message"]["records"]},
        }
        await request.app["napcat"]._broadcast({"type": "message_update", "data": payload})
    else:
        payload = {"chat_id": found["chat_id"], "message_id": message_id, "message": found["message"]}
    return web.json_response({"ok": True, "transcript": transcript, **payload})


def _group_file_group_id(chat_id):
    parsed = parse_chat_id(str(chat_id or "").strip())
    if not parsed or parsed["type"] != "group":
        raise ValueError("group chat_id is required")
    return parsed["group_id"]


def _group_file_error(result, fallback):
    if not result:
        return "not connected"
    return str(result.get("wording") or result.get("message") or fallback)


async def handle_group_files(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        group_id = _group_file_group_id(request.query.get("chat_id"))
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    folder_id = str(request.query.get("folder_id", "")).strip()
    if len(folder_id) > 2048:
        return web.json_response({"ok": False, "error": "folder_id is too long"}, status=400)
    try:
        listing, info, packet_available = await request.app["napcat"].group_files(group_id, folder_id)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not listing or listing.get("status") != "ok":
        return web.json_response({"ok": False, "error": _group_file_error(listing, "group files unavailable")}, status=500)
    data = listing.get("data") if isinstance(listing.get("data"), dict) else {}
    info_data = info.get("data") if info and info.get("status") == "ok" and isinstance(info.get("data"), dict) else {}
    return web.json_response({
        "ok": True,
        "folder_id": folder_id,
        "files": data.get("files") if isinstance(data.get("files"), list) else [],
        "folders": data.get("folders") if isinstance(data.get("folders"), list) else [],
        "info": info_data,
        "packet_available": packet_available,
    })


async def handle_group_file_upload(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    chat_id = ""
    folder_id = ""
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
            elif part.name == "folder_id":
                folder_id = (await part.text()).strip()
            elif part.name == "file":
                filename = safe_download_name(part.filename or "file")
                fd, temp_path = tempfile.mkstemp(prefix="webqq-group-file-")
                with os.fdopen(fd, "wb") as output:
                    while True:
                        chunk = await part.read_chunk(size=1024 * 1024)
                        if not chunk:
                            break
                        size += len(chunk)
                        if size > MAX_FILE_UPLOAD:
                            too_large = True
                        else:
                            output.write(chunk)
            else:
                await part.release()
        group_id = _group_file_group_id(chat_id)
        if not temp_path:
            return web.json_response({"ok": False, "error": "file is required"}, status=400)
        if too_large:
            return web.json_response({"ok": False, "error": "file is larger than 100 MB"}, status=413)
        if size <= 0:
            return web.json_response({"ok": False, "error": "file is empty"}, status=400)
        result = await request.app["napcat"].upload_group_file(group_id, temp_path, filename, folder_id)
        if not result or result.get("status") != "ok":
            return web.json_response({"ok": False, "error": _group_file_error(result, "group file upload failed")}, status=500)
        return web.json_response({"ok": True, "data": result.get("data")})
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def _group_file_json(request):
    body = await read_json_body(request)
    group_id = _group_file_group_id(body.get("chat_id"))
    return body, group_id


async def handle_group_file_folder_create(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        body, group_id = await _group_file_json(request)
        name = str(body.get("name", "")).strip()
        if not name or len(name) > 255:
            raise ValueError("folder name is required and must be at most 255 characters")
        result = await request.app["napcat"].create_group_file_folder(group_id, name)
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        return web.json_response({"ok": False, "error": _group_file_error(result, "folder creation failed")}, status=500)
    return web.json_response({"ok": True, "data": result.get("data")})


async def handle_group_file_delete(request):
    return await _handle_group_file_mutation(request, "file_id", "delete_group_file", "file deletion failed")


async def handle_group_file_folder_delete(request):
    return await _handle_group_file_mutation(request, "folder_id", "delete_group_file_folder", "folder deletion failed")


async def _handle_group_file_mutation(request, id_field, method_name, fallback):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        body, group_id = await _group_file_json(request)
        item_id = str(body.get(id_field, "")).strip()
        if not item_id or len(item_id) > 2048:
            raise ValueError("{} is required".format(id_field))
        result = await getattr(request.app["napcat"], method_name)(group_id, item_id)
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        return web.json_response({"ok": False, "error": _group_file_error(result, fallback)}, status=500)
    return web.json_response({"ok": True, "data": result.get("data")})


async def handle_group_file_rename(request):
    return await _handle_group_file_relocate(request, rename=True)


async def handle_group_file_move(request):
    return await _handle_group_file_relocate(request, rename=False)


async def _handle_group_file_relocate(request, rename):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    try:
        body, group_id = await _group_file_json(request)
        file_id = str(body.get("file_id", "")).strip()
        parent_id = str(body.get("parent_id", "")).strip() or "/"
        value_field = "new_name" if rename else "target_id"
        value = str(body.get(value_field, "")).strip()
        if not file_id or len(file_id) > 2048:
            raise ValueError("file_id is required")
        if rename:
            if not value or len(value) > 255:
                raise ValueError("new_name is required and must be at most 255 characters")
            result = await request.app["napcat"].rename_group_file(group_id, file_id, parent_id, value)
        else:
            result = await request.app["napcat"].move_group_file(group_id, file_id, parent_id, value or "/")
    except ValueError as error:
        return web.json_response({"ok": False, "error": str(error)}, status=400)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        status = 503 if result and "packet" in _group_file_error(result, "").lower() else 500
        return web.json_response({"ok": False, "error": _group_file_error(result, "file operation failed")}, status=status)
    return web.json_response({"ok": True, "data": result.get("data")})


async def handle_friend_remark(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    user_id = str(request.match_info.get("user_id", "")).strip()
    if not user_id.isdigit():
        return web.json_response({"ok": False, "error": "user_id must be numeric"}, status=400)
    body = await read_json_body(request)
    remark = str(body.get("remark", "")).strip()
    if len(remark) > 128:
        return web.json_response({"ok": False, "error": "remark must be at most 128 characters"}, status=400)
    napcat = request.app["napcat"]
    try:
        result = await napcat.set_friend_remark(user_id, remark)
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    if not result or result.get("status") != "ok":
        err = result.get("wording", result.get("message", "friend remark update failed")) if result else "not connected"
        return web.json_response({"ok": False, "error": err}, status=500)
    friend = await napcat.get_friend(user_id)
    friend = friend if isinstance(friend, dict) else {}
    nickname = str(friend.get("nickname") or friend.get("nick_name") or "")
    confirmed_remark = str(friend.get("remark") if "remark" in friend else remark or "")
    name = confirmed_remark or nickname or user_id
    chat = request.app["store"].set_private_display_name(
        user_id, name, nickname=nickname, remark=confirmed_remark,
    )
    return web.json_response({
        "ok": True, "user_id": user_id, "remark": confirmed_remark,
        "nickname": nickname, "name": name, "chat": chat,
    })


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
    store = request.app["store"]
    parsed_chat = parse_chat_id(found["chat_id"])
    if message.get("self"):
        allowed = True
    elif parsed_chat and parsed_chat["type"] == "group":
        allowed = store.current_group_role(parsed_chat["group_id"]) in ("owner", "admin")
    else:
        allowed = False
    if not allowed:
        return web.json_response({"ok": False, "error": "insufficient permission to revoke this message"}, status=400)
    if message.get("self"):
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
    recalled = store.mark_recalled(
        message_id,
        chat_id=chat_id if parse_chat_id(chat_id) else None,
        operator_id=store._self_user.get("user_id"),
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
    source = str(request.app["config"].get("web_background_image") or "").strip()
    return web.json_response({
        "napcat_connected": napcat.ws is not None,
        "chats_count": len(request.app["store"]._data),
        "self_user": dict(request.app["store"]._self_user),
        "web_background_image": bool(source),
        "web_background_revision": _background_revision(source),
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


def _background_data_dir():
    directory = ROOT_DIR / "data"
    directory.mkdir(parents=True, exist_ok=True)
    return directory.resolve()


def _managed_background_files():
    directory = _background_data_dir()
    return [path for path in directory.glob(f"{BACKGROUND_UPLOAD_PREFIX}.*") if path.is_file()]


def _managed_background_source(source):
    if not source:
        return None
    parsed = urlparse(str(source))
    if parsed.scheme:
        return None
    path = Path(source).expanduser()
    if not path.is_absolute():
        path = ROOT_DIR / path
    path = path.resolve()
    directory = _background_data_dir()
    if path.parent == directory and path.name.startswith(BACKGROUND_UPLOAD_PREFIX + "."):
        return path
    return None


def _background_revision(source):
    path = _managed_background_source(source)
    if path:
        try:
            return str(path.stat().st_mtime_ns)
        except OSError:
            return "missing"
    if source:
        return hashlib.sha256(str(source).encode("utf-8")).hexdigest()[:16]
    return ""


async def handle_background_image_upload(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    temp_path = None
    try:
        if getattr(request, "content_type", "") != "multipart/form-data":
            return web.json_response({"ok": False, "error": "multipart/form-data is required"}, status=400)
        reader = await request.multipart()
        file_seen = False
        size = 0
        data_dir = _background_data_dir()
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name != "file":
                await part.release()
                continue
            if file_seen:
                await part.release()
                continue
            file_seen = True
            fd, temp_path = tempfile.mkstemp(prefix="webqq-background-", dir=str(data_dir))
            with os.fdopen(fd, "wb") as image_file:
                while True:
                    chunk = await part.read_chunk(size=1024 * 1024)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size <= BACKGROUND_UPLOAD_LIMIT:
                        image_file.write(chunk)
            if size > BACKGROUND_UPLOAD_LIMIT:
                return web.json_response({"ok": False, "error": "background image is larger than 100 MB"}, status=413)
            break
        if not file_seen or not temp_path:
            return web.json_response({"ok": False, "error": "file is required"}, status=400)
        if size <= 0:
            return web.json_response({"ok": False, "error": "background image is empty"}, status=400)
        image_type = imghdr.what(temp_path)
        extension = BACKGROUND_IMAGE_EXTENSIONS.get(image_type)
        if not extension:
            return web.json_response({"ok": False, "error": "file is not a supported image"}, status=400)

        target = data_dir / f"{BACKGROUND_UPLOAD_PREFIX}{extension}"
        backup = None
        if target.exists():
            backup = data_dir / f".{BACKGROUND_UPLOAD_PREFIX}-backup-{uuid.uuid4().hex}"
            shutil.copy2(target, backup)
        config = request.app["config"]
        previous_source = config.get("web_background_image", "")
        try:
            os.replace(temp_path, target)
            temp_path = None
            config["web_background_image"] = str(target.relative_to(ROOT_DIR))
            save_config(config)
        except Exception:
            if target.exists() and backup:
                target.unlink()
            if backup:
                os.replace(backup, target)
            elif target.exists():
                target.unlink()
            config["web_background_image"] = previous_source
            raise
        finally:
            if backup:
                try:
                    backup.unlink()
                except OSError:
                    pass
        for old_path in _managed_background_files():
            if old_path != target:
                try:
                    old_path.unlink()
                except OSError:
                    pass
        return web.json_response({
            "ok": True,
            "web_background_image": True,
            "web_background_revision": _background_revision(config["web_background_image"]),
        })
    except Exception as error:
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    finally:
        if temp_path:
            try:
                os.unlink(temp_path)
            except OSError:
                pass


async def handle_background_image_clear(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    config = request.app["config"]
    previous_source = config.get("web_background_image", "")
    try:
        config["web_background_image"] = ""
        save_config(config)
    except Exception as error:
        config["web_background_image"] = previous_source
        return web.json_response({"ok": False, "error": str(error)}, status=500)
    for path in _managed_background_files():
        try:
            path.unlink()
        except OSError:
            pass
    return web.json_response({"ok": True, "web_background_image": False, "web_background_revision": ""})


async def handle_background_image(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    source = str(request.app["config"].get("web_background_image") or "").strip()
    if not source:
        return web.json_response({"error": "background image is not configured"}, status=404)
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        response = await fetch_first_image([source])
        response.headers["Cache-Control"] = "private, no-cache"
        return response
    if parsed.scheme:
        return web.json_response({"error": "unsupported background image source"}, status=400)
    path = Path(source).expanduser()
    if not path.is_absolute():
        path = ROOT_DIR / path
    path = path.resolve()
    if not path.is_file():
        return web.json_response({"error": "background image not found"}, status=404)
    content_type = mimetypes.guess_type(path.name)[0] or ""
    if not content_type.startswith("image/"):
        return web.json_response({"error": "background file is not an image"}, status=400)
    return web.FileResponse(path, headers={
        "Cache-Control": "private, no-cache",
        "Content-Type": content_type,
    })


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
