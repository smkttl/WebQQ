from aiohttp import web
import asyncio

from .common import *
from .auth import BanTracker, ban_middleware
from .api import *
from .store import MessageStore
from .plugins import PluginManager
from .napcat import NapCatConnection


def configured_web_port(config):
    raw = (
        os.environ.get("WEBQQ_PORT")
        or os.environ.get("PORT")
        or os.environ.get("WEB_PORT")
        or config.get("web_port", 8080)
    )
    try:
        port = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"invalid web port: {raw!r}")
    if not 1 <= port <= 65535:
        raise ValueError(f"web port out of range: {port}")
    return port


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

    port = configured_web_port(config)
    print(f"WebQQ running at http://localhost:{port}")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    await asyncio.Event().wait()
