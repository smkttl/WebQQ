from .common import *
from .messaging import send_text_and_register

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
