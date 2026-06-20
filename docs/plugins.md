# WebQQ Plugin Guide

Plugins are trusted local Python modules. Install a plugin by copying its folder into `plugins/`, then use the WebQQ **Plugins** panel to refresh, enable, edit config, and restart it.

## Folder Layout

Each plugin lives in its own folder:

```text
plugins/my_plugin/
  plugin.json
  config.json
  main.py
```

`plugin.json` describes the plugin:

```json
{
  "id": "my_plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "description": "Short description shown in the UI.",
  "entry": "main.py",
  "enabled_by_default": false
}
```

Rules:

- The folder name and `plugin.json.id` must match.
- `entry` must point to a Python file inside the plugin folder.
- `config.json` must be a JSON object. WebQQ reads it on plugin load/restart and exposes it as `ctx.config`.
- Plugins run in the WebQQ process and are not sandboxed.

## Minimal Plugin

```python
async def handle_event(event, ctx):
    if event["type"] != "message":
        return
    message = event["message"]
    if message.get("self"):
        return
    if message["content"].startswith("/ping"):
        await ctx.send_message(message["chat_id"], "pong")
```

The bundled `plugins/echo/` plugin is a complete example. Its config is:

```json
{
  "prefix": "/echo",
  "respond_to_self": false
}
```

For the echo plugin, `respond_to_self: false` skips messages sent by the logged-in account. Set `respond_to_self: true` to let the plugin parse `/echo` messages sent by yourself too.

## Entry Points

Use one of these forms in `main.py`.

Simple handler:

```python
async def handle_event(event, ctx):
    ...
```

Setup function:

```python
def setup(ctx):
    return Plugin(ctx)

class Plugin:
    def __init__(self, ctx):
        self.ctx = ctx

    async def handle_event(self, event, ctx):
        ...
```

`handle_event` may be sync or async. Exceptions are caught and shown in the Plugins panel without stopping WebQQ or other plugins.

## Events

Every enabled plugin receives events as dictionaries.

Message event:

```python
{
  "type": "message",
  "message": {
    "message_id": 123,
    "time": 1781950000,
    "sender_id": 10001,
    "sender_name": "Alice",
    "content": "hello",
    "mentions": {},
    "images": [],
    "forwards": [],
    "files": [],
    "videos": [],
    "records": [],
    "extra_segments": [],
    "reactions": [],
    "chat_id": "group_123456",
    "type": "group",
    "group_id": 123456,
    "user_id": 10001,
    "chat_name": "Group name",
    "self": false
  },
  "raw": {}
}
```

Notes:

- `type: "message"` is used for incoming messages and messages sent by the user or plugins.
- Use `message["self"]` to detect messages from the logged-in account.
- Loop avoidance is the plugin's responsibility.
- `chat_id` is one of `group_<group_id>`, `private_<user_id>`, or `temp_<group_id>_<user_id>`.
- `raw` is the original NapCat event payload.

Notice event:

```python
{
  "type": "notice",
  "notice": {},
  "system_message": {},
  "raw": {}
}
```

`system_message` is present only when WebQQ converted the notice into a visible system message.

Request event:

```python
{
  "type": "request",
  "request": {},
  "raw": {}
}
```

## Context API

`ctx` is a `PluginContext` with these attributes and helpers:

```python
ctx.plugin_id              # plugin id string
ctx.config                 # parsed config.json dict
ctx.log("message")         # print a plugin-prefixed log line
```

Messaging and actions:

```python
await ctx.send_message(chat_id, text, reply_to=None)
await ctx.upload_file(chat_id, path, name=None)
await ctx.set_msg_emoji_like(message_id, emoji_id, enabled=True)
await ctx.mark_chat_read(chat_id)
await ctx.fetch_history(chat_id, before_message_id=None, count=50)
```

Local reads:

```python
ctx.get_messages(chat_id, limit=50, before=None)
ctx.get_chats()
```

Raw NapCat escape hatch:

```python
await ctx.napcat("set_group_ban", {
    "group_id": 123456,
    "user_id": 10001,
    "duration": 60
})
```

Raw calls are powerful and plugin authors must follow NapCat's action schemas.

## Managing Plugins

From the web UI:

- Open **Plugins** in the status bar.
- Click **Refresh** after copying a new folder into `plugins/`.
- Enable or disable each plugin independently.
- Edit `config.json`, then save. Saving restarts only that plugin.
- Use **Restart** to reload a plugin after editing code on disk.

HTTP APIs are authenticated with the normal WebQQ token:

| Method | Path                            | Description                     |
| ------ | ------------------------------- | ------------------------------- |
| GET    | `/api/plugins`                  | List plugin status              |
| POST   | `/api/plugins/refresh`          | Rescan `plugins/`               |
| POST   | `/api/plugins/{id}/enable`      | Enable and load a plugin        |
| POST   | `/api/plugins/{id}/disable`     | Disable a plugin                |
| POST   | `/api/plugins/{id}/restart`     | Reload one plugin               |
| GET    | `/api/plugins/{id}/config`      | Read plugin config text         |
| PUT    | `/api/plugins/{id}/config`      | Save config and restart plugin  |

## Operational Notes

- Plugins are trusted code with access to the WebQQ process.
- A disabled plugin stops receiving future events; already-running handler calls are not cancelled.
- A bad `config.json` prevents that plugin from loading and shows the parse error in the UI.
- If a plugin needs third-party Python packages, install them in the same Python environment that runs WebQQ.
