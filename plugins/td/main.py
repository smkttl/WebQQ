import re


ALL_MENTION_RE = re.compile(r"@\[all\]", re.IGNORECASE)


def _mentions_all(message):
    mentions = message.get("mentions") or {}
    if isinstance(mentions, dict) and any(
        str(user_id).casefold() == "all" for user_id in mentions
    ):
        return True
    return bool(ALL_MENTION_RE.search(str(message.get("content") or "")))


async def handle_event(event, ctx):
    if event.get("type") != "message":
        return
    message = event.get("message") or {}
    if message.get("self") or str(message.get("source") or "").startswith("plugin:"):
        return
    chat_id = str(message.get("chat_id") or "")
    if message.get("type") != "group" and not chat_id.startswith("group_"):
        return
    if not _mentions_all(message):
        return
    try:
        await ctx.send_message(chat_id, "td")
    except Exception as error:
        ctx.log("send failed: {}".format(error))
