async def handle_event(event, ctx):
    if event.get("type") != "message":
        return
    message = event.get("message") or {}
    if message.get("self") and not ctx.config.get("respond_to_self"):
        return
    content = message.get("content") or ""
    prefix = str(ctx.config.get("prefix") or "/echo")
    if not content.startswith(prefix):
        return
    reply = content[len(prefix):].strip()
    if not reply:
        if not ctx.config.get("echo_empty_payload"):
            return
        reply = content
    await ctx.send_message(message["chat_id"], reply)
