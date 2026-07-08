import time


MESSAGE_LIMIT = 1000


def setup(ctx):
    return RecentPlugin(ctx)


class RecentPlugin:
    def __init__(self, ctx):
        self.ctx = ctx

    async def handle_event(self, event, ctx):
        if event.get("type") != "message":
            return
        message = event.get("message") or {}
        if not self._is_group_message(message):
            return
        if str(message.get("content") or "").strip() != self._command():
            return

        chat_id = str(message.get("chat_id") or "")
        messages = list(ctx.get_messages(chat_id, limit=MESSAGE_LIMIT) or [])
        messages = self._with_current_message(messages, message)
        await ctx.send_message(chat_id, self._rank_text(messages))

    def _rank_text(self, messages):
        ranked = self._rank(messages)
        if not ranked:
            return "No group senders in the last 1000 messages."
        lines = [self._title()]
        for index, (sender_id, data) in enumerate(ranked[:self._max_rank()], start=1):
            name = str(data.get("name") or sender_id)
            count = int(data.get("count") or 0)
            lines.append(f"{index}. {name} ({sender_id}): {count}")
        return "\n".join(lines)

    def _rank(self, messages):
        senders = {}
        for message in messages[-MESSAGE_LIMIT:]:
            if not self._countable_group_message(message):
                continue
            sender_id = str(message.get("sender_id") or message.get("user_id") or "").strip()
            if not sender_id:
                continue
            sender_name = str(message.get("sender_name") or sender_id).strip() or sender_id
            message_time = self._message_time(message)
            entry = senders.setdefault(sender_id, {"name": sender_name, "count": 0, "last_seen": 0})
            entry["name"] = sender_name
            entry["count"] = int(entry.get("count") or 0) + 1
            entry["last_seen"] = max(float(entry.get("last_seen") or 0), message_time)
        ranked = list(senders.items())
        ranked.sort(key=lambda item: (-int(item[1].get("count") or 0), -float(item[1].get("last_seen") or 0), item[0]))
        return ranked

    def _with_current_message(self, messages, current):
        current_id = str(current.get("message_id") or "")
        if current_id and any(str(message.get("message_id") or "") == current_id for message in messages):
            return messages
        return messages + [current]

    def _is_group_message(self, message):
        chat_id = str(message.get("chat_id") or "")
        return message.get("type") == "group" and chat_id.startswith("group_")

    def _countable_group_message(self, message):
        return (
            self._is_group_message(message)
            and not message.get("self")
            and not message.get("system")
            and not message.get("recalled")
        )

    def _command(self):
        return str(self.ctx.config.get("command") or "/recent rank").strip() or "/recent rank"

    def _title(self):
        return str(self.ctx.config.get("title") or "Recent senders in the last 1000 messages:")

    def _max_rank(self):
        try:
            value = int(self.ctx.config.get("max_rank", 20))
        except (TypeError, ValueError):
            value = 20
        return max(1, value)

    @staticmethod
    def _message_time(message):
        try:
            return float(message.get("time") or time.time())
        except (TypeError, ValueError):
            return time.time()
