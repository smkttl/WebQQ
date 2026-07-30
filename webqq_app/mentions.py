import re


MENTION_PATTERN = r"@\[(\d+)\](?:\(([^)\r\n]*)\))?"
MENTION_RE = re.compile(MENTION_PATTERN)


def format_mentions_for_agent(text, mentions):
    """Add display names to mention tokens exposed to an agent."""
    content = str(text or "")
    if not isinstance(mentions, dict) or not mentions:
        return content

    names = {str(user_id): _safe_name(name) for user_id, name in mentions.items()}

    def replace(match):
        user_id = match.group(1)
        name = names.get(user_id)
        return f"@[{user_id}]({name})" if name else match.group(0)

    return MENTION_RE.sub(replace, content)


def _safe_name(value):
    name = " ".join(str(value or "").split())
    return name.replace("(", "（").replace(")", "）")
