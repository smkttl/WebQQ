import json
from pathlib import Path
from typing import Dict


EMOJI_NAMES_PATH = Path(__file__).with_name("emoji_names.json")


def load_emoji_names(path: Path = EMOJI_NAMES_PATH) -> Dict[str, str]:
    try:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return {}
    names = payload.get("names") if isinstance(payload, dict) else None
    if not isinstance(names, dict):
        return {}
    return {
        str(emoji_id): str(name)
        for emoji_id, name in names.items()
        if str(emoji_id).isdigit() and isinstance(name, str) and name.strip()
    }


EMOJI_NAMES = load_emoji_names()


def explain_emoji(emoji_id: str) -> str:
    emoji_id = str(emoji_id)
    name = EMOJI_NAMES.get(emoji_id)
    return "[face:{}{}]".format(emoji_id, " " + name if name else "")
