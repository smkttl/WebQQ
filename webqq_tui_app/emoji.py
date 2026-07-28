import json
from pathlib import Path
from typing import Dict, List, Tuple


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

REACTION_EMOJI_IDS = tuple(str(value) for value in (
    4, 5, 8, 9, 10, 12, 14, 16, 21, 23, 24, 25, 26, 27, 28, 29, 30, 32,
    33, 34, 38, 39, 41, 42, 43, 49, 53, 60, 63, 66, 74, 75, 76, 78, 79,
    85, 89, 96, 97, 98, 99, 100, 101, 102, 103, 104, 106, 109, 111, 116,
    118, 120, 122, 123, 124, 125, 129, 144, 147, 171, 173, 174, 175, 176,
    179, 180, 181, 182, 183, 201, 203, 212, 214, 219, 222, 227, 232, 240,
    243, 246, 262, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 277,
    278, 281, 282, 284, 285, 287, 289, 290, 293, 294, 297, 298, 299, 305,
    306, 307, 314, 315, 318, 319, 320, 322, 324, 326,
))


def explain_emoji(emoji_id: str) -> str:
    emoji_id = str(emoji_id)
    name = EMOJI_NAMES.get(emoji_id)
    return "[face:{}{}]".format(emoji_id, " " + name if name else "")


def reaction_emoji_entries() -> List[Tuple[str, str]]:
    return [(emoji_id, EMOJI_NAMES.get(emoji_id, emoji_id)) for emoji_id in REACTION_EMOJI_IDS]
