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

UNNAMED_REACTION_EMOJI_IDS = {
    "422", "423", "432", "450", "451", "452", "453", "454", "455", "456",
    "457", "458", "459", "460", "461", "462", "463", "464", "465", "466",
    "467", "468", "469", "470", "472", "474", "475", "476", "477", "478",
    "479", "480", "481", "482", "483", "484",
}
REACTION_EMOJI_IDS = tuple(sorted(set(EMOJI_NAMES).union(UNNAMED_REACTION_EMOJI_IDS), key=int))


def explain_emoji(emoji_id: str) -> str:
    emoji_id = str(emoji_id)
    name = EMOJI_NAMES.get(emoji_id)
    return "[face:{}{}]".format(emoji_id, " " + name if name else "")


def reaction_emoji_entries() -> List[Tuple[str, str]]:
    return [(emoji_id, EMOJI_NAMES.get(emoji_id, emoji_id)) for emoji_id in REACTION_EMOJI_IDS]
