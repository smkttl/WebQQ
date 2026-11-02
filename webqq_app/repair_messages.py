"""Conservative offline repair: python3 -m webqq_app.repair_messages [--apply]."""

import argparse
import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

import aiohttp

from .common import CONFIG_PATH, DATA_DIR, normalize_config, normalize_message_id, parse_chat_id


class RepairClient:
    """Read-only OneBot calls on a separate, authenticated connection."""

    def __init__(self, ws):
        self.ws = ws

    async def request(self, action, params):
        if action not in ("get_login_info", "get_msg"):
            raise ValueError("repair only permits read-only identity/message lookups")
        echo = uuid.uuid4().hex
        await self.ws.send_json({"action": action, "params": params, "echo": echo})

        async def receive():
            async for incoming in self.ws:
                if incoming.type != aiohttp.WSMsgType.TEXT:
                    continue
                response = json.loads(incoming.data)
                if isinstance(response, dict) and response.get("echo") == echo:
                    return response.get("data") if response.get("status") == "ok" else None
            raise ConnectionError("Snowluma disconnected during repair")

        return await asyncio.wait_for(receive(), timeout=15)


def _same_message(left, right):
    return all(str(left.get(key)) == str(right.get(key)) for key in ("message_id", "sender_id", "time", "content"))


async def plan_repair(data_dir, lookup):
    login = await lookup("get_login_info", {})
    account = str((login or {}).get("user_id") or "")
    if not account.isascii() or not account.isdigit() or int(account) <= 0:
        raise RuntimeError("cannot verify account identity; no files changed")
    originals = {}
    records = {}
    for path in sorted(Path(data_dir).glob("*.json")):
        if not parse_chat_id(path.stem):
            continue
        raw = path.read_bytes()
        messages = json.loads(raw)
        if not isinstance(messages, list) or any(not isinstance(item, dict) for item in messages):
            raise RuntimeError(f"invalid message file: {path.name}; no files changed")
        originals[path.name] = raw
        records[path.name] = messages
    repaired = copy.deepcopy(records)
    report = {"account_id": account, "direction_changes": 0, "moved": [], "duplicates_removed": 0, "unresolved": []}

    for messages in repaired.values():
        for message in messages:
            sender = str(message.get("sender_id") or "")
            if message.get("type") == "system" or not sender.isascii() or not sender.isdigit() or int(sender) <= 0:
                continue
            outgoing = sender == account
            if message.get("self") is not outgoing:
                message["self"] = outgoing
                report["direction_changes"] += 1

    self_file = f"private_{account}.json"
    for message in list(repaired.get(self_file, [])):
        if str(message.get("sender_id")) != account or message.get("type") != "private":
            continue
        try:
            message_id = normalize_message_id(message.get("message_id"))
        except ValueError:
            report["unresolved"].append({"file": self_file, "message_id": message.get("message_id"), "reason": "no upstream message ID"})
            continue
        peer = message.get("target_id")
        if peer is None:
            try:
                upstream = await lookup("get_msg", {"message_id": int(message_id)})
            except (TimeoutError, ConnectionError, aiohttp.ClientError) as error:
                upstream = None
                reason = str(error) or type(error).__name__
            else:
                reason = "upstream record unavailable or identity/timestamp does not match"
            if (not isinstance(upstream, dict) or upstream.get("message_type") != "private"
                    or str(upstream.get("message_id")) != str(message.get("message_id"))
                    or str(upstream.get("user_id")) != account
                    or upstream.get("time") != message.get("time")):
                report["unresolved"].append({"file": self_file, "message_id": message_id, "reason": reason})
                continue
            peer = upstream.get("target_id")
        peer = str(peer or "")
        if not peer.isascii() or not peer.isdigit() or int(peer) <= 0:
            report["unresolved"].append({"file": self_file, "message_id": message_id, "reason": "no verified recipient"})
            continue
        if peer == account:
            continue  # A verified conversation with oneself is legitimate.
        destination = f"private_{peer}.json"
        messages = repaired.setdefault(destination, [])
        duplicate = next((item for item in messages if str(item.get("message_id")) == message_id), None)
        if duplicate is not None and not _same_message(message, duplicate):
            report["unresolved"].append({"file": self_file, "message_id": message_id, "reason": "destination ID conflicts with saved message"})
            continue
        moved = {**message, "chat_id": f"private_{peer}", "user_id": int(peer), "target_id": int(peer)}
        if duplicate is not None:
            # Preserve richer attachment/reaction metadata from either record.
            for key, value in moved.items():
                if not duplicate.get(key) and value:
                    duplicate[key] = value
            duplicate.update(chat_id=f"private_{peer}", user_id=int(peer), target_id=int(peer), self=True)
            report["duplicates_removed"] += 1
        else:
            # An old chat_name may be the account's own name. A QQ number is a
            # safe fallback until contact refresh supplies the recipient's name.
            moved["chat_name"] = peer
            messages.append(moved)
        repaired[self_file].remove(message)
        report["moved"].append({"message_id": message_id, "from": self_file, "to": destination})

    changes = {}
    for name, messages in repaired.items():
        messages.sort(key=lambda item: item.get("time", 0))
        if name not in records or messages != records[name]:
            changes[name] = (originals.get(name), json.dumps(messages, ensure_ascii=False, separators=(",", ":")).encode())
    report["changed_files"] = sorted(changes)
    return changes, report


def atomic_write(path, raw):
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def apply_repair(data_dir, changes, report):
    root = Path(data_dir)
    # Detect edits made since the snapshot; never overwrite a running writer.
    for name, (before, after) in changes.items():
        path = root / name
        current = path.read_bytes() if path.exists() else None
        if current != before:
            raise RuntimeError(f"{name} changed during repair; stop WebQQ and retry")
    if not changes:
        return None
    backup = root / "repair-backups" / (time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    backup.mkdir(parents=True, mode=0o700)
    manifest = {**report, "state": "prepared", "files": {}}
    for name, (before, after) in changes.items():
        if before is not None:
            atomic_write(backup / name, before)
        manifest["files"][name] = {
            "existed": before is not None,
            "before_sha256": hashlib.sha256(before).hexdigest() if before is not None else None,
            "after_sha256": hashlib.sha256(after).hexdigest(),
        }
    manifest_path = backup / "manifest.json"

    def save_manifest():
        atomic_write(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2).encode())

    save_manifest()
    written = []
    try:
        for name, (before, after) in changes.items():
            atomic_write(root / name, after)
            written.append(name)
        manifest["state"] = "applied"
        save_manifest()
    except Exception:
        rollback_errors = []
        for name in reversed(written):
            try:
                before = changes[name][0]
                if before is None:
                    (root / name).unlink()
                else:
                    atomic_write(root / name, before)
            except Exception as error:
                rollback_errors.append({"file": name, "error": str(error)})
        manifest["state"] = "rollback_failed" if rollback_errors else "rolled_back"
        manifest["rollback_errors"] = rollback_errors
        save_manifest()
        raise RuntimeError(f"repair failed; recovery manifest: {manifest_path}")
    return backup


async def run(args):
    if args.apply:
        status = subprocess.run(["systemctl", "is-active", "--quiet", "WebQQ.service"], check=False)
        if status.returncode == 0:
            raise RuntimeError("stop WebQQ.service before applying saved-message repairs")
    config = normalize_config(json.loads(Path(args.config).read_text()))
    if config["backend"] != "snowluma":
        raise RuntimeError("saved-message recovery requires the configured Snowluma backend")
    headers = {"Authorization": f"Bearer {config['ws_token']}"} if config["ws_token"] else {}
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(config["ws_url"], headers=headers) as ws:
            changes, report = await plan_repair(args.data_dir, RepairClient(ws).request)
    if args.apply:
        backup = apply_repair(args.data_dir, changes, report)
        report["backup_directory"] = str(backup) if backup else None
    report["applied"] = args.apply
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.report:
        Path(args.report).write_text(output + "\n")
    print(output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(CONFIG_PATH))
    parser.add_argument("--data-dir", default=str(DATA_DIR))
    parser.add_argument("--report")
    parser.add_argument("--apply", action="store_true", help="write repairs after stopping WebQQ.service")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except Exception as error:
        parser.exit(1, f"repair aborted: {error}\n")


if __name__ == "__main__":
    main()
