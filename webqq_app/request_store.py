import hashlib
import json
import os
import time
from pathlib import Path

from .common import DATA_DIR


class ContactRequestStore:
    """Durable audit log for friend and group requests."""

    def __init__(self, data_dir=DATA_DIR):
        self._dir = Path(data_dir) / "requests"
        self._path = self._dir / "requests.json"
        self._items = {}
        self._dir.mkdir(parents=True, exist_ok=True)
        self.load()

    @staticmethod
    def request_id(request_type, sub_type, flag):
        source = f"{request_type}\0{sub_type}\0{flag}".encode("utf-8")
        return hashlib.sha256(source).hexdigest()[:24]

    def load(self):
        try:
            with open(self._path, encoding="utf-8") as source:
                records = json.load(source)
            if isinstance(records, list):
                self._items = {
                    str(item["id"]): item for item in records
                    if isinstance(item, dict) and item.get("id")
                }
        except (OSError, ValueError, TypeError):
            self._items = {}

    def _save(self):
        tmp = self._path.with_suffix(".json.tmp")
        records = sorted(self._items.values(), key=lambda item: item.get("received_at", 0))
        with open(tmp, "w", encoding="utf-8") as output:
            json.dump(records, output, ensure_ascii=False, indent=2)
            output.write("\n")
        os.replace(tmp, self._path)

    def upsert(self, event, source="event"):
        request_type = str(event.get("request_type") or "").strip()
        sub_type = str(event.get("sub_type") or "").strip()
        flag = str(event.get("flag") or "").strip()
        if request_type not in ("friend", "group") or not flag:
            return None, False
        request_id = self.request_id(request_type, sub_type, flag)
        now = time.time()
        existing = self._items.get(request_id)
        record = dict(existing or {})
        user_id = event.get("user_id") or event.get("invitor_uin") or event.get("requester_uin")
        user_name = (
            event.get("user_name") or event.get("nickname") or event.get("requester_nick")
            or event.get("invitor_nick") or ""
        )
        record.update({
            "id": request_id,
            "request_type": request_type,
            "sub_type": sub_type,
            "flag": flag,
            "user_id": str(user_id or record.get("user_id") or ""),
            "user_name": str(user_name or record.get("user_name") or ""),
            "group_id": str(event.get("group_id") or record.get("group_id") or ""),
            "group_name": str(event.get("group_name") or record.get("group_name") or ""),
            "comment": str(event.get("comment") or event.get("message") or record.get("comment") or ""),
            "source": str(record.get("source") or source),
            "received_at": float(record.get("received_at") or event.get("time") or now),
            "updated_at": now,
        })
        if existing is None:
            record.update({"status": "pending", "decision": "", "error": "", "auto": False})
        changed = existing != record
        self._items[request_id] = record
        if changed:
            self._save()
        return dict(record), existing is None

    def get(self, request_id):
        item = self._items.get(str(request_id))
        return dict(item) if item else None

    def update(self, request_id, **changes):
        request_id = str(request_id)
        if request_id not in self._items:
            return None
        item = self._items[request_id]
        item.update(changes)
        item["updated_at"] = time.time()
        self._save()
        return dict(item)

    def list(self, status="", request_type=""):
        records = list(self._items.values())
        if status:
            records = [record for record in records if record.get("status") == status]
        if request_type:
            records = [record for record in records if record.get("request_type") == request_type]
        records.sort(key=lambda item: (item.get("received_at", 0), item.get("id", "")), reverse=True)
        return [dict(record) for record in records]

    def pending_count(self):
        return sum(record.get("status") == "pending" for record in self._items.values())
