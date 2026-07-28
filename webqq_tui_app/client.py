import json
import mimetypes
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional
from urllib.parse import unquote

import aiohttp
from yarl import URL

from .config import TuiConfig
from .models import Attachment, Chat, Message


MAX_UPLOAD_SIZE = 100 * 1024 * 1024


class WebQQClientError(Exception):
    pass


class AuthenticationError(WebQQClientError):
    pass


class ServerResponseError(WebQQClientError):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


class WebQQClient:
    def __init__(self, config: TuiConfig):
        self.config = config
        self._base = URL(config.server_url)
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        if self.session and not self.session.closed:
            return
        timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=30)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            headers={"User-Agent": "WebQQ-TUI/1"},
        )

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    def endpoint(self, path: str, websocket: bool = False) -> URL:
        if not path.startswith("/"):
            path = "/" + path
        scheme = self._base.scheme
        if websocket:
            scheme = "wss" if scheme == "https" else "ws"
        base_path = self._base.path.rstrip("/")
        return self._base.with_scheme(scheme).with_path(base_path + path).with_query(None)

    async def login(self, token: Optional[str] = None) -> None:
        token = self.config.token if token is None else token
        response = await self._request_json(
            "POST",
            "/api/login",
            json_body={"token": token or ""},
            allow_auth_error=True,
        )
        if not response.get("ok"):
            raise AuthenticationError(str(response.get("error") or "login failed"))

    async def status(self) -> Mapping[str, Any]:
        return await self._request_json("GET", "/api/status")

    async def chats(self) -> List[Chat]:
        payload = await self._request_json("GET", "/api/chats")
        values = payload.get("chats")
        return [Chat.from_json(item) for item in values if isinstance(item, dict)] if isinstance(values, list) else []

    async def messages(self, chat_id: str, limit: int = 50, before: Optional[float] = None) -> List[Message]:
        params: Dict[str, str] = {"chat_id": chat_id, "limit": str(limit)}
        if before is not None:
            params["before"] = str(before)
        payload = await self._request_json("GET", "/api/messages", params=params)
        values = payload.get("messages")
        return [Message.from_json(item) for item in values if isinstance(item, dict)] if isinstance(values, list) else []

    async def group_members(self, chat_id: str) -> List[Mapping[str, Any]]:
        payload = await self._request_json("GET", "/api/group-members", params={"chat_id": chat_id})
        members = payload.get("members")
        return [dict(item) for item in members if isinstance(item, dict)] if isinstance(members, list) else []

    async def forward(self, forward_id: str) -> Mapping[str, Any]:
        payload = await self._request_json("GET", "/api/forward", params={"id": forward_id})
        self._require_ok(payload, "forward load failed")
        forward = payload.get("forward")
        if not isinstance(forward, dict):
            raise ServerResponseError("invalid forward response")
        return dict(forward)

    async def send_message(self, chat_id: str, text: str, reply_to: str = "") -> Mapping[str, Any]:
        body: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_to:
            body["reply_to"] = reply_to
        payload = await self._request_json("POST", "/api/send", json_body=body)
        self._require_ok(payload, "send failed")
        return payload

    async def poke(self, chat_id: str, user_id: str) -> Mapping[str, Any]:
        payload = await self._request_json(
            "POST",
            "/api/poke",
            json_body={"chat_id": chat_id, "user_id": user_id},
        )
        self._require_ok(payload, "poke failed")
        return payload

    async def send_face_reply(self, chat_id: str, message_id: str, emoji_id: str) -> Mapping[str, Any]:
        payload = await self._request_json(
            "POST",
            "/api/message/emoji-like",
            json_body={"chat_id": chat_id, "message_id": message_id, "emoji_id": emoji_id},
        )
        self._require_ok(payload, "reaction failed")
        return payload

    async def mark_read(self, chat_id: str) -> None:
        payload = await self._request_json("POST", "/api/mark-read", json_body={"chat_id": chat_id})
        self._require_ok(payload, "mark read failed")

    async def send_file(self, chat_id: str, path: Path) -> Mapping[str, Any]:
        path = path.expanduser().resolve()
        if not path.is_file():
            raise WebQQClientError("file does not exist: {}".format(path))
        size = path.stat().st_size
        if size <= 0:
            raise WebQQClientError("file is empty")
        if size > MAX_UPLOAD_SIZE:
            raise WebQQClientError("file is larger than 100 MB")
        session = self._session()
        form = aiohttp.FormData()
        form.add_field("chat_id", chat_id)
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        with path.open("rb") as body:
            form.add_field("file", body, filename=path.name, content_type=content_type)
            try:
                async with session.post(self.endpoint("/api/send-file"), data=form) as response:
                    payload = await self._read_json(response)
            except (aiohttp.ClientError, OSError) as exc:
                raise WebQQClientError("file upload failed: {}".format(exc)) from exc
        self._require_ok(payload, "file upload failed")
        return payload

    async def websocket(self) -> aiohttp.ClientWebSocketResponse:
        try:
            return await self._session().ws_connect(self.endpoint("/ws", websocket=True), heartbeat=30)
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status == 401:
                raise AuthenticationError("WebSocket authentication failed") from exc
            raise WebQQClientError("WebSocket connection failed: HTTP {}".format(exc.status)) from exc
        except (aiohttp.ClientError, OSError) as exc:
            raise WebQQClientError("WebSocket connection failed: {}".format(exc)) from exc

    async def download_attachment(
        self,
        chat_id: str,
        attachment: Attachment,
        progress: Optional[Callable[[int, int], None]] = None,
    ) -> Path:
        params = self._attachment_params(chat_id, attachment)
        endpoint = "/api/image/full" if attachment.kind == "image" else "/api/file"
        target = collision_safe_path(self.config.download_dir, attachment.name or attachment.kind)
        self.config.download_dir.mkdir(parents=True, exist_ok=True)
        try:
            async with self._session().get(self.endpoint(endpoint), params=params, timeout=aiohttp.ClientTimeout(total=180)) as response:
                if response.status == 401:
                    raise AuthenticationError("download authentication failed")
                if response.status >= 400:
                    payload = await self._read_json(response)
                    raise ServerResponseError(str(payload.get("error") or "download failed"), response.status)
                total = int(response.headers.get("Content-Length") or 0)
                received = 0
                with target.open("wb") as output:
                    async for chunk in response.content.iter_chunked(256 * 1024):
                        output.write(chunk)
                        received += len(chunk)
                        if progress:
                            progress(received, total)
        except Exception:
            if target.exists():
                target.unlink()
            raise
        return target

    def _attachment_params(self, chat_id: str, attachment: Attachment) -> Dict[str, str]:
        data = attachment.data
        if attachment.kind == "image":
            return {
                key: str(value)
                for key, value in (("url", data.get("url") or data.get("thumbnail")), ("file", data.get("file")))
                if value
            }
        params: Dict[str, str] = {"chat_id": chat_id, "name": attachment.name}
        aliases = {
            "id": data.get("id") or data.get("file_id") or data.get("msgId"),
            "url": data.get("url"),
            "file": data.get("file"),
            "busid": data.get("busid"),
        }
        params.update({key: str(value) for key, value in aliases.items() if value is not None and value != ""})
        return params

    async def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Mapping[str, str]] = None,
        json_body: Optional[Mapping[str, Any]] = None,
        allow_auth_error: bool = False,
    ) -> Mapping[str, Any]:
        try:
            async with self._session().request(method, self.endpoint(path), params=params, json=json_body) as response:
                payload = await self._read_json(response)
        except (aiohttp.ClientError, OSError) as exc:
            raise WebQQClientError("server request failed: {}".format(exc)) from exc
        if response.status == 401:
            if allow_auth_error:
                return payload
            raise AuthenticationError(str(payload.get("error") or "unauthorized"))
        if response.status == 404:
            raise ServerResponseError(
                "WebQQ API not found at {}; check --url and do not include /api".format(self.config.server_url),
                response.status,
            )
        if response.status >= 400:
            raise ServerResponseError(str(payload.get("error") or "HTTP {}".format(response.status)), response.status)
        return payload

    async def _read_json(self, response: aiohttp.ClientResponse) -> Mapping[str, Any]:
        text = await response.text()
        if not text:
            return {}
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return {"ok": False, "error": text.strip() or "HTTP {}".format(response.status)}
        return payload if isinstance(payload, dict) else {"ok": False, "error": "invalid server response"}

    def _session(self) -> aiohttp.ClientSession:
        if not self.session or self.session.closed:
            raise WebQQClientError("client is not started")
        return self.session

    @staticmethod
    def _require_ok(payload: Mapping[str, Any], fallback: str) -> None:
        if not payload.get("ok"):
            raise ServerResponseError(str(payload.get("error") or fallback))


INVALID_FILENAME = re.compile(r"[\\/\x00-\x1f]+")


def safe_filename(value: str) -> str:
    value = unquote(str(value or "file")).strip()
    value = INVALID_FILENAME.sub("_", value).strip(". ")
    return value[:240] or "file"


def collision_safe_path(directory: Path, filename: str) -> Path:
    filename = safe_filename(filename)
    candidate = directory / filename
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    number = 1
    while True:
        candidate = directory / "{} ({}){}".format(stem, number, suffix)
        if not candidate.exists():
            return candidate
        number += 1
