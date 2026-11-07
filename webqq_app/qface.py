"""Persistent, same-origin cache for the public Koishi QQ face library."""

import asyncio
import json
import os
from pathlib import Path
import re
import tempfile
import time

import aiohttp
from aiohttp import web
from yarl import URL

from .common import DATA_DIR

UPSTREAM = "https://koishi.js.org/QFace/"
MANIFEST = "assets/qq_emoji/_index.json"
ASSET_PATH = re.compile(r"assets/qq_emoji/[A-Za-z0-9_\-\u00a0-\U0010ffff]+/(?:png|apng)/[A-Za-z0-9_\-\u00a0-\U0010ffff]+\.png\Z")
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def valid_path(path):
    return len(path) <= 512 and (path == MANIFEST or bool(ASSET_PATH.fullmatch(path)))


class FaceMirror:
    def __init__(self, directory, session, upstream=UPSTREAM):
        self.directory = Path(directory)
        self.session = session
        self.upstream = URL(upstream)
        self.pending = {}
        self.slots = asyncio.Semaphore(4)

    async def close(self):
        tasks = list(self.pending.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def get(self, path):
        if not valid_path(path):
            raise web.HTTPNotFound()
        target = self.directory / path
        ttl = 86400 if path == MANIFEST else 7 * 86400
        try:
            if time.time() - target.stat().st_mtime < ttl:
                return await asyncio.to_thread(target.read_bytes), False
        except FileNotFoundError:
            pass
        task = self.pending.get(path)
        if task is None:
            task = asyncio.create_task(self._refresh(path, target))
            self.pending[path] = task
            def finished(done):
                self.pending.pop(path, None)
                if not done.cancelled():
                    done.exception()  # Retrieve failures even if all clients disconnect.
            task.add_done_callback(finished)
        return await asyncio.shield(task)

    async def _refresh(self, path, target):
        try:
            async with self.slots:
                raw = await asyncio.wait_for(self._download(path), timeout=15)
                await asyncio.to_thread(self._write, target, raw)
            return raw, False
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError, ValueError) as error:
            try:
                return await asyncio.to_thread(target.read_bytes), True
            except FileNotFoundError:
                raise web.HTTPBadGateway(text="Face library temporarily unavailable") from error

    async def _download(self, path):
        url = self.upstream.join(URL(path))
        limit = 5 * 1024 * 1024 if path == MANIFEST else 10 * 1024 * 1024
        for redirect in range(4):
            # Validate every hop before issuing the next request.
            if (url.origin() != self.upstream.origin() or url.user or url.password or url.query_string or url.fragment
                    or not url.path.startswith(self.upstream.path)
                    or not valid_path(url.path[len(self.upstream.path):])):
                raise ValueError("invalid face redirect")
            async with self.session.get(url, allow_redirects=False) as response:
                if response.status in (301, 302, 303, 307, 308):
                    location = response.headers.get("Location")
                    if not location or redirect == 3:
                        raise ValueError("invalid face redirect")
                    url = url.join(URL(location))
                    continue
                response.raise_for_status()
                if response.status != 200:
                    raise ValueError("unexpected upstream response")
                if response.content_length is not None and response.content_length > limit:
                    raise ValueError("face response too large")
                raw = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    raw.extend(chunk)
                    if len(raw) > limit:
                        raise ValueError("face response too large")
                if path == MANIFEST:
                    data = json.loads(raw)
                    if not isinstance(data, list) or not data:
                        raise ValueError("invalid face manifest")
                    for item in data:
                        if not isinstance(item, dict) or not isinstance(item.get("assets"), list):
                            raise ValueError("invalid face manifest entry")
                        for asset in item["assets"]:
                            if not isinstance(asset, dict):
                                raise ValueError("invalid face asset")
                            if asset.get("type") in (0, 2) and not valid_path(str(asset.get("path", ""))):
                                raise ValueError("invalid face asset path")
                elif not raw.startswith(PNG_SIGNATURE):
                    raise ValueError("invalid PNG asset")
                return bytes(raw)
        raise ValueError("face redirect limit exceeded")

    @staticmethod
    def _write(target, raw):
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".qface-", dir=target.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


async def handle_face(request):
    path = request.match_info["path"]
    if request.query_string or not valid_path(path):
        raise web.HTTPNotFound()
    raw, stale = await request.app["qface_mirror"].get(path)
    headers = {"Cache-Control": "public, max-age=" + ("60" if stale else "3600" if path == MANIFEST else "86400"),
               "X-Content-Type-Options": "nosniff"}
    if stale:
        headers["X-QFace-Cache"] = "stale"
    # APNG is a PNG container; preserve its animation bytes without conversion.
    return web.Response(body=raw, content_type="application/json" if path == MANIFEST else "image/png", headers=headers)


async def mirror_context(app):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        mirror = FaceMirror(DATA_DIR / "qface-cache", session)
        app["qface_mirror"] = mirror
        try:
            yield
        finally:
            await mirror.close()


def setup_face_mirror(app):
    app.cleanup_ctx.append(mirror_context)
    app.router.add_get("/qface/{path:.*}", handle_face)
