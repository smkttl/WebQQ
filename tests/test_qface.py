import asyncio
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from webqq_app.qface import FaceMirror, MANIFEST, PNG_SIGNATURE, handle_face, valid_path


ASSET = "assets/qq_emoji/4/png/4.png"
ANIMATED = "assets/qq_emoji/4/apng/4.png"
SAMPLE = [{"emojiId": "4", "describe": "/得意", "emojiType": 0, "assets": [
    {"type": 0, "path": ASSET}, {"type": 2, "path": ANIMATED},
]}]


class MirrorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.calls = []
        self.active = 0
        self.maximum_active = 0
        self.status = 200
        self.payload = None
        self.redirect = None
        self.redirect_once = False
        self.chunked = False
        self.delay = False

        async def upstream(request):
            self.calls.append(request.path)
            if self.redirect:
                location = self.redirect
                if self.redirect_once:
                    self.redirect = None
                return web.Response(status=302, headers={"Location": location})
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            try:
                if self.delay:
                    await asyncio.sleep(.03)
                raw = self.payload
                if raw is None:
                    raw = json.dumps(SAMPLE).encode() if request.path.endswith("_index.json") else PNG_SIGNATURE + b"test"
                if self.chunked:
                    response = web.StreamResponse(status=self.status)
                    await response.prepare(request)
                    try:
                        for offset in range(0, len(raw), 65536):
                            await response.write(raw[offset:offset + 65536])
                        await response.write_eof()
                    except ConnectionResetError:
                        pass  # The mirror closes oversized responses early.
                    return response
                return web.Response(body=raw, status=self.status)
            finally:
                self.active -= 1

        app = web.Application()
        app.router.add_get("/{path:.*}", upstream)
        self.upstream = TestServer(app)
        await self.upstream.start_server()
        self.session = aiohttp.ClientSession()
        self.mirror = FaceMirror(self.temporary.name, self.session, str(self.upstream.make_url("/QFace/")))
        local = web.Application()
        local["qface_mirror"] = self.mirror
        local.router.add_get("/qface/{path:.*}", handle_face)
        self.client = TestClient(TestServer(local))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        await self.mirror.close()
        await self.session.close()
        await self.upstream.close()

    def expire(self, path):
        os.utime(Path(self.temporary.name) / path, (1, 1))

    async def test_manifest_and_animation_cached_with_content_types(self):
        for path, expected_type in [(MANIFEST, "application/json"), (ASSET, "image/png"), (ANIMATED, "image/png")]:
            response = await self.client.get("/qface/" + path)
            self.assertEqual(response.status, 200)
            self.assertEqual(response.content_type, expected_type)
            raw = await response.read()
            self.assertEqual(raw, (Path(self.temporary.name) / path).read_bytes())
            if path != MANIFEST:
                self.assertEqual(raw, PNG_SIGNATURE + b"test")
            self.assertIn("public", response.headers["Cache-Control"])
            await self.mirror.get(path)
        self.assertEqual(len(self.calls), 3)
        # Persistent cache works with a new mirror/session instance, too.
        second = FaceMirror(self.temporary.name, self.session)
        self.assertFalse((await second.get(ASSET))[1])
        self.assertEqual(len(self.calls), 3)

    async def test_refresh_and_stale_fallback(self):
        await self.mirror.get(ASSET)
        self.expire(ASSET)
        self.payload = PNG_SIGNATURE + b"new"
        self.assertEqual((await self.mirror.get(ASSET))[0], self.payload)
        self.expire(ASSET)
        self.status = 503
        raw, stale = await self.mirror.get(ASSET)
        self.assertTrue(stale)
        self.assertEqual(raw, self.payload)
        response = await self.client.get("/qface/" + ASSET)
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers["X-QFace-Cache"], "stale")
        self.assertEqual(response.headers["Cache-Control"], "public, max-age=60")

    async def test_ttls_differ_for_manifest_and_images(self):
        await self.mirror.get(MANIFEST)
        await self.mirror.get(ASSET)
        import time
        age = time.time() - 2 * 86400
        for path in (MANIFEST, ASSET):
            os.utime(Path(self.temporary.name) / path, (age, age))
        await self.mirror.get(MANIFEST)
        await self.mirror.get(ASSET)
        self.assertEqual(len(self.calls), 3)

    async def test_unicode_asset_names(self):
        path = "assets/qq_emoji/😀/png/😀.png"
        response = await self.client.get("/qface/" + path)
        self.assertEqual(response.status, 200)
        self.assertEqual(await response.read(), PNG_SIGNATURE + b"test")
        self.assertTrue((Path(self.temporary.name) / path).exists())

    async def test_uncached_failure_and_invalid_payloads(self):
        for path, raw in [(MANIFEST, b"not json"), (MANIFEST, b"{}"),
                          (MANIFEST, b'[{"assets":[{"type":0,"path":"https://evil.test/a.png"}]}]'),
                          (ASSET, b"<html>error</html>")]:
            self.payload = raw
            response = await self.client.get("/qface/" + path)
            self.assertEqual(response.status, 502)
            self.assertFalse((Path(self.temporary.name) / path).exists())
        self.status = 404
        response = await self.client.get("/qface/" + ANIMATED)
        self.assertEqual(response.status, 502)

    async def test_path_and_query_rejections_never_fetch(self):
        for path in ("https://evil.test/a.png", "../secret", "assets/qq_emoji/4/png/../secret", "assets/qq_emoji/4/lottie/4.json", "assets/qq_emoji/4/png/a.png?x=1"):
            self.assertFalse(valid_path(path))
        for path in ("evil", ASSET + "?url=https://evil.test/", "assets/qq_emoji/4/png/%2Fetc.png"):
            response = await self.client.get("/qface/" + path)
            self.assertEqual(response.status, 404)
        self.assertEqual(self.calls, [])

    async def test_redirect_restrictions(self):
        for redirect in ("https://evil.test/face.png", "/outside.png", "./4.png?x=1", "./4.png#x"):
            self.redirect = redirect
            response = await self.client.get("/qface/" + ASSET)
            self.assertEqual(response.status, 502)
        self.assertEqual(len(self.calls), 4)
        self.redirect = ASSET  # Resolves inside origin but outside the permitted library prefix.
        response = await self.client.get("/qface/" + ASSET)
        self.assertEqual(response.status, 502)

    async def test_size_limits(self):
        for chunked in (False, True):
            self.chunked = chunked
            for path, limit in [(MANIFEST, 5 * 1024 * 1024), (ASSET, 10 * 1024 * 1024)]:
                self.payload = b"x" * (limit + 1)
                response = await self.client.get("/qface/" + path)
                self.assertEqual(response.status, 502)
                self.assertFalse((Path(self.temporary.name) / path).exists())

    async def test_same_origin_library_redirect_and_loop_limit(self):
        self.redirect = "/QFace/" + ANIMATED
        self.redirect_once = True
        raw, stale = await self.mirror.get(ASSET)
        self.assertEqual(raw, PNG_SIGNATURE + b"test")
        self.assertFalse(stale)
        self.assertEqual(len(self.calls), 2)
        self.redirect = "/QFace/" + ASSET
        self.redirect_once = False
        with self.assertRaises(web.HTTPBadGateway):
            await self.mirror.get(ANIMATED)
        self.assertEqual(len(self.calls), 6)

    async def test_same_resource_coalesced_and_download_concurrency_bounded(self):
        self.delay = True
        await asyncio.gather(*(self.mirror.get(ASSET) for _ in range(12)))
        self.assertEqual(len(self.calls), 1)
        await asyncio.gather(*(self.mirror.get(f"assets/qq_emoji/{i}/png/{i}.png") for i in range(20, 32)))
        self.assertEqual(self.maximum_active, 4)

    async def test_timeout_uses_stale_and_cleanup_cancels_downloads(self):
        await self.mirror.get(ASSET)
        self.expire(ASSET)
        with patch.object(self.mirror, "_download", side_effect=asyncio.TimeoutError):
            self.assertTrue((await self.mirror.get(ASSET))[1])
        started = asyncio.Event()
        async def stuck(path):
            started.set()
            await asyncio.Future()
        with patch.object(self.mirror, "_download", side_effect=stuck):
            request = asyncio.create_task(self.mirror.get(ASSET))
            await started.wait()
            await self.mirror.close()
            with self.assertRaises(asyncio.CancelledError):
                await request
        self.assertEqual(self.mirror.pending, {})


class FaceFrontendTests(unittest.TestCase):
    def test_actual_manifest_normalizer_uses_only_local_asset_urls(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("Node is required to execute browser normalization")
        html = Path("static/index.html").read_text()
        start = html.index("function mirroredFaceAsset(")
        end = html.index("\nasync function updateStatus", start)
        script = "const assert = require('assert/strict'); const faceAssetBaseUrl = 'https://webqq.test/chat/qface/';\n" + html[start:end]
        script += "\nconst sample = " + json.dumps(SAMPLE) + ";\n"
        script += r"""
sample.push({emojiId:'😀', qcid:128512, describe:'/笑', assets:[{type:0,path:'assets/qq_emoji/😀/png/😀.png'}]});
sample.push({emojiId:'5', describe:'/bad', assets:[{type:0,path:'https://koishi.js.org/QFace/assets/qq_emoji/5/png/5.png'}]});
const result = normalizeFaceManifest(sample);
assert.equal(result.faces['4'].name, '得意');
assert.equal(result.faces['4'].png, faceAssetBaseUrl + 'assets/qq_emoji/4/png/4.png');
assert.equal(result.faces['4'].gif, faceAssetBaseUrl + 'assets/qq_emoji/4/apng/4.png');
assert.equal(result.faces['128512'].unicode, '😀');
assert.equal(result.faces['128512'].png, new URL('assets/qq_emoji/😀/png/😀.png', faceAssetBaseUrl).href);
assert.equal(result.faces['128512'].gif, result.faces['128512'].png);
assert.deepEqual(result.reaction_ids, ['4','128512','5']);
assert.equal(result.faces['5'].png, '');
assert.deepEqual(normalizeFaceManifest({faces:{'4':{png:'https://evil.test/a.png'}}}).faces, {});
for (const path of ['//evil.test/a.png', '../a.png', 'assets/qq_emoji/4/png/4.png?url=x']) assert.equal(mirroredFaceAsset(path), '');
for (const face of Object.values(result.faces)) for (const key of ['png','gif']) if (face[key]) assert.equal(new URL(face[key]).origin, 'https://webqq.test');
"""
        subprocess.run([node, "-e", script], check=True, capture_output=True, text=True)
        self.assertIn("new URL('qface/', document.baseURI)", html)
        self.assertNotIn("fetch(faceManifestUrl, {mode: 'cors'})", html)
