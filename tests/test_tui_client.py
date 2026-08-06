import tempfile
import unittest
from pathlib import Path

from aiohttp import web

from webqq_tui_app.client import AuthenticationError, WebQQClient
from webqq_tui_app.config import TuiConfig
from webqq_tui_app.models import Attachment


class WebQQClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.upload = {}
        self.image_upload = {}
        self.pokes = []
        self.reactions = []
        self.forward_ids = []
        app = web.Application()
        app.router.add_post("/api/login", self.login)
        app.router.add_get("/api/status", self.status)
        app.router.add_get("/api/chats", self.chats)
        app.router.add_get("/api/messages", self.messages)
        app.router.add_get("/api/group-members", self.group_members)
        app.router.add_get("/api/forward", self.forward)
        app.router.add_post("/api/send", self.send)
        app.router.add_post("/api/poke", self.poke)
        app.router.add_post("/api/message/emoji-like", self.face_reply)
        app.router.add_post("/api/mark-read", self.mark_read)
        app.router.add_post("/api/send-file", self.send_file)
        app.router.add_post("/api/send-image", self.send_image)
        app.router.add_get("/api/file", self.download)
        app.router.add_get("/ws", self.websocket)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        config = TuiConfig("http://127.0.0.1:{}".format(port), "secret", Path(self.tmp.name))
        self.client = WebQQClient(config)
        await self.client.start()

    async def asyncTearDown(self):
        await self.client.close()
        await self.runner.cleanup()
        self.tmp.cleanup()

    @staticmethod
    def authed(request):
        return request.cookies.get("token") == "secret"

    async def login(self, request):
        body = await request.json()
        if body.get("token") != "secret":
            return web.json_response({"ok": False, "error": "invalid token"}, status=401)
        response = web.json_response({"ok": True})
        response.set_cookie("token", "secret")
        return response

    async def status(self, request):
        return web.json_response({"napcat_connected": True, "chats_count": 1, "self_user": {"name": "Me"}})

    async def chats(self, request):
        if not self.authed(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return web.json_response({"chats": [{"chat_id": "group_1", "name": "Group", "type": "group"}]})

    async def messages(self, request):
        return web.json_response({"messages": [{
            "chat_id": request.query["chat_id"], "message_id": 1, "time": 1, "sender_name": "A", "content": "hello",
        }]})

    async def group_members(self, request):
        return web.json_response({"members": [{"user_id": 2, "display_name": "Alice"}]})

    async def forward(self, request):
        self.forward_ids.append(request.query.get("id"))
        return web.json_response({
            "ok": True,
            "forward": {"id": request.query.get("id"), "status": "ok", "nodes": [{"content": "inside"}]},
        })

    async def send(self, request):
        body = await request.json()
        return web.json_response({"ok": True, "data": body})

    async def poke(self, request):
        body = await request.json()
        self.pokes.append(body)
        return web.json_response({"ok": True})

    async def face_reply(self, request):
        body = await request.json()
        self.reactions.append(body)
        return web.json_response({
            "ok": True,
            "message_id": body["message_id"],
            "reactions": [{"emoji_id": body["emoji_id"], "count": 1}],
        })

    async def mark_read(self, request):
        return web.json_response({"ok": True})

    async def send_file(self, request):
        reader = await request.multipart()
        fields = {}
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                fields["name"] = part.filename
                fields["body"] = await part.read()
            else:
                fields[part.name] = await part.text()
        self.upload = fields
        return web.json_response({"ok": True})

    async def send_image(self, request):
        reader = await request.multipart()
        fields = {}
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == "file":
                fields["name"] = part.filename
                fields["body"] = await part.read()
            else:
                fields[part.name] = await part.text()
        self.image_upload = fields
        return web.json_response({"ok": True})

    async def download(self, request):
        return web.Response(body=b"attachment body")

    async def websocket(self, request):
        if not self.authed(request):
            raise web.HTTPUnauthorized()
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_json({"type": "new_message", "data": {"chat_id": "group_1", "content": "live"}})
        await socket.close()
        return socket

    async def test_login_cookie_and_core_requests(self):
        await self.client.login()
        chats = await self.client.chats()
        self.assertEqual(chats[0].chat_id, "group_1")
        messages = await self.client.messages("group_1", before=10)
        self.assertEqual(messages[0].content, "hello")
        members = await self.client.group_members("group_1")
        self.assertEqual(members[0]["display_name"], "Alice")
        forward = await self.client.forward("forward-1")
        self.assertEqual(forward["nodes"][0]["content"], "inside")
        self.assertEqual(self.forward_ids, ["forward-1"])
        sent = await self.client.send_message("group_1", "hi", reply_to="1")
        self.assertEqual(sent["data"]["reply_to"], "1")
        await self.client.poke("group_1", "2")
        self.assertEqual(self.pokes, [{"chat_id": "group_1", "user_id": "2"}])
        reaction = await self.client.send_face_reply("group_1", "1", "14")
        self.assertEqual(self.reactions, [{"chat_id": "group_1", "message_id": "1", "emoji_id": "14"}])
        self.assertEqual(reaction["reactions"][0]["emoji_id"], "14")
        await self.client.mark_read("group_1")

    async def test_invalid_login_is_reported(self):
        with self.assertRaises(AuthenticationError):
            await self.client.login("wrong")

    async def test_upload_download_and_websocket(self):
        await self.client.login()
        source = Path(self.tmp.name) / "source.txt"
        source.write_bytes(b"upload body")
        await self.client.send_file("group_1", source)
        self.assertEqual(self.upload["chat_id"], "group_1")
        self.assertEqual(self.upload["body"], b"upload body")

        image = Path(self.tmp.name) / "photo.png"
        image.write_bytes(b"image body")
        await self.client.send_image("private_2", image)
        self.assertEqual(self.image_upload["chat_id"], "private_2")
        self.assertEqual(self.image_upload["name"], "photo.png")
        self.assertEqual(self.image_upload["body"], b"image body")

        existing = Path(self.tmp.name) / "report.txt"
        existing.write_text("old", encoding="utf-8")
        attachment = Attachment("file", "report.txt", 0, {"id": "1", "name": "report.txt"})
        saved = await self.client.download_attachment("group_1", attachment)
        self.assertEqual(saved.name, "report (1).txt")
        self.assertEqual(saved.read_bytes(), b"attachment body")

        socket = await self.client.websocket()
        event = await socket.receive_json()
        self.assertEqual(event["type"], "new_message")
        await socket.close()

    def test_endpoint_preserves_server_path_prefix(self):
        self.client._base = self.client._base.with_path("/webqq")
        self.assertEqual(str(self.client.endpoint("/api/chats")), str(self.client._base) + "/api/chats")
        self.assertTrue(str(self.client.endpoint("/ws", websocket=True)).startswith("ws://"))


if __name__ == "__main__":
    unittest.main()
