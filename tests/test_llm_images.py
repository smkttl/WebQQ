import base64
import unittest
from unittest.mock import ANY, AsyncMock

from plugins.llm.main import LlmPlugin, LlmRequestError


PNG = b"\x89PNG\r\n\x1a\n" + b"test"
GIF = b"GIF89a" + b"test"


def data_url(mime_type, body):
    return "data:{};base64,{}".format(mime_type, base64.b64encode(body).decode("ascii"))


class FakeContext:
    def __init__(self, config=None, messages=None):
        self.config = dict(config or {})
        self.messages = list(messages or [])
        self.logs = []
        self.napcat_responses = []

    def get_messages(self, chat_id, limit=50, before=None):
        return self.messages[-limit:]

    def get_self_user(self):
        return {}

    def log(self, message):
        self.logs.append(str(message))

    async def napcat(self, action, params=None, timeout=10):
        if self.napcat_responses:
            return self.napcat_responses.pop(0)
        return {"status": "failed"}


class RedirectResponse:
    status = 302
    headers = {"Location": "http://127.0.0.1/private.png"}
    content_length = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        return False


class RedirectSession:
    def get(self, url, **kwargs):
        return RedirectResponse()


def message(message_id, content="look", images=None):
    return {
        "chat_id": "group_1",
        "type": "group",
        "message_id": message_id,
        "sender_id": str(message_id),
        "sender_name": "User{}".format(message_id),
        "time": message_id,
        "content": content,
        "images": list(images or []),
    }


class LlmImageMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_image_input_keeps_string_content(self):
        trigger = message(2, images=[{"url": data_url("image/png", PNG)}])
        plugin = LlmPlugin(FakeContext(messages=[trigger]))

        messages = await plugin._build_messages(trigger, "look")

        self.assertTrue(all(isinstance(item["content"], str) for item in messages))

    async def test_only_trigger_images_are_attached(self):
        previous = message(1, images=[{"url": data_url("image/gif", GIF)}])
        trigger = message(2, images=[{"url": data_url("image/png", PNG)}])
        ctx = FakeContext({"image_input_enabled": True}, [previous, trigger])
        plugin = LlmPlugin(ctx)

        messages = await plugin._build_messages(trigger, "look")
        user_messages = [item for item in messages if item["role"] == "user"]

        self.assertIsInstance(user_messages[0]["content"], str)
        self.assertIsInstance(user_messages[1]["content"], list)
        self.assertEqual(user_messages[1]["content"][0]["type"], "text")
        self.assertEqual(user_messages[1]["content"][1]["type"], "image_url")
        self.assertEqual(user_messages[1]["content"][1]["image_url"]["detail"], "auto")
        self.assertTrue(user_messages[1]["content"][1]["image_url"]["url"].startswith("data:image/png;base64,"))

    async def test_count_size_and_type_limits_omit_invalid_images(self):
        oversized = b"\x89PNG\r\n\x1a\n" + (b"x" * 1024)
        trigger = message(2, images=[
            {"url": data_url("image/png", PNG)},
            {"url": data_url("image/png", oversized)},
            {"url": data_url("image/gif", GIF)},
        ])
        ctx = FakeContext({
            "image_input_enabled": True,
            "max_images_per_request": 2,
            "max_image_bytes": 1024,
        })
        plugin = LlmPlugin(ctx)

        parts = await plugin._trigger_image_parts(trigger)

        self.assertEqual(len(parts), 1)
        self.assertTrue(any("size limit" in item for item in ctx.logs))

    async def test_file_identifier_can_refresh_through_napcat(self):
        ctx = FakeContext({"image_input_enabled": True})
        ctx.napcat_responses.append({
            "status": "ok",
            "data": {"url": "https://gchat.qpic.cn/refreshed.png"},
        })
        plugin = LlmPlugin(ctx)
        plugin._candidate_data_url = AsyncMock(return_value=data_url("image/png", PNG))

        value = await plugin._image_data_url(object(), {"file": "opaque-file-id"}, 1024, 5)

        self.assertTrue(value.startswith("data:image/png;base64,"))
        plugin._candidate_data_url.assert_awaited_once_with(
            ANY,
            "https://gchat.qpic.cn/refreshed.png",
            1024,
        )

    async def test_redirect_to_unapproved_host_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "host is not allowed"):
            await LlmPlugin._download_remote_image(
                RedirectSession(),
                "https://gchat.qpic.cn/start.png",
                1024,
            )

    def test_unsupported_data_url_type_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported image type"):
            LlmPlugin._decode_image_data_url(data_url("image/svg+xml", b"<svg/>"), 1024)


class LlmImageFallbackTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = FakeContext()
        self.plugin = LlmPlugin(self.ctx)
        self.multimodal = [{
            "role": "user",
            "content": [
                {"type": "text", "text": "describe"},
                {"type": "image_url", "image_url": {"url": data_url("image/png", PNG)}},
            ],
        }]

    async def test_capability_error_retries_without_images(self):
        self.plugin._post_llm = AsyncMock(side_effect=[LlmRequestError(400, "unsupported"), "ok"])

        result = await self.plugin._call_llm("key", self.multimodal)

        self.assertEqual(result, "ok")
        self.assertEqual(self.plugin._post_llm.await_count, 2)
        fallback_messages = self.plugin._post_llm.await_args_list[1].args[1]
        self.assertEqual(fallback_messages, [{"role": "user", "content": "describe"}])
        self.assertEqual(self.multimodal, fallback_messages)
        self.assertTrue(any("retrying without images" in item for item in self.ctx.logs))

    async def test_rate_limit_does_not_retry(self):
        self.plugin._post_llm = AsyncMock(side_effect=LlmRequestError(429, "limited"))

        with self.assertRaises(LlmRequestError):
            await self.plugin._call_llm("key", self.multimodal)

        self.assertEqual(self.plugin._post_llm.await_count, 1)

    def test_oracle_and_prompt_accounting_ignore_image_data(self):
        self.assertEqual(self.plugin._messages_chars(self.multimodal), len("describe"))

        oracle = self.plugin._build_oracle_messages("question", self.multimodal)
        combined = "\n".join(item["content"] for item in oracle)

        self.assertIn("describe", combined)
        self.assertNotIn("base64", combined)


if __name__ == "__main__":
    unittest.main()
