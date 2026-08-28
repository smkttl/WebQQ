import base64
import unittest
from unittest.mock import AsyncMock

from plugins.llm.main import LlmPlugin


PNG = b"\x89PNG\r\n\x1a\n" + b"test"


def data_url(mime_type, body):
    return "data:{};base64,{}".format(mime_type, base64.b64encode(body).decode("ascii"))


class FakeContext:
    def __init__(self, config=None, messages=None):
        self.config = dict(config or {})
        self.messages = list(messages or [])
        self.logs = []
        self.sent = []

    def get_messages(self, chat_id, limit=50, before=None):
        return self.messages[-limit:]

    def get_self_user(self):
        return {}

    def log(self, message):
        self.logs.append(str(message))

    async def send_message(self, chat_id, text, **kwargs):
        self.sent.append((chat_id, text, kwargs))


def message(message_id, content="look", images=None, self_sent=False, source="qq"):
    return {
        "chat_id": "group_1",
        "type": "group",
        "message_id": message_id,
        "sender_id": str(message_id),
        "sender_name": "User{}".format(message_id),
        "time": message_id,
        "content": content,
        "images": list(images or []),
        "self": self_sent,
        "source": source,
    }


class LlmSimpleModeMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_build_messages_default_keeps_json_instruction(self):
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({}, [trigger]))

        messages = await plugin._build_messages(trigger, "hi")
        system_text = "\n".join(
            item["content"] for item in messages if item["role"] == "system"
        )

        self.assertIn("canonical JSON action arrays", system_text)

    async def test_build_messages_simple_is_single_user_message(self):
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({
            "simple_mode": True,
            "reply_prompt": "You must output valid JSON only, with no prose before or after it.",
        }, [trigger]))

        messages = await plugin._build_messages(trigger, "hi", simple=True)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["role"], "user")
        self.assertIsInstance(messages[0]["content"], str)
        self.assertNotIn("valid JSON", messages[0]["content"])
        self.assertNotIn("canonical", messages[0]["content"])
        self.assertIn("plain chat text only", messages[0]["content"])

    async def test_build_messages_simple_renders_assistant_history_plain(self):
        previous = message(1, content="Hello there", self_sent=True, source="plugin:llm")
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, [previous, trigger]))

        messages = await plugin._build_messages(trigger, "hi", simple=True)

        self.assertEqual(len(messages), 1)
        self.assertIn("You: Hello there", messages[0]["content"])
        self.assertNotIn('[{"type"', messages[0]["content"])

    async def test_build_messages_simple_keeps_image_support(self):
        trigger = message(2, content="look", images=[{"url": data_url("image/png", PNG)}])
        plugin = LlmPlugin(FakeContext({"simple_mode": True, "image_input_enabled": True}, [trigger]))

        messages = await plugin._build_messages(trigger, "look", simple=True)

        self.assertEqual(len(messages), 1)
        self.assertIsInstance(messages[0]["content"], list)
        self.assertEqual(messages[0]["content"][0]["type"], "text")
        self.assertIn("look", messages[0]["content"][0]["text"])
        self.assertTrue(
            any(
                isinstance(part, dict) and part.get("type") == "image_url"
                for part in messages[0]["content"]
            )
        )

    async def test_build_messages_simple_drops_images_when_disabled(self):
        trigger = message(2, content="look", images=[{"url": data_url("image/png", PNG)}])
        plugin = LlmPlugin(FakeContext({"simple_mode": True, "image_input_enabled": False}, [trigger]))

        messages = await plugin._build_messages(trigger, "look", simple=True)

        self.assertEqual(len(messages), 1)
        self.assertIsInstance(messages[0]["content"], str)

    async def test_build_messages_simple_includes_context(self):
        previous = message(1, content="older")
        trigger = message(2, content="hi")
        ctx = FakeContext({
            "simple_mode": True,
            "runtime_guidance": [{"time": 1, "chat_id": "group_1", "text": "say hi"}],
        }, [previous, trigger])
        plugin = LlmPlugin(ctx)

        messages = await plugin._build_messages(trigger, "hi", simple=True)
        content = messages[0]["content"]

        self.assertEqual(len(messages), 1)
        self.assertIsInstance(content, str)
        self.assertIn("Current time", content)
        self.assertNotIn("Known chat users", content)
        self.assertNotIn("display_name=Alice", content)
        self.assertIn("say hi", content)
        self.assertIn("(message_id=1) User(name=User1, user_id=1): older", content)
        self.assertIn("(message_id=2) User(name=User2, user_id=2): hi", content)


class LlmSimpleModeReplyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = FakeContext({
            "simple_mode": True,
            "send_errors_to_chat": False,
            "api_key": "test-key",
        }, [])
        self.plugin = LlmPlugin(self.ctx)
        self.trigger = message(42, content="hi")

    async def test_sends_extracted_text_verbatim_without_quote(self):
        self.plugin._call_llm = AsyncMock(return_value="  hello world  ")
        self.plugin._parse_actions = AsyncMock()

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "hello world", {})])
        self.plugin._parse_actions.assert_not_awaited()

    async def test_multiline_reply_sends_each_line_separately(self):
        self.plugin._call_llm = AsyncMock(return_value="first line\n\nsecond line\n  third line  ")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [
            ("group_1", "first line", {}),
            ("group_1", "second line", {}),
            ("group_1", "third line", {}),
        ])

    async def test_sends_json_shaped_output_as_plain_text(self):
        payload = '[{"type":"message","reply_to":0,"text":"hi"}]'
        self.plugin._call_llm = AsyncMock(return_value=payload)

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", payload, {})])

    async def test_empty_output_sends_nothing(self):
        self.plugin._call_llm = AsyncMock(return_value="   ")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])
        self.assertTrue(any("empty" in item for item in self.ctx.logs))

    async def test_failure_respects_send_errors_to_chat(self):
        self.plugin._call_llm = AsyncMock(side_effect=RuntimeError("boom"))

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])

        self.ctx.sent.clear()
        self.ctx.config["send_errors_to_chat"] = True
        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(len(self.ctx.sent), 1)
        self.assertIn("boom", self.ctx.sent[0][1])


if __name__ == "__main__":
    unittest.main()
