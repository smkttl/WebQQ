import base64
import time
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


def message(
    message_id,
    content="look",
    images=None,
    self_sent=False,
    source="qq",
    chat_id="group_1",
    chat_type=None,
    sender_name=None,
    sender_id=None,
):
    return {
        "chat_id": chat_id,
        "type": chat_type or ("private" if chat_id.startswith("private_") else "group"),
        "message_id": message_id,
        "sender_id": str(sender_id if sender_id is not None else message_id),
        "sender_name": "User{}".format(message_id) if sender_name is None else sender_name,
        "time": message_id,
        "content": content,
        "images": list(images or []),
        "self": self_sent,
        "source": source,
    }


class LlmSimpleModeMessageTests(unittest.IsolatedAsyncioTestCase):
    async def _prompt_text(self, plugin, trigger, prompt="hi"):
        messages = await plugin._build_messages(trigger, prompt, simple=True)
        self.assertEqual(len(messages), 1)
        content = messages[0]["content"]
        if isinstance(content, list):
            return str(content[0].get("text") or "")
        return content

    async def test_build_messages_default_keeps_json_instruction(self):
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({}, [trigger]))

        messages = await plugin._build_messages(trigger, "hi")
        system_text = "\n".join(
            item["content"] for item in messages if item["role"] == "system"
        )

        self.assertIn("canonical JSON action arrays", system_text)

    async def test_build_messages_puts_all_system_messages_before_history(self):
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({}, [trigger]))

        messages = await plugin._build_messages(trigger, "hi")
        roles = [item["role"] for item in messages]
        first_chat_message = next(
            index for index, role in enumerate(roles) if role != "system"
        )

        self.assertNotIn("system", roles[first_chat_message + 1:])
        self.assertIn(
            "Current time",
            "\n".join(
                item["content"]
                for item in messages[:first_chat_message]
            ),
        )

    async def test_chat_payload_moves_late_system_messages_to_front(self):
        plugin = LlmPlugin(FakeContext({"model": "test-model"}, []))
        messages = [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "answer"},
            {"role": "system", "content": "Oracle round limit reached."},
        ]

        payload = plugin._chat_payload(messages)

        self.assertEqual(
            payload["messages"],
            [
                {
                    "role": "system",
                    "content": "Oracle round limit reached.",
                },
                {"role": "user", "content": "question"},
                {"role": "assistant", "content": "answer"},
            ],
        )

    async def test_chat_payload_coalesces_system_messages_for_strict_models(self):
        plugin = LlmPlugin(FakeContext({"model": "test-model"}, []))
        messages = [
            {"role": "system", "content": "persona"},
            {"role": "system", "content": "output rules"},
            {"role": "user", "content": "question"},
            {"role": "system", "content": "oracle note"},
        ]

        payload = plugin._chat_payload(messages)

        self.assertEqual(
            payload["messages"],
            [
                {
                    "role": "system",
                    "content": "persona\n\noutput rules\n\noracle note",
                },
                {"role": "user", "content": "question"},
            ],
        )

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
        self.assertIn("像群友一样续聊", messages[0]["content"])
        self.assertIn("要回复的消息", messages[0]["content"])
        self.assertNotIn("valid JSON", messages[0]["content"])
        self.assertNotIn("canonical", messages[0]["content"])

    async def test_build_messages_simple_renders_assistant_history_plain(self):
        previous = message(1, content="Hello there", self_sent=True, source="plugin:llm")
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, [previous, trigger]))

        content = await self._prompt_text(plugin, trigger)

        self.assertIn("You: Hello there", content)
        self.assertNotIn('[{"type"', content)

    async def test_simple_prompt_contains_no_old_message_markers(self):
        previous = message(1, content="older")
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, [previous, trigger]))

        content = await self._prompt_text(plugin, trigger)

        self.assertNotIn("(message_id=", content)
        self.assertNotIn("User(name=", content)

    async def test_named_numeric_sender_uses_mention_syntax(self):
        previous = message(1, content="hello", sender_name="Alice", sender_id=42)
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, [previous, trigger]))

        content = await self._prompt_text(plugin, trigger)

        self.assertIn("@[42](Alice): hello", content)

    async def test_sender_fallbacks_are_compact(self):
        messages = [
            message(1, content="one", sender_name="", sender_id=7),
            message(2, content="two", sender_name="Bob", sender_id=""),
            message(3, content="three", sender_name="Carol", sender_id="abc"),
            message(4, content="four", self_sent=True, source="plugin:llm"),
        ]
        trigger = message(9, content="go", sender_name="", sender_id="")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, messages))

        content = await self._prompt_text(plugin, trigger, prompt=trigger["content"])

        self.assertIn("@[7]: one", content)
        self.assertIn("Bob: two", content)
        self.assertIn("Carol: three", content)
        self.assertIn("You: four", content)
        self.assertIn("User: go", content)

    async def test_sender_name_newlines_are_normalized(self):
        previous = message(1, content="hello", sender_name="A\nB\r\nC", sender_id=5)
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, [previous, trigger]))

        content = await self._prompt_text(plugin, trigger)

        self.assertIn("@[5](A B C): hello", content)
        self.assertNotIn("@[5](A\n", content)

    async def test_trigger_is_absent_from_history_and_rendered_once(self):
        history = [
            message(1, content="old"),
            message(2, content="hi", sender_name="Bob", sender_id=22),
            message(3, content="after"),
        ]
        trigger = history[1]
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, history))

        content = await self._prompt_text(plugin, trigger)
        before_final, final = content.split("要回复的消息：", 1)

        self.assertNotIn("@[22](Bob): hi", before_final)
        self.assertEqual(final.count("@[22](Bob): hi"), 1)

    async def test_trigger_not_newest_is_still_final_section(self):
        history = [
            message(1, content="one"),
            message(2, content="two"),
            message(3, content="three"),
            message(4, content="four"),
        ]
        trigger = history[1]
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, history))

        content = await self._prompt_text(plugin, trigger, prompt=trigger["content"])
        before_final, final = content.split("要回复的消息：", 1)

        self.assertIn("@[4](User4): four", before_final)
        self.assertNotIn("@[2](User2): two", before_final)
        self.assertIn("@[2](User2): two", final)

    async def test_simple_history_limit_keeps_newest_in_order(self):
        history = [message(index) for index in range(1, 6)]
        trigger = history[-1]
        plugin = LlmPlugin(FakeContext({
            "simple_mode": True,
            "simple_history_limit": 3,
        }, history))

        content = await self._prompt_text(plugin, trigger)

        self.assertIn("@[3](User3): look", content)
        self.assertIn("@[4](User4): look", content)
        self.assertNotIn("@[1](User1): look", content)

    async def test_simple_history_max_chars_is_enforced_without_losing_trigger(self):
        history = [message(index, content="x" * 50) for index in range(1, 6)]
        trigger = history[-1]
        plugin = LlmPlugin(FakeContext({
            "simple_mode": True,
            "simple_history_limit": 15,
            "simple_history_max_chars": 100,
        }, history))

        content = await self._prompt_text(plugin, trigger)

        self.assertIn("@[5](User5): hi", content)
        self.assertIn("@[4](User4): " + "x" * 50, content)
        self.assertNotIn("@[3](User3):", content)

    async def test_max_prompt_chars_caps_long_history_and_guidance(self):
        history = [message(index, content="x" * 500) for index in range(1, 6)]
        trigger = message(9, content="y" * 400)
        plugin = LlmPlugin(FakeContext({
            "simple_mode": True,
            "max_prompt_chars": 1000,
            "simple_history_limit": 15,
            "simple_history_max_chars": 10000,
            "runtime_guidance": [{"chat_id": "group_1", "text": "g" * 2000}],
        }, history))

        content = await self._prompt_text(plugin, trigger)

        self.assertLessEqual(len(content), 1000)
        self.assertIn("要回复的消息", content)
        self.assertNotIn("g" * 2000, content)

    async def test_fixed_refusals_removed_but_natural_and_trigger_remain(self):
        history = [
            message(1, content="抱歉，我无法回答这个问题", sender_name="A"),
            message(2, content="不行", sender_name="B"),
            message(3, content="对不起，我暂时无法回答这个问题！", sender_name="C"),
            message(4, content="抱歉，我无法回答这个问题", sender_name="D"),
        ]
        trigger = history[-1]
        plugin = LlmPlugin(FakeContext({"simple_mode": True}, history))

        content = await self._prompt_text(plugin, trigger, prompt=trigger["content"])
        before_final, final = content.split("要回复的消息：", 1)

        self.assertNotIn("无法回答", before_final)
        self.assertIn("不行", before_final)
        self.assertIn("抱歉，我无法回答这个问题", final)

    async def test_runtime_guidance_deduplicates_and_ignores_expired(self):
        now = time.time()
        trigger = message(2, content="hi")
        plugin = LlmPlugin(FakeContext({
            "simple_mode": True,
            "runtime_guidance": [
                {"time": 1, "chat_id": "group_1", "text": "first"},
                {"time": 2, "chat_id": "group_1", "text": "first"},
                {"time": 3, "chat_id": "group_1", "text": "expired", "expires_at": now - 1},
                {"time": 4, "chat_id": "group_1", "text": "persistent"},
            ],
        }, [trigger]))

        content = await self._prompt_text(plugin, trigger)

        self.assertEqual(content.count("first"), 1)
        self.assertNotIn("expired", content)
        self.assertIn("persistent", content)

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


class LlmSimpleModeReplyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.ctx = FakeContext({
            "simple_mode": True,
            "send_errors_to_chat": False,
            "api_key": "test-key",
            "model": "test-model",
        }, [])
        self.plugin = LlmPlugin(self.ctx)
        self.trigger = message(42, content="hi")

    async def test_sends_extracted_text_and_quotes_group_trigger(self):
        self.plugin._call_llm = AsyncMock(return_value="  hello world  ")
        self.plugin._parse_actions = AsyncMock()

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "hello world", {"reply_to": "42"})])
        self.plugin._parse_actions.assert_not_awaited()

    async def test_multiline_reply_quotes_only_first_line(self):
        self.plugin._call_llm = AsyncMock(return_value="first line\n\nsecond line\n  third line  ")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [
            ("group_1", "first line", {"reply_to": "42"}),
            ("group_1", "second line", {}),
            ("group_1", "third line", {}),
        ])

    async def test_sends_json_shaped_output_as_plain_text(self):
        payload = '[{"type":"message","reply_to":0,"text":"hi"}]'
        self.plugin._call_llm = AsyncMock(return_value=payload)

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", payload, {"reply_to": "42"})])

    async def test_private_chat_output_never_quotes(self):
        private_trigger = message(42, content="hi", chat_id="private_1", chat_type="private")
        self.plugin._call_llm = AsyncMock(return_value="hello")

        await self.plugin._reply(private_trigger, "hi")

        self.assertEqual(self.ctx.sent, [("private_1", "hello", {})])

    async def test_quote_requires_valid_event_trigger_id(self):
        base = {"chat_id": "group_1", "type": "group"}
        self.assertEqual(self.plugin._simple_reply_to({**base, "message_id": 42}), "42")
        for invalid in (0, -1, "abc", "", None):
            self.assertIsNone(self.plugin._simple_reply_to({**base, "message_id": invalid}))

    async def test_simple_reply_to_trigger_false_restores_unquoted(self):
        self.ctx.config["simple_reply_to_trigger"] = False
        self.plugin._call_llm = AsyncMock(return_value="hello")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "hello", {})])

    async def test_mention_only_output_sends_nothing(self):
        self.plugin._call_llm = AsyncMock(return_value="@[857005487]")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])
        self.assertTrue(any("empty" in item for item in self.ctx.logs))

    async def test_named_mention_only_output_sends_nothing(self):
        self.plugin._call_llm = AsyncMock(return_value="  @[857005487](colin1112)   ")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])
        self.assertTrue(any("empty" in item for item in self.ctx.logs))

    async def test_multiple_mention_only_line_sends_nothing(self):
        self.plugin._call_llm = AsyncMock(return_value="@[1](A) @[2](B)")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])
        self.assertTrue(any("empty" in item for item in self.ctx.logs))

    async def test_multiline_mention_only_lines_are_skipped(self):
        self.plugin._call_llm = AsyncMock(return_value="@[1]\n  @[2](B)  \nhello")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "hello", {"reply_to": "42"})])

    async def test_first_valid_line_after_mention_gets_quote(self):
        self.plugin._call_llm = AsyncMock(return_value="@[857005487]\nhello")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "hello", {"reply_to": "42"})])

    async def test_mention_with_text_is_sent(self):
        self.plugin._call_llm = AsyncMock(return_value="@[857005487] 不知道")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "@[857005487] 不知道", {"reply_to": "42"})])

    async def test_mention_label_with_text_is_sent(self):
        self.plugin._call_llm = AsyncMock(return_value="@[857005487](colin1112): 炸完了")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "@[857005487](colin1112): 炸完了", {"reply_to": "42"})])

    async def test_full_mode_keeps_mention_only_action_text(self):
        ctx = FakeContext({
            "simple_mode": False,
            "send_errors_to_chat": False,
            "api_key": "test-key",
            "model": "test-model",
        }, [self.trigger])
        plugin = LlmPlugin(ctx)
        plugin._call_llm = AsyncMock(
            return_value='[{"type":"message","reply_to":0,"text":"@[857005487]"}]'
        )

        await plugin._reply(self.trigger, "hi")

        self.assertEqual(ctx.sent, [("group_1", "@[857005487]", {})])

    async def test_portal_guidance_remains_unquoted(self):
        portal_trigger = {
            "chat_id": "group_1",
            "type": "group",
            "message_id": 0,
            "sender_name": "system",
            "sender_id": "system",
            "content": "guidance",
            "self": False,
            "source": "ui_portal",
        }
        self.plugin._call_llm = AsyncMock(return_value="hello")

        await self.plugin._reply(portal_trigger, "guidance")

        self.assertEqual(self.ctx.sent, [("group_1", "hello", {})])

    async def test_fixed_refusal_retries_with_clean_history_then_sends(self):
        refusal = "抱歉，我无法回答这个问题"
        self.plugin._call_llm = AsyncMock(side_effect=[refusal, "好的"])

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [("group_1", "好的", {"reply_to": "42"})])
        self.assertEqual(self.plugin._call_llm.await_count, 2)
        self.assertEqual(self.plugin._first_pass_fixed_refusals, 1)

    async def test_second_fixed_refusal_is_suppressed(self):
        refusal = "对不起，我暂时无法回答这个问题"
        self.plugin._call_llm = AsyncMock(side_effect=[refusal, refusal])

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.ctx.sent, [])
        self.assertEqual(self.plugin._retry_fixed_refusals, 1)
        self.assertTrue(any("suppressed" in item for item in self.ctx.logs))

    async def test_simple_max_tokens_is_sent_only_for_simple_inference(self):
        self.ctx.config["simple_max_tokens"] = 37
        self.ctx.config["max_tokens"] = 800
        self.plugin._post_llm = AsyncMock(return_value="hello")

        await self.plugin._reply(self.trigger, "hi")

        self.assertEqual(self.plugin._post_llm.await_count, 1)
        self.assertIs(self.plugin._post_llm.await_args.kwargs["simple"], True)
        self.assertEqual(self.plugin._chat_payload([], simple=True)["max_tokens"], 37)
        self.assertEqual(self.plugin._chat_payload([], simple=False)["max_tokens"], 800)

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
