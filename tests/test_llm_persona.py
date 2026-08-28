import unittest

from plugins.llm.main import LlmPlugin


COMMON = "COMMON_PERSONA"
GROUP = "GROUP_PERSONA"
PRIVATE = "PRIVATE_PERSONA"


class FakeContext:
    def __init__(self, config=None, messages=None):
        self.config = dict(config or {})
        self.messages = list(messages or [])
        self.logs = []

    def get_messages(self, chat_id, limit=50, before=None):
        return self.messages[-limit:]

    def get_self_user(self):
        return {}

    def log(self, message):
        self.logs.append(str(message))


def message(chat_id, message_id=1, content="hi"):
    return {
        "chat_id": chat_id,
        "type": "group" if chat_id.startswith("group_") else "private",
        "message_id": message_id,
        "sender_id": str(message_id),
        "sender_name": "User",
        "time": message_id,
        "content": content,
        "self": False,
        "source": "qq",
    }


def base_config():
    return {
        "persona_prompt": COMMON,
        "persona_prompt_group": GROUP,
        "persona_prompt_private": PRIVATE,
    }


class PersonaSelectionTests(unittest.IsolatedAsyncioTestCase):
    async def _system_text(self, plugin, trigger):
        messages = await plugin._build_messages(trigger, "hi")
        return "\n".join(item["content"] for item in messages if item["role"] == "system")

    async def test_complex_group_includes_group_persona(self):
        plugin = LlmPlugin(FakeContext(base_config(), [message("group_1")]))

        system_text = await self._system_text(plugin, message("group_1"))

        self.assertIn(COMMON, system_text)
        self.assertIn(GROUP, system_text)
        self.assertNotIn(PRIVATE, system_text)

    async def test_complex_private_includes_private_persona(self):
        plugin = LlmPlugin(FakeContext(base_config(), [message("private_1")]))

        system_text = await self._system_text(plugin, message("private_1"))

        self.assertIn(COMMON, system_text)
        self.assertIn(PRIVATE, system_text)
        self.assertNotIn(GROUP, system_text)

    async def test_complex_other_chat_common_only(self):
        plugin = LlmPlugin(FakeContext(base_config(), [message("temp_1_2")]))

        system_text = await self._system_text(plugin, message("temp_1_2"))

        self.assertIn(COMMON, system_text)
        self.assertNotIn(GROUP, system_text)
        self.assertNotIn(PRIVATE, system_text)

    async def test_simple_group_includes_group_persona(self):
        plugin = LlmPlugin(FakeContext(base_config(), [message("group_1")]))

        messages = await plugin._build_messages(message("group_1"), "hi", simple=True)
        content = messages[0]["content"]

        self.assertIn(COMMON, content)
        self.assertIn(GROUP, content)
        self.assertNotIn(PRIVATE, content)

    async def test_simple_private_includes_private_persona(self):
        plugin = LlmPlugin(FakeContext(base_config(), [message("private_1")]))

        messages = await plugin._build_messages(message("private_1"), "hi", simple=True)
        content = messages[0]["content"]

        self.assertIn(COMMON, content)
        self.assertIn(PRIVATE, content)
        self.assertNotIn(GROUP, content)

    async def test_empty_persona_parts_are_skipped(self):
        plugin = LlmPlugin(FakeContext({"persona_prompt_group": GROUP}, [message("group_1")]))

        messages = await plugin._build_messages(message("group_1"), "hi", simple=True)
        content = messages[0]["content"]

        self.assertIn(GROUP, content)
        self.assertNotIn(COMMON, content)
        self.assertNotIn(PRIVATE, content)


if __name__ == "__main__":
    unittest.main()
