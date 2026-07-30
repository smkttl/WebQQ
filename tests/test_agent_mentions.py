import unittest

from plugins.llm.main import LlmPlugin


class AgentMentionTests(unittest.TestCase):
    def test_llm_history_expands_mentions(self):
        plugin = LlmPlugin.__new__(LlmPlugin)
        message = {"content": "Ask @[42]", "mentions": {"42": "Alice"}}
        self.assertEqual(plugin._history_content(message), "Ask @[42](Alice)")

    def test_llm_trigger_strips_named_and_short_mentions(self):
        plugin = LlmPlugin.__new__(LlmPlugin)
        self.assertEqual(plugin._strip_mentions("@[42](Alice) hello @[43]"), "hello")
