import tempfile
import unittest
from pathlib import Path

from plugins.werewolf.main import WerewolfPlugin


class FakeContext:
    def __init__(self, config=None):
        self.config = config or {}
        self.sent = []
        self.logs = []
        self.failures = {}

    async def send_message(self, chat_id, text, reply_to=None):
        remaining = self.failures.get(chat_id, 0)
        if remaining:
            self.failures[chat_id] = remaining - 1
            raise RuntimeError("planned send failure")
        self.sent.append({"chat_id": chat_id, "text": text, "reply_to": reply_to})

    def log(self, message):
        self.logs.append(message)


class StableRandom:
    def shuffle(self, values):
        return None

    def choice(self, values):
        return values[0]


class WerewolfPluginTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ctx = FakeContext()
        self.state_path = Path(self.tmp.name) / "state.json"
        self.plugin = WerewolfPlugin(self.ctx, state_path=self.state_path, rng=StableRandom())
        self.message_number = 0

    async def group(self, user_id, content, name=None, group_id=123):
        self.message_number += 1
        message = {
            "message_id": f"g-{self.message_number}",
            "chat_id": f"group_{group_id}",
            "type": "group",
            "sender_id": str(user_id),
            "sender_name": name or f"P{user_id}",
            "content": content,
        }
        await self.plugin.handle_event({"type": "message", "message": message}, self.ctx)

    async def private(self, user_id, content, name=None):
        self.message_number += 1
        message = {
            "message_id": f"p-{self.message_number}",
            "chat_id": f"private_{user_id}",
            "type": "private",
            "sender_id": str(user_id),
            "sender_name": name or f"P{user_id}",
            "content": content,
        }
        await self.plugin.handle_event({"type": "message", "message": message}, self.ctx)

    async def configured_six_player_game(self, start=True):
        await self.group(1, "/wolf 创建", "Host")
        for user_id in range(2, 7):
            await self.group(user_id, "/wolf 加入")
        await self.group(1, "/wolf 配置", "Host")
        await self.group(1, "/wolf 角色 村民=2 狼人=2 预言家=1 女巫=1", "Host")
        await self.group(1, "/wolf 平票 2", "Host")
        await self.group(1, "/wolf 女巫自救 1", "Host")
        await self.group(1, "/wolf 女巫双药 否", "Host")
        await self.group(1, "/wolf 胜利 屠边", "Host")
        if start:
            await self.group(1, "/wolf 开始", "Host")
        return self.plugin.state["games"]["group_123"]

    async def reach_first_day(self):
        game = await self.configured_six_player_game(start=True)
        # StableRandom leaves the role order unchanged: wolves are seats 3 and 4,
        # the seer is seat 5, and the witch is seat 6.
        await self.private(3, "/wolf 刀 1")
        await self.private(4, "/wolf 刀 1")
        await self.private(5, "/wolf 查验 3")
        self.assertEqual(game["phase"], "witch")
        await self.private(6, "/wolf 过")
        self.assertEqual(game["phase"], "discussion")
        return game

    async def test_creator_is_host_and_seat_one_player(self):
        await self.group(100, "/wolf 创建", "Alice")
        game = self.plugin.state["games"]["group_123"]

        self.assertEqual(game["host_id"], "100")
        self.assertEqual(game["players"][0]["user_id"], "100")
        self.assertEqual(game["players"][0]["seat"], 1)

        await self.group(100, "/wolf 退出", "Alice")
        self.assertEqual(len(game["players"]), 1)
        self.assertIn("房主不能单独退出", self.ctx.sent[-1]["text"])

    async def test_configured_command_prefix_is_used_in_help(self):
        context = FakeContext({"command_prefix": "/ww"})
        plugin = WerewolfPlugin(context, state_path=Path(self.tmp.name) / "custom.json", rng=StableRandom())
        message = {
            "message_id": "custom-prefix",
            "chat_id": "group_456",
            "type": "group",
            "sender_id": "1",
            "sender_name": "Host",
            "content": "/ww 帮助",
        }

        await plugin.handle_event({"type": "message", "message": message}, context)

        self.assertIn("/ww 创建", context.sent[0]["text"])
        self.assertNotIn("/wolf", context.sent[0]["text"])

    async def test_start_introduces_rules_settings_commands_and_night_roster(self):
        game = await self.configured_six_player_game(start=True)

        group_texts = [item["text"] for item in self.ctx.sent if item["chat_id"] == "group_123"]
        rules_index = next(i for i, text in enumerate(group_texts) if text.startswith("【狼人杀规则】"))
        settings_index = next(i for i, text in enumerate(group_texts) if text.startswith("【本局设置】"))
        commands_index = next(i for i, text in enumerate(group_texts) if text.startswith("【命令列表】"))
        night_index = next(i for i, text in enumerate(group_texts) if text.startswith("第 1 夜开始"))

        self.assertLess(rules_index, settings_index)
        self.assertLess(settings_index, commands_index)
        self.assertLess(commands_index, night_index)
        self.assertIn("胜利条件：屠边", group_texts[settings_index])
        self.assertIn("狼聊 <内容>", group_texts[commands_index])
        self.assertIn("1号 Host（存活）", group_texts[night_index])
        self.assertIn("6号 P6（存活）", group_texts[night_index])
        self.assertEqual(game["phase"], "night_actions")

        identity_chats = {
            item["chat_id"] for item in self.ctx.sent
            if item["text"].startswith("你是 ")
        }
        self.assertEqual(identity_chats, {f"temp_123_{uid}" for uid in range(1, 7)})

    async def test_failed_identity_delivery_retries_without_reshuffle(self):
        await self.configured_six_player_game(start=False)
        self.ctx.failures["temp_123_3"] = 1

        await self.group(1, "/wolf 开始", "Host")
        game = self.plugin.state["games"]["group_123"]
        roles_before = [player["role"] for player in game["players"]]
        self.assertEqual(game["phase"], "dealing")
        self.assertFalse(game["players"][2]["identity_delivered"])

        await self.group(1, "/wolf 重发", "Host")
        self.assertEqual([player["role"] for player in game["players"]], roles_before)
        self.assertTrue(game["players"][2]["identity_delivered"])
        self.assertEqual(game["phase"], "night_actions")

    async def test_night_actions_are_private_and_dead_host_can_advance(self):
        game = await self.reach_first_day()

        self.assertFalse(game["players"][0]["alive"])
        seer_messages = [
            item["text"] for item in self.ctx.sent
            if item["chat_id"] == "temp_123_5"
        ]
        self.assertTrue(any("属于狼人阵营" in text for text in seer_messages))
        group_texts = [item["text"] for item in self.ctx.sent if item["chat_id"] == "group_123"]
        self.assertTrue(any("昨夜死亡：1号 Host" in text for text in group_texts))

        await self.group(1, "/wolf 推进", "Host")
        self.assertEqual(game["phase"], "vote")

    async def test_day_threshold_starts_hidden_vote(self):
        game = await self.reach_first_day()
        await self.group(2, "/wolf 结束发言")
        await self.group(3, "/wolf 结束发言")
        self.assertEqual(game["phase"], "discussion")
        await self.group(4, "/wolf 结束发言")
        self.assertEqual(game["phase"], "vote")

        for user_id in range(2, 7):
            await self.private(user_id, "/wolf 投票 2")

        group_texts = [item["text"] for item in self.ctx.sent if item["chat_id"] == "group_123"]
        self.assertTrue(any("2号 P2 被公投出局" in text for text in group_texts))
        self.assertFalse(any("投给" in text for text in group_texts))
        self.assertEqual(game["phase"], "ended")
        self.assertEqual(game["winner"], "wolves")

    async def test_second_night_roster_includes_public_death_status(self):
        game = await self.reach_first_day()
        for user_id in (2, 3, 4):
            await self.group(user_id, "/wolf 结束发言")
        for user_id in range(2, 7):
            await self.private(user_id, "/wolf 弃票")

        self.assertEqual(game["phase"], "night_actions")
        self.assertEqual(game["night"], 2)
        group_texts = [item["text"] for item in self.ctx.sent if item["chat_id"] == "group_123"]
        second_night = next(text for text in group_texts if text.startswith("第 2 夜开始"))
        self.assertIn("1号 Host（已死亡）", second_night)
        self.assertIn("2号 P2（存活）", second_night)

    async def test_restart_restores_private_game_context(self):
        game = await self.configured_six_player_game(start=True)
        await self.private(3, "/wolf 刀 1")

        restarted_context = FakeContext()
        restarted = WerewolfPlugin(restarted_context, state_path=self.state_path, rng=StableRandom())
        restored = restarted.state["games"]["group_123"]

        self.assertEqual(restored["phase"], "night_actions")
        self.assertEqual(restored["night_actions"]["wolves"]["3"], "1")
        self.assertEqual(restarted._active_game_for_user("3")["chat_id"], "group_123")
        self.assertEqual([player["role"] for player in restored["players"]], [player["role"] for player in game["players"]])

    async def test_death_skills_poison_and_lover_chain(self):
        game = await self.configured_six_player_game(start=True)
        game["players"][0]["role"] = "hunter"
        game["players"][1]["role"] = "wolf"
        game["lovers"] = ["1", "2"]
        game["lovers_cross"] = True
        game["pending_shots"] = []

        dead = self.plugin._apply_deaths(game, [("1", "poison")])
        self.assertEqual({player["user_id"] for player in dead}, {"1", "2"})
        self.assertEqual(game["pending_shots"], [])
        self.assertFalse(game["players"][1]["alive"])

    async def test_nine_role_night_supports_cupid_guard_and_death_shot(self):
        await self.group(1, "/wolf 创建", "Host")
        for user_id in range(2, 10):
            await self.group(user_id, "/wolf 加入")
        await self.group(1, "/wolf 配置", "Host")
        await self.group(
            1,
            "/wolf 角色 村民=1 狼人=1 预言家=1 女巫=1 猎人=1 守卫=1 白痴=1 狼王=1 丘比特=1",
            "Host",
        )
        await self.group(1, "/wolf 平票 1", "Host")
        await self.group(1, "/wolf 女巫自救 3", "Host")
        await self.group(1, "/wolf 女巫双药 是", "Host")
        await self.group(1, "/wolf 胜利 屠城", "Host")
        await self.group(1, "/wolf 开始", "Host")
        game = self.plugin.state["games"]["group_123"]

        # Stable role order: 2 wolf, 3 seer, 4 witch, 5 hunter,
        # 6 guard, 7 idiot, 8 wolf king, 9 Cupid.
        await self.private(9, "/wolf 连结 2 3")
        await self.private(6, "/wolf 守护 1")
        await self.private(2, "/wolf 刀 5")
        await self.private(8, "/wolf 刀 5")
        await self.private(3, "/wolf 查验 2")
        self.assertEqual(game["phase"], "witch")
        self.assertTrue(game["lovers_cross"])
        await self.private(4, "/wolf 过")
        self.assertEqual(game["phase"], "death_shot")
        self.assertEqual(game["pending_shots"], ["5"])

        await self.private(5, "/wolf 开枪 2")
        self.assertFalse(self.plugin._player(game, "2")["alive"])
        self.assertFalse(self.plugin._player(game, "3")["alive"])
        self.assertEqual(game["phase"], "discussion")
        group_texts = [item["text"] for item in self.ctx.sent if item["chat_id"] == "group_123"]
        self.assertTrue(any("情侣殉情：3号 P3" in text for text in group_texts))

    async def test_victory_conditions_and_cross_camp_lovers(self):
        game = await self.configured_six_player_game(start=True)
        # Stable roles: villagers 1-2, wolves 3-4, seer 5, witch 6.
        game["players"][0]["alive"] = False
        game["players"][1]["alive"] = False
        self.assertEqual(self.plugin._winner(game), "wolves")

        game["settings"]["victory"] = "slaughter_city"
        self.assertIsNone(self.plugin._winner(game))

        for player in game["players"]:
            player["alive"] = player["user_id"] in ("3", "5", "6")
        game["lovers"] = ["3", "5"]
        game["lovers_cross"] = True
        self.assertIsNone(self.plugin._winner(game))
        game["players"][5]["alive"] = False
        self.assertEqual(self.plugin._winner(game), "lovers")


if __name__ == "__main__":
    unittest.main()
