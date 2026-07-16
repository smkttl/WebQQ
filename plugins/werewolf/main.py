import asyncio
import json
import math
import os
import random
from pathlib import Path


STATE_VERSION = 1

ROLE_NAMES = {
    "villager": "村民",
    "wolf": "狼人",
    "seer": "预言家",
    "witch": "女巫",
    "hunter": "猎人",
    "guard": "守卫",
    "idiot": "白痴",
    "wolf_king": "狼王",
    "cupid": "丘比特",
}
ROLE_KEYS = {name: key for key, name in ROLE_NAMES.items()}
WOLF_ROLES = {"wolf", "wolf_king"}
DIVINE_ROLES = {"seer", "witch", "hunter", "guard", "idiot", "cupid"}
SPECIAL_ROLES = set(ROLE_NAMES) - {"villager", "wolf"}

ROLE_HELP = {
    "villager": "没有夜间技能，通过发言和投票找出狼人。",
    "wolf": "每晚与狼队友讨论并提交刀人目标。",
    "seer": "每晚查验一名玩家是否属于狼人阵营。",
    "witch": "拥有一瓶解药和一瓶毒药，按本局规则使用。",
    "hunter": "死亡时可以开枪带走一人，但被毒死时不能开枪。",
    "guard": "每晚守护一人，可以自守，但不能连续两晚守同一人。",
    "idiot": "首次被公投出局时翻牌免死，之后失去投票权。",
    "wolf_king": "属于狼人阵营，死亡时可以开枪，但被毒死时不能开枪。",
    "cupid": "首夜连接两名玩家成为情侣。",
}

TIE_POLICIES = {
    "1": "runoff",
    "2": "no_exile",
    "3": "random",
}
TIE_NAMES = {
    "runoff": "平票者再次投票，再次平票则无人出局",
    "no_exile": "平票立即无人出局",
    "random": "从平票者中随机出局",
}
WITCH_SELF_POLICIES = {
    "1": "first_night",
    "2": "never",
    "3": "always",
}
WITCH_SELF_NAMES = {
    "first_night": "仅首夜可以自救",
    "never": "不能自救",
    "always": "任意夜晚可以自救",
}


def setup(ctx):
    return WerewolfPlugin(ctx)


class WerewolfPlugin:
    def __init__(self, ctx, state_path=None, rng=None):
        self.ctx = ctx
        self.prefix = str(ctx.config.get("command_prefix") or "/wolf").strip() or "/wolf"
        self.min_players = self._config_int("min_players", 6, 3, 50)
        self.max_players = self._config_int("max_players", 12, self.min_players, 50)
        try:
            self.day_ready_threshold = float(ctx.config.get("day_ready_threshold", 0.6))
        except (TypeError, ValueError):
            raise ValueError("day_ready_threshold must be a number in (0, 1]")
        if not 0 < self.day_ready_threshold <= 1:
            raise ValueError("day_ready_threshold must be a number in (0, 1]")

        default_path = Path(__file__).resolve().parents[2] / "data" / "werewolf" / "state.json"
        self.state_path = Path(state_path) if state_path else default_path
        self.rng = rng or random.SystemRandom()
        self.lock = asyncio.Lock()
        self.state = self._load_state()

    def _config_int(self, key, default, minimum, maximum):
        try:
            value = int(self.ctx.config.get(key, default))
        except (TypeError, ValueError):
            raise ValueError(f"{key} must be an integer")
        if not minimum <= value <= maximum:
            raise ValueError(f"{key} must be between {minimum} and {maximum}")
        return value

    def _load_state(self):
        if not self.state_path.exists():
            return {"version": STATE_VERSION, "games": {}, "processed_ids": []}
        with open(self.state_path, encoding="utf-8") as f:
            state = json.load(f)
        if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
            raise ValueError("unsupported werewolf state version")
        if not isinstance(state.get("games"), dict) or not isinstance(state.get("processed_ids", []), list):
            raise ValueError("malformed werewolf state")
        state.setdefault("processed_ids", [])
        return state

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.state_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, self.state_path)

    async def handle_event(self, event, ctx):
        if event.get("type") != "message":
            return
        message = event.get("message") or {}
        if message.get("self") or message.get("system") or message.get("recalled"):
            return
        if str(message.get("source") or "").startswith("plugin:"):
            return

        chat_type = str(message.get("type") or "")
        chat_id = str(message.get("chat_id") or "")
        sender_id = str(message.get("sender_id") or message.get("user_id") or "")
        if not sender_id:
            return

        async with self.lock:
            if chat_type == "group" and chat_id.startswith("group_"):
                self._refresh_player_name(chat_id, sender_id, message.get("sender_name"))

            content = str(message.get("content") or "").strip()
            if not (content == self.prefix or content.startswith(self.prefix + " ")):
                return
            message_id = str(message.get("message_id") or "")
            if message_id and message_id in self.state["processed_ids"]:
                return
            if message_id:
                self.state["processed_ids"].append(message_id)
                self.state["processed_ids"] = self.state["processed_ids"][-500:]
                self._save()

            command_text = content[len(self.prefix):].strip()
            parts = command_text.split()
            command = parts[0] if parts else "帮助"
            args = parts[1:]
            if chat_type == "group" and chat_id.startswith("group_"):
                await self._handle_group(message, command, args)
            elif chat_type == "private" or chat_id.startswith("private_"):
                await self._handle_private(message, command, args)

    def _refresh_player_name(self, chat_id, user_id, name):
        game = self.state["games"].get(chat_id)
        player = self._player(game, user_id) if game else None
        name = str(name or "").strip()
        if player and name and player.get("name") != name:
            player["name"] = name
            self._save()

    async def _handle_group(self, message, command, args):
        chat_id = str(message["chat_id"])
        user_id = str(message.get("sender_id") or message.get("user_id"))
        user_name = str(message.get("sender_name") or user_id)
        game = self.state["games"].get(chat_id)

        if command == "帮助":
            await self._safe_send(chat_id, self._command_text())
            return
        if command == "创建":
            await self._create_game(chat_id, user_id, user_name)
            return
        if not game:
            await self._safe_send(chat_id, f"当前群没有游戏。发送 {self.prefix} 创建 开房。")
            return

        if command == "加入":
            await self._join_game(game, user_id, user_name)
        elif command == "退出":
            await self._leave_game(game, user_id)
        elif command == "名单":
            await self._safe_send(chat_id, self._seat_list(game, include_status=game["phase"] != "lobby"))
        elif command == "配置":
            await self._start_setup(game, user_id)
        elif command == "角色":
            await self._setup_roles(game, user_id, args)
        elif command == "平票":
            await self._setup_tie(game, user_id, args)
        elif command == "女巫自救":
            await self._setup_witch_self(game, user_id, args)
        elif command == "女巫双药":
            await self._setup_witch_double(game, user_id, args)
        elif command == "胜利":
            await self._setup_victory(game, user_id, args)
        elif command == "开始":
            await self._start_game(game, user_id)
        elif command == "结束发言":
            await self._day_ready(game, user_id)
        elif command == "状态":
            await self._safe_send(chat_id, self._public_status(game))
        elif command == "推进":
            await self._force_advance(game, user_id)
        elif command == "重发":
            await self._resend(game, user_id, args)
        elif command == "取消":
            await self._cancel(game, user_id)
        elif command == "清理":
            await self._clear(game, user_id)
        else:
            await self._safe_send(chat_id, f"未知群聊命令。发送 {self.prefix} 帮助 查看列表。")

    async def _handle_private(self, message, command, args):
        user_id = str(message.get("sender_id") or message.get("user_id"))
        game = self._active_game_for_user(user_id)
        chat_id = str(message.get("chat_id") or f"private_{user_id}")
        if not game:
            await self._safe_send(chat_id, "你当前没有参加进行中的狼人杀游戏。")
            return
        player = self._player(game, user_id)
        if command == "状态":
            delivered = await self._safe_send(self._temp_id(game, user_id), self._private_status(game, player))
            if delivered and game["phase"] == "dealing" and player.get("role"):
                player["identity_delivered"] = True
                self._save()
                await self._deliver_start(game)
        elif command == "狼聊":
            await self._wolf_relay(game, player, " ".join(args))
        elif command in ("连结", "守护", "空守", "刀", "空刀", "查验"):
            await self._night_action(game, player, command, args)
        elif command in ("救", "毒", "救毒", "过"):
            await self._witch_action(game, player, command, args)
        elif command in ("开枪", "不开枪"):
            await self._shot_action(game, player, command, args)
        elif command in ("投票", "弃票"):
            await self._vote_action(game, player, command, args)
        else:
            await self._safe_send(chat_id, f"当前私聊命令无效。发送 {self.prefix} 状态 查看身份和可用操作。")

    async def _create_game(self, chat_id, host_id, host_name):
        existing = self.state["games"].get(chat_id)
        if existing and existing.get("phase") != "ended":
            await self._safe_send(chat_id, "当前群已经有一局游戏。")
            return
        other = self._active_game_for_user(host_id)
        if other and other.get("chat_id") != chat_id:
            await self._safe_send(chat_id, "你已经参加了其他群的游戏。")
            return
        game = {
            "chat_id": chat_id,
            "group_id": int(chat_id.split("_", 1)[1]),
            "host_id": host_id,
            "phase": "lobby",
            "players": [self._new_player(host_id, host_name, 1)],
            "settings": {"day_ready_threshold": self.day_ready_threshold},
            "setup_step": None,
            "night": 0,
            "day": 0,
            "intro_index": 0,
            "night_actions": {},
            "votes": {},
            "ready": [],
            "pending_shots": [],
            "lovers": [],
            "lovers_cross": False,
            "witch_antidote": True,
            "witch_poison": True,
            "last_guard_target": None,
        }
        self.state["games"][chat_id] = game
        self._save()
        await self._safe_send(chat_id, f"狼人杀房间已创建，{host_name} 自动成为 1 号玩家兼房主。\n其他玩家发送 {self.prefix} 加入。")

    @staticmethod
    def _new_player(user_id, name, seat):
        return {
            "user_id": str(user_id),
            "name": str(name or user_id),
            "seat": int(seat),
            "alive": True,
            "role": None,
            "identity_delivered": False,
            "idiot_revealed": False,
            "no_vote": False,
            "death_causes": [],
        }

    async def _join_game(self, game, user_id, user_name):
        if game["phase"] != "lobby":
            await self._safe_send(game["chat_id"], "游戏已经锁定，无法加入。")
            return
        if self._player(game, user_id):
            await self._safe_send(game["chat_id"], "你已经在玩家名单中。")
            return
        if self._active_game_for_user(user_id):
            await self._safe_send(game["chat_id"], "你已经参加了其他群的游戏。")
            return
        if len(game["players"]) >= self.max_players:
            await self._safe_send(game["chat_id"], f"本局最多 {self.max_players} 人。")
            return
        game["players"].append(self._new_player(user_id, user_name, len(game["players"]) + 1))
        self._save()
        await self._safe_send(game["chat_id"], f"{user_name} 加入成功，当前 {len(game['players'])} 人。")

    async def _leave_game(self, game, user_id):
        if game["phase"] != "lobby":
            await self._safe_send(game["chat_id"], "游戏已经锁定，无法退出。")
            return
        player = self._player(game, user_id)
        if not player:
            await self._safe_send(game["chat_id"], "你不在玩家名单中。")
            return
        if user_id == game["host_id"]:
            await self._safe_send(game["chat_id"], f"房主不能单独退出，请使用 {self.prefix} 取消。")
            return
        game["players"].remove(player)
        for seat, item in enumerate(game["players"], 1):
            item["seat"] = seat
        self._save()
        await self._safe_send(game["chat_id"], f"{player['name']} 已退出。")

    async def _start_setup(self, game, user_id):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以配置游戏。")
            return
        if game["phase"] not in ("lobby", "setup"):
            await self._safe_send(game["chat_id"], "当前阶段不能重新配置。")
            return
        count = len(game["players"])
        if not self.min_players <= count <= self.max_players:
            await self._safe_send(game["chat_id"], f"需要 {self.min_players}–{self.max_players} 名玩家，当前 {count} 人。")
            return
        game["phase"] = "setup"
        game["setup_step"] = "roles"
        game["settings"] = {"day_ready_threshold": self.day_ready_threshold}
        self._save()
        await self._safe_send(game["chat_id"], self._roles_prompt(game))

    def _roles_prompt(self, game):
        return (
            f"配置 1/5：当前 {len(game['players'])} 人，请设置角色数量。\n"
            f"格式：{self.prefix} 角色 狼人=2 村民=2 预言家=1 女巫=1\n"
            "可用角色：村民、狼人、预言家、女巫、猎人、守卫、白痴、狼王、丘比特；未填写按 0 计算。"
        )

    async def _setup_roles(self, game, user_id, args):
        if not await self._setup_allowed(game, user_id, "roles"):
            return
        counts = {key: 0 for key in ROLE_NAMES}
        try:
            for token in args:
                name, raw_count = token.split("=", 1)
                key = ROLE_KEYS[name]
                value = int(raw_count)
                if value < 0:
                    raise ValueError
                counts[key] = value
        except (ValueError, KeyError):
            await self._safe_send(game["chat_id"], "角色格式无效，请使用“角色名=数量”，数量必须为非负整数。")
            return
        error = self._validate_role_counts(counts, len(game["players"]))
        if error:
            await self._safe_send(game["chat_id"], error)
            return
        game["settings"]["roles"] = counts
        game["setup_step"] = "tie"
        self._save()
        await self._safe_send(
            game["chat_id"],
            f"配置 2/5：平票规则？\n{self.prefix} 平票 1：再次投票后无人出局\n"
            f"{self.prefix} 平票 2：立即无人出局\n{self.prefix} 平票 3：随机一人出局",
        )

    def _validate_role_counts(self, counts, player_count):
        if sum(counts.values()) != player_count:
            return f"角色总数必须等于玩家数 {player_count}。"
        if any(counts[key] > 1 for key in SPECIAL_ROLES):
            return "预言家、女巫、猎人、守卫、白痴、狼王和丘比特均最多一名。"
        wolf_count = counts["wolf"] + counts["wolf_king"]
        divine_count = sum(counts[key] for key in DIVINE_ROLES)
        if counts["villager"] < 1 or divine_count < 1 or wolf_count < 1:
            return "至少需要一名村民、一名神职和一名狼人阵营玩家。"
        if wolf_count >= player_count - wolf_count:
            return "狼人阵营初始人数必须少于其他玩家。"
        return ""

    async def _setup_tie(self, game, user_id, args):
        if not await self._setup_allowed(game, user_id, "tie"):
            return
        choice = args[0] if args else ""
        if choice not in TIE_POLICIES:
            await self._safe_send(game["chat_id"], f"请选择 {self.prefix} 平票 1、2 或 3。")
            return
        game["settings"]["tie_policy"] = TIE_POLICIES[choice]
        game["setup_step"] = "witch_self"
        self._save()
        await self._safe_send(
            game["chat_id"],
            f"配置 3/5：女巫何时可以自救？\n{self.prefix} 女巫自救 1：仅首夜\n"
            f"{self.prefix} 女巫自救 2：不能自救\n{self.prefix} 女巫自救 3：任意夜晚",
        )

    async def _setup_witch_self(self, game, user_id, args):
        if not await self._setup_allowed(game, user_id, "witch_self"):
            return
        choice = args[0] if args else ""
        if choice not in WITCH_SELF_POLICIES:
            await self._safe_send(game["chat_id"], f"请选择 {self.prefix} 女巫自救 1、2 或 3。")
            return
        game["settings"]["witch_self"] = WITCH_SELF_POLICIES[choice]
        game["setup_step"] = "witch_double"
        self._save()
        await self._safe_send(
            game["chat_id"],
            f"配置 4/5：女巫一晚能否同时使用两瓶药？\n{self.prefix} 女巫双药 是\n{self.prefix} 女巫双药 否",
        )

    async def _setup_witch_double(self, game, user_id, args):
        if not await self._setup_allowed(game, user_id, "witch_double"):
            return
        choice = args[0] if args else ""
        if choice not in ("是", "否"):
            await self._safe_send(game["chat_id"], f"请选择 {self.prefix} 女巫双药 是 或 {self.prefix} 女巫双药 否。")
            return
        game["settings"]["witch_double"] = choice == "是"
        game["setup_step"] = "victory"
        self._save()
        await self._safe_send(
            game["chat_id"],
            f"配置 5/5：狼人胜利条件？\n{self.prefix} 胜利 屠边\n{self.prefix} 胜利 屠城",
        )

    async def _setup_victory(self, game, user_id, args):
        if not await self._setup_allowed(game, user_id, "victory"):
            return
        choice = args[0] if args else ""
        if choice not in ("屠边", "屠城"):
            await self._safe_send(game["chat_id"], f"请选择 {self.prefix} 胜利 屠边 或 {self.prefix} 胜利 屠城。")
            return
        game["settings"]["victory"] = "slaughter_side" if choice == "屠边" else "slaughter_city"
        game["setup_step"] = None
        game["phase"] = "ready"
        self._save()
        await self._safe_send(game["chat_id"], f"配置完成。房主确认后发送 {self.prefix} 开始。")

    async def _setup_allowed(self, game, user_id, step):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以回答配置问题。")
            return False
        if game.get("phase") != "setup" or game.get("setup_step") != step:
            await self._safe_send(game["chat_id"], "当前不是这项配置的回答阶段，请按群内提示操作。")
            return False
        return True

    async def _start_game(self, game, user_id):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以开始游戏。")
            return
        if game["phase"] == "dealing":
            await self._deliver_start(game)
            return
        if game["phase"] != "ready":
            await self._safe_send(game["chat_id"], "请先完成报名和配置。")
            return

        roles = []
        for role, count in game["settings"]["roles"].items():
            roles.extend([role] * int(count))
        self.rng.shuffle(roles)
        for player, role in zip(game["players"], roles):
            player.update({
                "role": role,
                "alive": True,
                "identity_delivered": False,
                "idiot_revealed": False,
                "no_vote": False,
                "death_causes": [],
            })
        game["phase"] = "dealing"
        game["intro_index"] = 0
        self._save()
        await self._deliver_start(game)

    async def _deliver_start(self, game, only_seat=None):
        introductions = [self._rules_text(), self._settings_text(game), self._command_text()]
        while game.get("intro_index", 0) < len(introductions):
            index = game["intro_index"]
            if not await self._safe_send(game["chat_id"], introductions[index]):
                break
            game["intro_index"] = index + 1
            self._save()

        failed = []
        if game.get("intro_index") == len(introductions):
            for player in game["players"]:
                if only_seat is not None and player["seat"] != only_seat:
                    continue
                if only_seat is None and player.get("identity_delivered"):
                    continue
                if await self._safe_send(self._temp_id(game, player["user_id"]), self._identity_text(game, player)):
                    player["identity_delivered"] = True
                    self._save()
                else:
                    failed.append(player["seat"])

        all_delivered = all(player.get("identity_delivered") for player in game["players"])
        if game.get("intro_index") == len(introductions) and all_delivered:
            await self._safe_send(game["chat_id"], "规则、配置、命令和身份均已送达，游戏正式开始。")
            await self._begin_night(game)
        elif failed or game.get("intro_index") != len(introductions):
            details = "、".join(f"{seat}号" for seat in failed) or "开场介绍"
            await self._safe_send(game["chat_id"], f"送达未完成：{details}。房主可发送 {self.prefix} 重发。")

    async def _resend(self, game, user_id, args):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以重发。")
            return
        if game["phase"] == "ended" and not game.get("result_announced"):
            await self._announce_result(game)
            return
        if game["phase"] != "dealing":
            await self._safe_send(game["chat_id"], "当前没有待重发的身份。")
            return
        seat = None
        if args:
            try:
                seat = int(args[0])
            except ValueError:
                await self._safe_send(game["chat_id"], "座位号无效。")
                return
            if not self._by_seat(game, seat):
                await self._safe_send(game["chat_id"], "座位号不存在。")
                return
        await self._deliver_start(game, only_seat=seat)

    def _rules_text(self):
        return (
            "【狼人杀规则】\n"
            "夜间角色通过临时会话行动；全部必要行动完成后自动结算。白天自由发言，达到结束发言阈值后进入私密投票。\n"
            "支持村民、狼人、预言家、女巫、猎人、守卫、白痴、狼王和丘比特。守卫不能连续守同一人，同守同救仍死亡；毒杀不能开枪。\n"
            "跨阵营情侣成为第三方，必须成为最终两名存活者才能获胜。"
        )

    def _settings_text(self, game):
        counts = game["settings"]["roles"]
        roles = "、".join(f"{ROLE_NAMES[key]}×{value}" for key, value in counts.items() if value)
        victory = "屠边" if game["settings"]["victory"] == "slaughter_side" else "屠城"
        double = "允许" if game["settings"]["witch_double"] else "不允许"
        needed = math.ceil(float(game["settings"]["day_ready_threshold"]) * len(game["players"]))
        return (
            "【本局设置】\n"
            f"角色：{roles}\n"
            f"平票：{TIE_NAMES[game['settings']['tie_policy']]}\n"
            f"女巫：{WITCH_SELF_NAMES[game['settings']['witch_self']]}，{double}同夜双药\n"
            f"胜利条件：{victory}\n"
            f"首日结束发言阈值：{game['settings']['day_ready_threshold']:.0%}（当前需 {needed} 人）"
        )

    def _command_text(self):
        return (
            "【命令列表】\n"
            f"群聊：{self.prefix} 创建、加入、退出、名单、配置、开始、结束发言、状态、推进、重发 [座位]、取消、清理、帮助\n"
            f"临时会话：{self.prefix} 状态、连结 <两座位>、守护 <座位>、空守、刀 <座位>、空刀、查验 <座位>、救、毒 <座位>、救毒 <座位>、过、开枪 <座位>、不开枪、投票 <座位>、弃票、狼聊 <内容>\n"
            "所有目标均使用座位号。只有当前阶段和身份允许的命令会生效。"
        )

    def _identity_text(self, game, player):
        role = player["role"]
        lines = [f"你是 {player['seat']} 号 {player['name']}。", f"身份：{ROLE_NAMES[role]}", ROLE_HELP[role]]
        if role in WOLF_ROLES:
            wolves = [f"{p['seat']}号 {p['name']}（{ROLE_NAMES[p['role']]}）" for p in game["players"] if p["role"] in WOLF_ROLES]
            lines.append("狼队成员：" + "、".join(wolves))
        lines.append("身份信息仅供本人查看，请勿在群内转发机器人私聊。")
        return "\n".join(lines)

    async def _begin_night(self, game):
        game["night"] = int(game.get("night") or 0) + 1
        game["phase"] = "night_actions"
        game["night_actions"] = {"wolves": {}}
        game["ready"] = []
        game["votes"] = {}
        self._save()
        await self._safe_send(game["chat_id"], f"第 {game['night']} 夜开始。\n{self._seat_list(game, include_status=True)}")
        await self._send_night_prompts(game)
        await self._maybe_finish_initial_night(game)

    async def _send_night_prompts(self, game):
        for player in self._living(game):
            prompt = None
            role = player["role"]
            if role == "cupid" and game["night"] == 1 and not game.get("lovers"):
                prompt = f"请选择两名情侣：{self.prefix} 连结 <座位1> <座位2>"
            elif role == "guard":
                prompt = f"请选择守护目标：{self.prefix} 守护 <座位>，或 {self.prefix} 空守"
            elif role in WOLF_ROLES:
                prompt = f"请选择刀人目标：{self.prefix} 刀 <座位>，或 {self.prefix} 空刀。可使用 {self.prefix} 狼聊 <内容> 与狼队交流。"
            elif role == "seer":
                prompt = f"请选择查验目标：{self.prefix} 查验 <座位>"
            if prompt:
                await self._safe_send(self._temp_id(game, player["user_id"]), f"第 {game['night']} 夜。{prompt}")

    async def _night_action(self, game, player, command, args):
        if game["phase"] != "night_actions" or not player.get("alive"):
            await self._private_error(game, player, "当前不能执行该夜间操作。")
            return
        role = player["role"]
        actions = game["night_actions"]
        target = None

        if command == "连结":
            if role != "cupid" or game["night"] != 1 or game.get("lovers"):
                await self._private_error(game, player, "你当前不能连接情侣。")
                return
            if len(args) != 2:
                await self._private_error(game, player, f"格式：{self.prefix} 连结 <座位1> <座位2>")
                return
            targets = self._parse_seats(game, args, living=True)
            if not targets or targets[0]["user_id"] == targets[1]["user_id"]:
                await self._private_error(game, player, "请选择两名不同的存活玩家。")
                return
            actions["cupid"] = [targets[0]["user_id"], targets[1]["user_id"]]
            await self._private_ack(game, player, "情侣选择已记录，在本阶段结束前可修改。")
        elif command in ("守护", "空守"):
            if role != "guard":
                await self._private_error(game, player, "只有守卫可以守护。")
                return
            if command == "守护":
                target = self._single_target(game, args)
                if not target:
                    await self._private_error(game, player, "请选择有效的存活座位。")
                    return
                if target["user_id"] == game.get("last_guard_target"):
                    await self._private_error(game, player, "不能连续两晚守护同一名玩家。")
                    return
                target = target["user_id"]
            actions["guard"] = target
            await self._private_ack(game, player, "守护选择已记录。")
        elif command in ("刀", "空刀"):
            if role not in WOLF_ROLES:
                await self._private_error(game, player, "只有狼人阵营可以提交刀人选择。")
                return
            if command == "刀":
                target_player = self._single_target(game, args)
                if not target_player or target_player["role"] in WOLF_ROLES:
                    await self._private_error(game, player, "请选择一名存活的非狼队玩家。")
                    return
                target = target_player["user_id"]
            actions.setdefault("wolves", {})[player["user_id"]] = target
            await self._private_ack(game, player, "刀人选择已记录。")
        elif command == "查验":
            if role != "seer" or "seer" in actions:
                await self._private_error(game, player, "你当前不能再次查验。")
                return
            target_player = self._single_target(game, args)
            if not target_player or target_player["user_id"] == player["user_id"]:
                await self._private_error(game, player, "请选择另一名存活玩家。")
                return
            actions["seer"] = target_player["user_id"]
            result = "狼人阵营" if target_player["role"] in WOLF_ROLES else "非狼人阵营"
            player["last_seer_result"] = {
                "night": game["night"],
                "seat": target_player["seat"],
                "name": target_player["name"],
                "result": result,
            }
            await self._private_ack(game, player, f"查验结果：{target_player['seat']}号 {target_player['name']} 属于{result}。")
        else:
            return
        self._save()
        await self._maybe_finish_initial_night(game)

    async def _maybe_finish_initial_night(self, game):
        if game["phase"] != "night_actions":
            return
        actions = game["night_actions"]
        required = []
        cupid = self._living_role(game, "cupid")
        if game["night"] == 1 and cupid and not game.get("lovers"):
            required.append("cupid")
        if self._living_role(game, "guard"):
            required.append("guard")
        if self._living_role(game, "seer"):
            required.append("seer")
        wolf_ids = [p["user_id"] for p in self._living(game) if p["role"] in WOLF_ROLES]
        complete = all(key in actions for key in required) and all(uid in actions.get("wolves", {}) for uid in wolf_ids)
        if not complete:
            return

        if "cupid" in actions and actions["cupid"]:
            game["lovers"] = list(actions["cupid"])
            first, second = [self._player(game, uid) for uid in game["lovers"]]
            game["lovers_cross"] = self._camp(first["role"]) != self._camp(second["role"])
            await self._safe_send(self._temp_id(game, first["user_id"]), f"你的情侣是 {second['seat']}号 {second['name']}。")
            await self._safe_send(self._temp_id(game, second["user_id"]), f"你的情侣是 {first['seat']}号 {first['name']}。")

        wolf_choices = [uid for uid in actions.get("wolves", {}).values() if uid]
        actions["wolf_target"] = self._plurality(wolf_choices)
        game["phase"] = "witch"
        self._save()
        witch = self._living_role(game, "witch")
        can_act = witch and (game.get("witch_poison") or (game.get("witch_antidote") and actions["wolf_target"]))
        if not can_act:
            actions["witch"] = {"heal": False, "poison": None}
            await self._resolve_night(game)
            return
        await self._safe_send(self._temp_id(game, witch["user_id"]), self._witch_prompt(game, witch))

    async def _witch_action(self, game, player, command, args):
        if game["phase"] != "witch" or not player.get("alive") or player["role"] != "witch":
            await self._private_error(game, player, "当前不能使用女巫技能。")
            return
        wolf_target = game["night_actions"].get("wolf_target")
        heal = command in ("救", "救毒")
        poison = None
        if heal:
            if not wolf_target or not game.get("witch_antidote"):
                await self._private_error(game, player, "当前没有可以使用解药的目标。")
                return
            if not self._witch_can_heal(game, player, wolf_target):
                await self._private_error(game, player, "本局规则不允许此时自救。")
                return
        if command in ("毒", "救毒"):
            if not game.get("witch_poison"):
                await self._private_error(game, player, "毒药已经用完。")
                return
            target_player = self._single_target(game, args)
            if not target_player or target_player["user_id"] == player["user_id"]:
                await self._private_error(game, player, "请选择另一名存活玩家作为毒药目标。")
                return
            poison = target_player["user_id"]
        if command == "救毒" and not game["settings"].get("witch_double"):
            await self._private_error(game, player, "本局不允许同夜使用两瓶药。")
            return
        if command == "过":
            heal = False
            poison = None
        game["night_actions"]["witch"] = {"heal": heal, "poison": poison}
        if heal:
            game["witch_antidote"] = False
        if poison:
            game["witch_poison"] = False
        self._save()
        await self._private_ack(game, player, "女巫操作已确认。")
        await self._resolve_night(game)

    async def _resolve_night(self, game):
        actions = game["night_actions"]
        guard = actions.get("guard")
        game["last_guard_target"] = guard
        wolf_target = actions.get("wolf_target")
        witch = actions.get("witch") or {"heal": False, "poison": None}
        deaths = []
        if wolf_target:
            protected = guard == wolf_target
            healed = bool(witch.get("heal"))
            if (protected and healed) or (not protected and not healed):
                deaths.append((wolf_target, "wolf"))
        if witch.get("poison"):
            deaths.append((witch["poison"], "poison"))
        game["transition_after_shots"] = "day"
        newly_dead = self._apply_deaths(game, deaths)
        self._save()
        if newly_dead:
            names = [f"{player['seat']}号 {player['name']}" for player in newly_dead]
            await self._safe_send(game["chat_id"], "昨夜死亡：" + "、".join(dict.fromkeys(names)) + "。")
        else:
            await self._safe_send(game["chat_id"], "昨夜是平安夜。")
        await self._continue_death_resolution(game)

    def _apply_deaths(self, game, initial):
        queue = []
        causes = {}
        for uid, cause in initial:
            causes.setdefault(str(uid), set()).add(cause)
            queue.append(str(uid))
        handled = set()
        newly_dead = []
        while queue:
            uid = queue.pop(0)
            if uid in handled:
                continue
            handled.add(uid)
            player = self._player(game, uid)
            if not player or not player.get("alive"):
                continue
            player["alive"] = False
            player["death_causes"] = sorted(causes.get(uid) or {"unknown"})
            newly_dead.append(player)
            if uid in game.get("lovers", []):
                for lover_id in game["lovers"]:
                    if lover_id != uid:
                        lover = self._player(game, lover_id)
                        if lover and lover.get("alive"):
                            causes.setdefault(lover_id, set()).add("heartbreak")
                            queue.append(lover_id)
            if player["role"] in ("hunter", "wolf_king") and "poison" not in causes.get(uid, set()):
                if uid not in game.setdefault("pending_shots", []):
                    game["pending_shots"].append(uid)
        return newly_dead

    async def _continue_death_resolution(self, game):
        while game.get("pending_shots"):
            shooter = self._player(game, game["pending_shots"][0])
            if not shooter:
                game["pending_shots"].pop(0)
                continue
            game["phase"] = "death_shot"
            self._save()
            await self._safe_send(
                self._temp_id(game, shooter["user_id"]),
                f"你可以发动死亡技能：{self.prefix} 开枪 <座位>，或 {self.prefix} 不开枪",
            )
            return
        await self._after_deaths(game)

    async def _shot_action(self, game, player, command, args):
        pending = game.get("pending_shots") or []
        if game["phase"] != "death_shot" or not pending or pending[0] != player["user_id"]:
            await self._private_error(game, player, "当前不能开枪。")
            return
        target = None
        if command == "开枪":
            target = self._single_target(game, args)
            if not target:
                await self._private_error(game, player, "请选择有效的存活目标。")
                return
        game["pending_shots"].pop(0)
        if target:
            await self._safe_send(game["chat_id"], f"{player['seat']}号 {player['name']} 开枪带走了 {target['seat']}号 {target['name']}。")
            newly_dead = self._apply_deaths(game, [(target["user_id"], "shot")])
            chained = [item for item in newly_dead if item["user_id"] != target["user_id"]]
            if chained:
                labels = "、".join(f"{item['seat']}号 {item['name']}" for item in chained)
                await self._safe_send(game["chat_id"], f"情侣殉情：{labels}。")
        else:
            await self._safe_send(game["chat_id"], f"{player['seat']}号 {player['name']} 放弃开枪。")
        self._save()
        await self._continue_death_resolution(game)

    async def _after_deaths(self, game):
        winner = self._winner(game)
        if winner:
            await self._finish_game(game, winner)
            return
        transition = game.pop("transition_after_shots", "day")
        self._save()
        if transition == "night":
            await self._begin_night(game)
        else:
            await self._begin_day(game)

    async def _begin_day(self, game):
        game["day"] = int(game.get("day") or 0) + 1
        game["phase"] = "discussion"
        game["ready"] = []
        game["votes"] = {}
        self._save()
        needed = self._ready_needed(game)
        await self._safe_send(game["chat_id"], f"第 {game['day']} 天天亮，请开始讨论。存活玩家发送 {self.prefix} 结束发言；达到 {needed} 人后进入投票。")

    async def _day_ready(self, game, user_id):
        if game["phase"] != "discussion":
            await self._safe_send(game["chat_id"], "当前不在白天讨论阶段。")
            return
        player = self._player(game, user_id)
        if not player or not player.get("alive"):
            await self._safe_send(game["chat_id"], "只有存活玩家可以确认结束发言。")
            return
        if user_id not in game["ready"]:
            game["ready"].append(user_id)
            self._save()
        needed = self._ready_needed(game)
        await self._safe_send(game["chat_id"], f"结束发言确认：{len(game['ready'])}/{needed}。")
        if len(game["ready"]) >= needed:
            await self._begin_vote(game, round_number=1, candidates=None)

    def _ready_needed(self, game):
        return max(1, math.ceil(float(game["settings"]["day_ready_threshold"]) * len(self._living(game))))

    async def _begin_vote(self, game, round_number, candidates):
        game["phase"] = "vote"
        game["vote_round"] = round_number
        game["vote_candidates"] = list(candidates or [p["user_id"] for p in self._living(game)])
        game["votes"] = {}
        self._save()
        candidate_players = [self._player(game, uid) for uid in game["vote_candidates"]]
        labels = "、".join(f"{p['seat']}号 {p['name']}" for p in candidate_players)
        title = "平票加赛" if round_number == 2 else "投票开始"
        await self._safe_send(game["chat_id"], f"{title}，候选人：{labels}。请在临时会话提交，具体票型不会公开。")
        voters = self._eligible_voters(game)
        if not voters:
            await self._finish_vote_without_exile(game)
            return
        for voter in voters:
            await self._safe_send(
                self._temp_id(game, voter["user_id"]),
                f"候选人：{labels}\n投票：{self.prefix} 投票 <座位>，或 {self.prefix} 弃票",
            )

    def _eligible_voters(self, game):
        players = [p for p in self._living(game) if not p.get("no_vote")]
        if int(game.get("vote_round") or 1) == 2:
            candidates = set(game.get("vote_candidates") or [])
            players = [p for p in players if p["user_id"] not in candidates]
        return players

    async def _vote_action(self, game, player, command, args):
        if game["phase"] != "vote" or not player.get("alive") or player.get("no_vote"):
            await self._private_error(game, player, "当前不能投票。")
            return
        if player not in self._eligible_voters(game):
            await self._private_error(game, player, "你不是本轮投票人。")
            return
        target_id = None
        if command == "投票":
            target = self._single_target(game, args)
            if not target or target["user_id"] not in game.get("vote_candidates", []):
                await self._private_error(game, player, "请选择有效候选人。")
                return
            target_id = target["user_id"]
        game["votes"][player["user_id"]] = target_id
        self._save()
        await self._private_ack(game, player, "投票已记录，在本轮结束前可以修改。")
        if all(voter["user_id"] in game["votes"] for voter in self._eligible_voters(game)):
            await self._resolve_vote(game)

    async def _resolve_vote(self, game):
        choices = [uid for uid in game["votes"].values() if uid]
        if not choices:
            await self._finish_vote_without_exile(game)
            return
        counts = {uid: choices.count(uid) for uid in set(choices)}
        top_count = max(counts.values())
        top = [uid for uid, count in counts.items() if count == top_count]
        if len(top) > 1:
            policy = game["settings"]["tie_policy"]
            labels = "、".join(f"{self._player(game, uid)['seat']}号 {self._player(game, uid)['name']}" for uid in top)
            await self._safe_send(game["chat_id"], f"本轮最高票平票：{labels}。")
            if policy == "runoff" and int(game.get("vote_round") or 1) == 1:
                await self._begin_vote(game, round_number=2, candidates=top)
                return
            if policy == "random":
                exile_id = self.rng.choice(top)
            else:
                await self._finish_vote_without_exile(game)
                return
        else:
            exile_id = top[0]
        await self._exile(game, exile_id)

    async def _finish_vote_without_exile(self, game):
        await self._safe_send(game["chat_id"], "本轮无人出局。")
        game["transition_after_shots"] = "night"
        self._save()
        await self._after_deaths(game)

    async def _exile(self, game, user_id):
        player = self._player(game, user_id)
        if player["role"] == "idiot" and not player.get("idiot_revealed"):
            player["idiot_revealed"] = True
            player["no_vote"] = True
            self._save()
            await self._safe_send(game["chat_id"], f"{player['seat']}号 {player['name']} 被公投，但其身份是白痴，翻牌免死并永久失去投票权。")
            game["transition_after_shots"] = "night"
            await self._after_deaths(game)
            return
        await self._safe_send(game["chat_id"], f"{player['seat']}号 {player['name']} 被公投出局。")
        game["transition_after_shots"] = "night"
        newly_dead = self._apply_deaths(game, [(user_id, "exile")])
        chained = [item for item in newly_dead if item["user_id"] != user_id]
        if chained:
            labels = "、".join(f"{item['seat']}号 {item['name']}" for item in chained)
            await self._safe_send(game["chat_id"], f"情侣殉情：{labels}。")
        self._save()
        await self._continue_death_resolution(game)

    async def _wolf_relay(self, game, player, text):
        if game["phase"] not in ("night_actions", "witch") or not player.get("alive") or player["role"] not in WOLF_ROLES:
            await self._private_error(game, player, "当前不能使用狼聊。")
            return
        text = text.strip()
        if not text:
            await self._private_error(game, player, f"格式：{self.prefix} 狼聊 <内容>")
            return
        payload = f"【狼聊】{player['seat']}号 {player['name']}：{text}"
        for wolf in self._living(game):
            if wolf["role"] in WOLF_ROLES:
                await self._safe_send(self._temp_id(game, wolf["user_id"]), payload)

    async def _force_advance(self, game, user_id):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以推进游戏。")
            return
        phase = game["phase"]
        if phase == "dealing":
            await self._deliver_start(game)
        elif phase == "discussion":
            await self._begin_vote(game, 1, None)
        elif phase == "night_actions":
            actions = game["night_actions"]
            cupid = self._living_role(game, "cupid")
            if game["night"] == 1 and cupid and not game.get("lovers"):
                actions.setdefault("cupid", None)
            if self._living_role(game, "guard"):
                actions.setdefault("guard", None)
            if self._living_role(game, "seer"):
                actions.setdefault("seer", None)
            for wolf in self._living(game):
                if wolf["role"] in WOLF_ROLES:
                    actions.setdefault("wolves", {}).setdefault(wolf["user_id"], None)
            self._save()
            await self._maybe_finish_initial_night(game)
        elif phase == "witch":
            game["night_actions"]["witch"] = {"heal": False, "poison": None}
            self._save()
            await self._resolve_night(game)
        elif phase == "vote":
            for voter in self._eligible_voters(game):
                game["votes"].setdefault(voter["user_id"], None)
            self._save()
            await self._resolve_vote(game)
        elif phase == "death_shot":
            shooter = self._player(game, game["pending_shots"].pop(0))
            await self._safe_send(game["chat_id"], f"房主推进：{shooter['seat']}号 {shooter['name']} 视为放弃开枪。")
            self._save()
            await self._continue_death_resolution(game)
        else:
            await self._safe_send(game["chat_id"], "当前阶段不能推进。")

    async def _cancel(self, game, user_id):
        if not self._is_host(game, user_id):
            await self._safe_send(game["chat_id"], "只有房主可以取消游戏。")
            return
        del self.state["games"][game["chat_id"]]
        self._save()
        await self._safe_send(game["chat_id"], "本局游戏已取消，身份不会公开。")

    async def _clear(self, game, user_id):
        if not self._is_host(game, user_id) or game["phase"] != "ended":
            await self._safe_send(game["chat_id"], "只有房主可以清理已结束的游戏。")
            return
        del self.state["games"][game["chat_id"]]
        self._save()
        await self._safe_send(game["chat_id"], "已清理上一局记录，可以创建新房间。")

    def _winner(self, game):
        living = self._living(game)
        lovers = game.get("lovers") or []
        living_ids = {p["user_id"] for p in living}
        if game.get("lovers_cross") and len(lovers) == 2 and set(lovers) == living_ids:
            return "lovers"
        if game.get("lovers_cross") and all(uid in living_ids for uid in lovers):
            return None
        wolves = [p for p in living if p["role"] in WOLF_ROLES]
        if not wolves:
            return "good"
        if game["settings"]["victory"] == "slaughter_side":
            villagers = [p for p in living if p["role"] == "villager"]
            gods = [p for p in living if p["role"] in DIVINE_ROLES]
            if not villagers or not gods:
                return "wolves"
        else:
            if not [p for p in living if p["role"] not in WOLF_ROLES]:
                return "wolves"
        return None

    async def _finish_game(self, game, winner):
        game["phase"] = "ended"
        game["winner"] = winner
        self._save()
        await self._announce_result(game)

    async def _announce_result(self, game):
        winner = game["winner"]
        winner_name = {"good": "好人阵营", "wolves": "狼人阵营", "lovers": "跨阵营情侣"}[winner]
        roles = "\n".join(f"{p['seat']}号 {p['name']}：{ROLE_NAMES[p['role']]}" for p in game["players"])
        lover_text = "无"
        if game.get("lovers"):
            pair = [self._player(game, uid) for uid in game["lovers"]]
            lover_text = " 与 ".join(f"{p['seat']}号 {p['name']}" for p in pair)
        text = f"游戏结束，{winner_name}获胜。\n【身份公开】\n{roles}\n情侣：{lover_text}"
        if await self._safe_send(game["chat_id"], text):
            game["result_announced"] = True
            self._save()

    def _public_status(self, game):
        phase_names = {
            "lobby": "报名",
            "setup": "配置",
            "ready": "等待开始",
            "dealing": "身份送达",
            "night_actions": "夜间行动",
            "witch": "女巫行动",
            "death_shot": "死亡技能",
            "discussion": "白天讨论",
            "vote": "投票",
            "ended": "已结束",
        }
        lines = [f"当前阶段：{phase_names.get(game['phase'], game['phase'])}", self._seat_list(game, include_status=game["phase"] != "lobby")]
        if game["phase"] == "night_actions":
            done, required = self._night_progress(game)
            lines.append(f"夜间必要行动：{done}/{required}")
        elif game["phase"] == "vote":
            lines.append(f"投票完成：{len(game['votes'])}/{len(self._eligible_voters(game))}")
        elif game["phase"] == "discussion":
            lines.append(f"结束发言确认：{len(game['ready'])}/{self._ready_needed(game)}")
        return "\n".join(lines)

    def _night_progress(self, game):
        actions = game["night_actions"]
        required_keys = []
        if game["night"] == 1 and self._living_role(game, "cupid") and not game.get("lovers"):
            required_keys.append(("single", "cupid"))
        if self._living_role(game, "guard"):
            required_keys.append(("single", "guard"))
        if self._living_role(game, "seer"):
            required_keys.append(("single", "seer"))
        for wolf in self._living(game):
            if wolf["role"] in WOLF_ROLES:
                required_keys.append(("wolf", wolf["user_id"]))
        done = sum(1 for kind, key in required_keys if (key in actions if kind == "single" else key in actions.get("wolves", {})))
        return done, len(required_keys)

    def _private_status(self, game, player):
        if not player.get("role"):
            return f"你是 {player['seat']}号 {player['name']}。当前仍在报名或配置阶段，身份尚未分配。"
        lines = [self._identity_text(game, player)]
        if game.get("lovers") and player["user_id"] in game["lovers"]:
            other_id = next(uid for uid in game["lovers"] if uid != player["user_id"])
            other = self._player(game, other_id)
            lines.append(f"情侣：{other['seat']}号 {other['name']}")
        lines.append(f"当前阶段：{game['phase']}")
        result = player.get("last_seer_result")
        if result:
            lines.append(f"最近查验（第 {result['night']} 夜）：{result['seat']}号 {result['name']} 属于{result['result']}。")
        if not player.get("alive"):
            lines.append("你已死亡，不能再提交普通游戏操作。")
        else:
            prompt = self._current_private_prompt(game, player)
            if prompt:
                lines.append(prompt)
        return "\n".join(lines)

    def _current_private_prompt(self, game, player):
        phase = game["phase"]
        role = player["role"]
        if phase == "night_actions":
            if role == "cupid" and game["night"] == 1 and not game.get("lovers"):
                return f"当前操作：{self.prefix} 连结 <座位1> <座位2>"
            if role == "guard":
                return f"当前操作：{self.prefix} 守护 <座位>，或 {self.prefix} 空守"
            if role in WOLF_ROLES:
                return f"当前操作：{self.prefix} 刀 <座位>、{self.prefix} 空刀，或 {self.prefix} 狼聊 <内容>"
            if role == "seer":
                if "seer" in game["night_actions"]:
                    return "本夜查验已经提交。"
                return f"当前操作：{self.prefix} 查验 <座位>"
        if phase == "witch" and role == "witch":
            return self._witch_prompt(game, player)
        if phase == "death_shot" and game.get("pending_shots") and game["pending_shots"][0] == player["user_id"]:
            return f"当前操作：{self.prefix} 开枪 <座位>，或 {self.prefix} 不开枪"
        if phase == "vote" and player in self._eligible_voters(game):
            candidates = [self._player(game, uid) for uid in game.get("vote_candidates", [])]
            labels = "、".join(f"{item['seat']}号 {item['name']}" for item in candidates)
            return f"候选人：{labels}。当前操作：{self.prefix} 投票 <座位>，或 {self.prefix} 弃票"
        return ""

    def _witch_prompt(self, game, witch):
        target_id = game["night_actions"].get("wolf_target")
        target = self._player(game, target_id) if target_id else None
        victim = f"{target['seat']}号 {target['name']}" if target else "无人"
        options = [f"狼刀目标：{victim}。"]
        can_heal = bool(game.get("witch_antidote") and target and self._witch_can_heal(game, witch, target_id))
        if can_heal:
            options.append(f"使用解药：{self.prefix} 救")
        if game.get("witch_poison"):
            options.append(f"使用毒药：{self.prefix} 毒 <座位>")
        if can_heal and game.get("witch_poison") and game["settings"].get("witch_double"):
            options.append(f"同时使用：{self.prefix} 救毒 <座位>")
        options.append(f"不使用：{self.prefix} 过")
        return "\n".join(options)

    @staticmethod
    def _witch_can_heal(game, witch, target_id):
        if target_id != witch["user_id"]:
            return True
        policy = game["settings"]["witch_self"]
        return policy == "always" or (policy == "first_night" and game["night"] == 1)

    def _seat_list(self, game, include_status=True):
        lines = ["【座位名单】"]
        for player in sorted(game["players"], key=lambda item: item["seat"]):
            status = "（存活）" if player.get("alive") else "（已死亡）"
            lines.append(f"{player['seat']}号 {player['name']}{status if include_status else ''}")
        return "\n".join(lines)

    def _active_game_for_user(self, user_id):
        for game in self.state["games"].values():
            if game.get("phase") != "ended" and self._player(game, user_id):
                return game
        return None

    @staticmethod
    def _player(game, user_id):
        if not game:
            return None
        user_id = str(user_id)
        return next((player for player in game.get("players", []) if player["user_id"] == user_id), None)

    @staticmethod
    def _by_seat(game, seat):
        return next((player for player in game.get("players", []) if player["seat"] == int(seat)), None)

    def _parse_seats(self, game, args, living=False):
        try:
            players = [self._by_seat(game, int(value)) for value in args]
        except (TypeError, ValueError):
            return None
        if any(player is None for player in players):
            return None
        if living and any(not player.get("alive") for player in players):
            return None
        return players

    def _single_target(self, game, args):
        players = self._parse_seats(game, args[:1], living=True) if len(args) == 1 else None
        return players[0] if players else None

    @staticmethod
    def _living(game):
        return [player for player in game["players"] if player.get("alive")]

    def _living_role(self, game, role):
        return next((player for player in self._living(game) if player["role"] == role), None)

    @staticmethod
    def _camp(role):
        return "wolf" if role in WOLF_ROLES else "good"

    @staticmethod
    def _plurality(values):
        if not values:
            return None
        counts = {value: values.count(value) for value in set(values)}
        maximum = max(counts.values())
        top = [value for value, count in counts.items() if count == maximum]
        return top[0] if len(top) == 1 else None

    @staticmethod
    def _is_host(game, user_id):
        return str(game.get("host_id")) == str(user_id)

    @staticmethod
    def _temp_id(game, user_id):
        return f"temp_{game['group_id']}_{user_id}"

    async def _private_ack(self, game, player, text):
        await self._safe_send(self._temp_id(game, player["user_id"]), text)

    async def _private_error(self, game, player, text):
        await self._safe_send(self._temp_id(game, player["user_id"]), text)

    async def _safe_send(self, chat_id, text):
        try:
            await self.ctx.send_message(chat_id, text)
            return True
        except Exception as exc:
            self.ctx.log(f"send to {chat_id} failed: {exc}")
            return False
