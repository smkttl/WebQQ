import json
import asyncio
import random
import re
import time
from urllib.parse import urljoin

import aiohttp


MENTION_RE = re.compile(r"@\[[^\]]+\]")
REPLY_RE = re.compile(r"^\[reply:[^\]]+\]")
LEAKED_MESSAGE_ID_RE = re.compile(r"\s*[\(（]?\s*(?:message_id|消息\s*ID|消息id|消息编号)\s*[=:：]?\s*(\d+)\s*[\)）]?\s*", re.IGNORECASE)


def setup(ctx):
    return LlmPlugin(ctx)


class LlmPlugin:
    def __init__(self, ctx):
        self.ctx = ctx
        self._active_requests = 0
        self._active_oracles = 0
        self._active_lock = asyncio.Lock()
        self._oracle_lock = asyncio.Lock()

    async def handle_event(self, event, ctx):
        if event.get("type") != "message":
            return
        message = event.get("message") or {}
        if not self._is_candidate_trigger(message):
            return

        chat_id = str(message.get("chat_id") or "")
        chat_type = str(message.get("type") or "")
        mode = self._reply_mode(chat_id, chat_type)
        if mode == -1:
            return
        if not await self._try_acquire_request_slot():
            return

        explicit, prompt = self._explicit_prompt(message, event)
        try:
            if not self._should_reply(message, mode, explicit):
                return
            if not prompt.strip():
                prompt = str(message.get("content") or "").strip()
            if not prompt:
                return
            await self._reply(message, prompt)
        finally:
            await self._release_request_slot()

    async def handle_portal_message(self, message, ctx):
        text = str((message or {}).get("text") or "").strip()
        chat_id = str((message or {}).get("chat_id") or "").strip()
        if not text:
            return
        if text == "/clear-guidance":
            self.ctx.config["runtime_guidance"] = []
            self._save_config()
            if self.ctx.config.get("portal_confirm", True) and chat_id:
                await self._send(chat_id, "已清空实时指导。")
            return
        if text == "/guidance":
            guidance = self._runtime_guidance()
            if self.ctx.config.get("portal_confirm", True) and chat_id:
                if guidance:
                    lines = [f"{i + 1}. {item.get('text') or ''}" for i, item in enumerate(guidance)]
                    await self._send(chat_id, "当前实时指导：\n" + "\n".join(lines))
                else:
                    await self._send(chat_id, "当前没有实时指导。")
            return

        guidance = self._runtime_guidance()
        guidance.append({
            "time": int(time.time()),
            "chat_id": chat_id,
            "text": text,
        })
        limit = self._int_config("runtime_guidance_limit", 50, minimum=1)
        self.ctx.config["runtime_guidance"] = guidance[-limit:]
        self._save_config()
        if self.ctx.config.get("portal_confirm", True) and chat_id:
            task = asyncio.create_task(self._reply_to_portal_guidance(message, text))
            task.add_done_callback(self._log_background_error)

    async def _try_acquire_request_slot(self):
        max_parallels = self._int_config("max_parallels", 3, minimum=1)
        async with self._active_lock:
            if self._active_requests >= max_parallels:
                return False
            self._active_requests += 1
            return True

    async def _release_request_slot(self):
        async with self._active_lock:
            self._active_requests = max(0, self._active_requests - 1)

    async def _try_acquire_oracle_slot(self):
        max_parallels = self._int_config("max_oracle_parallels", 1, minimum=1)
        async with self._oracle_lock:
            if self._active_oracles >= max_parallels:
                return False
            self._active_oracles += 1
            return True

    async def _release_oracle_slot(self):
        async with self._oracle_lock:
            self._active_oracles = max(0, self._active_oracles - 1)

    def _is_candidate_trigger(self, message):
        if message.get("self"):
            return False
        if message.get("system") or message.get("recalled"):
            return False
        source = str(message.get("source") or "")
        if source.startswith("plugin:"):
            return False
        return bool(str(message.get("chat_id") or ""))

    def _reply_mode(self, chat_id, chat_type):
        raw_modes = self.ctx.config.get("reply_possibility") or {}
        if not isinstance(raw_modes, dict) or chat_id not in raw_modes:
            return -1
        raw = raw_modes.get(chat_id)
        try:
            mode = float(raw)
        except (TypeError, ValueError):
            self.ctx.log(f"invalid reply_possibility for {chat_id}: {raw!r}; treating as -1")
            return -1

        is_private = chat_type == "private" or chat_id.startswith("private_")
        if is_private:
            if mode in (-1, 1):
                return int(mode)
            self.ctx.log(f"private chat {chat_id} only accepts -1 or 1; treating as -1")
            return -1

        if mode == -1:
            return -1
        if 0 <= mode <= 1:
            return mode
        self.ctx.log(f"group/temp chat {chat_id} only accepts -1 or [0, 1]; treating as -1")
        return -1

    def _explicit_prompt(self, message, event):
        content = str(message.get("content") or "").strip()
        content = REPLY_RE.sub("", content).strip()

        for prefix in self._prefixes():
            if content.startswith(prefix):
                return True, content[len(prefix):].strip()

        if self._mentioned(message, event):
            return True, self._strip_mentions(content)

        return False, content

    def _prefixes(self):
        prefixes = self.ctx.config.get("prefixes")
        if not isinstance(prefixes, list):
            return []
        return [str(item) for item in prefixes if str(item)]

    def _mentioned(self, message, event):
        if not bool(self.ctx.config.get("reply_to_mentions", True)):
            return False
        mentions = message.get("mentions") or {}
        if not isinstance(mentions, dict) or not mentions:
            return False
        raw = event.get("raw") or {}
        self_id = raw.get("self_user_id") if isinstance(raw, dict) else None
        if self_id is None:
            return True
        return str(self_id) in {str(key) for key in mentions}

    def _strip_mentions(self, content):
        return MENTION_RE.sub("", content).strip()

    def _should_reply(self, message, mode, explicit):
        chat_type = str(message.get("type") or "")
        is_private = chat_type == "private" or str(message.get("chat_id") or "").startswith("private_")
        if is_private:
            return mode == 1
        if explicit:
            return mode >= 0
        if mode <= 0:
            return False
        if mode >= 1:
            return True
        return random.random() < mode

    async def _reply(self, message, prompt):
        api_key = str(self.ctx.config.get("api_key") or "").strip()
        if not api_key:
            self.ctx.log("api_key is missing")
            if self.ctx.config.get("send_errors_to_chat"):
                await self._send(message["chat_id"], "LLM is not configured: missing api_key.")
            return

        llm_messages = self._build_messages(message, prompt)
        oracle_rounds = 0
        max_oracle_rounds = self._int_config("max_oracle_rounds", 2, minimum=0)
        force_no_oracle = False
        while True:
            try:
                text = await self._call_llm(api_key, llm_messages)
            except Exception as e:
                self.ctx.log(f"llm request failed: {e}")
                if self.ctx.config.get("send_errors_to_chat"):
                    await self._send(message["chat_id"], f"LLM request failed: {e}")
                return
            actions = self._parse_actions(text)
            if not actions and self._looks_non_json(text):
                actions = await self._repair_actions(api_key, text)
            if not actions:
                self.ctx.log(f"ignored non-JSON LLM output: {text[:300]!r}")
                return

            oracle_actions = []
            for action in actions:
                if action.get("type") == "oracle":
                    oracle_actions.append(action)
                else:
                    await self._send_message_action(message["chat_id"], action)
                    llm_messages.append({
                        "role": "assistant",
                        "content": f"(sent_to_chat reply_to={action.get('reply_to') or 0}) {action.get('text') or ''}",
                    })

            if not oracle_actions:
                return
            if force_no_oracle:
                return
            if oracle_rounds >= max_oracle_rounds:
                llm_messages.append({
                    "role": "system",
                    "content": "Oracle round limit reached. Do not call oracle again; answer the user with current information or say what remains uncertain.",
                })
                force_no_oracle = True
                continue

            oracle_rounds += 1
            oracle_notes = []
            for action in oracle_actions:
                oracle_notes.append(await self._run_oracle_action(llm_messages, action))
            llm_messages.extend(oracle_notes)

    async def _reply_to_portal_guidance(self, portal_message, guidance_text):
        chat_id = str((portal_message or {}).get("chat_id") or "").strip()
        if not chat_id:
            return
        synthetic = {
            "chat_id": chat_id,
            "type": str((portal_message or {}).get("chat_type") or ""),
            "message_id": 0,
            "sender_name": "system",
            "sender_id": "system",
            "user_id": "system",
            "content": guidance_text,
            "self": False,
            "source": "ui_portal",
            "self_user": (portal_message or {}).get("self_user"),
        }
        await self._reply(synthetic, guidance_text)

    def _log_background_error(self, task):
        try:
            task.result()
        except Exception as e:
            self.ctx.log(f"background guidance reply failed: {e}")

    async def _send_message_action(self, chat_id, action):
        text = action.get("text")
        if not text:
            return
        reply_to = action.get("reply_to")
        kwargs = {}
        if reply_to:
            kwargs["reply_to"] = reply_to
        await self._send(chat_id, text, **kwargs)

    async def _send(self, chat_id, text, **kwargs):
        try:
            await self.ctx.send_message(chat_id, text, **kwargs)
            return True
        except Exception as e:
            self.ctx.log(f"send failed: {e}")
            return False

    async def _repair_actions(self, api_key, bad_output):
        repair_messages = [
            {
                "role": "system",
                "content": "Convert the assistant output into valid JSON only. Output a JSON array of actions. Use message actions only: {\"type\":\"message\",\"reply_to\":0,\"text\":\"...\"}. Preserve the intended user-visible text. Do not add prose.",
            },
            {"role": "user", "content": str(bad_output)},
        ]
        try:
            repaired = await self._call_llm(api_key, repair_messages)
        except Exception as e:
            self.ctx.log(f"repair request failed: {e}")
            return []
        actions = self._parse_actions(repaired, allow_plain_fallback=False)
        if not actions:
            self.ctx.log(f"repair produced invalid output: {repaired[:300]!r}")
        return actions

    def _build_messages(self, trigger_message, prompt):
        messages = []
        for prompt_key in ("persona_prompt", "reply_prompt", "system_prompt"):
            system_prompt = str(self.ctx.config.get(prompt_key) or "").strip()
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})

        chat_id = trigger_message["chat_id"]
        self_note = self._self_user_note(trigger_message)
        if self_note:
            messages.append({"role": "system", "content": self_note})
        context_note = self._context_note(chat_id)
        if context_note:
            messages.append({"role": "system", "content": context_note})
        messages.append({
            "role": "system",
            "content": "Prior assistant messages may be shown as canonical JSON action arrays. Treat them as examples of the correct output format.",
        })

        history_limit = self._int_config("history_limit", 30, minimum=1)
        history = self.ctx.get_messages(chat_id, limit=history_limit)
        trigger_id = str(trigger_message.get("message_id") or "")
        appended_trigger = False
        timeline = []

        for item in history:
            content = self._history_content(item)
            if not content:
                continue
            if trigger_id and str(item.get("message_id") or "") == trigger_id:
                content = prompt
                appended_trigger = True
            timeline.append({
                "kind": "message",
                "time": self._item_time(item),
                "order": len(timeline),
                "item": item,
                "content": content,
            })

        if not appended_trigger:
            timeline.append({
                "kind": "message",
                "time": self._item_time(trigger_message),
                "order": len(timeline),
                "item": trigger_message,
                "content": prompt,
            })

        for item in self._runtime_guidance(chat_id):
            content = str(item.get("text") or "").strip()
            if content:
                timeline.append({
                    "kind": "guidance",
                    "time": self._item_time(item),
                    "order": len(timeline),
                    "content": content,
                })

        for entry in sorted(timeline, key=lambda entry: (entry["time"], entry["order"])):
            if entry["kind"] == "guidance":
                messages.append({
                    "role": "user",
                    "content": f"(message_id=0) User(name=system): {entry['content']}",
                })
                continue
            item = entry["item"]
            content = entry["content"]
            role = "assistant" if item.get("self") or str(item.get("source") or "").startswith("plugin:") else "user"
            if role == "user":
                content = f"{self._sender_label(item)}: {content}"
                content = f"(message_id={item.get('message_id') or 0}) {content}"
            else:
                content = self._assistant_history_content(item, content)
            messages.append({"role": role, "content": content})

        return self._trim_messages(messages)

    @staticmethod
    def _item_time(item):
        try:
            return float((item or {}).get("time") or 0)
        except (TypeError, ValueError):
            return 0

    def _history_content(self, message):
        if message.get("system") or message.get("recalled"):
            return ""
        content = str(message.get("content") or "").strip()
        content = REPLY_RE.sub("", content).strip()
        return content

    @classmethod
    def _assistant_history_content(cls, message, content):
        actions = cls._actions_from_leaked_reply_text(content)
        if actions:
            fallback_reply_to = cls._reply_to_from_content(message.get("content")) or 0
            canonical = []
            for action in actions:
                reply_to = action.get("reply_to") or fallback_reply_to or 0
                canonical.append({
                    "type": "message",
                    "reply_to": int(reply_to) if str(reply_to).isdigit() else 0,
                    "text": action.get("text") or "",
                })
            return json.dumps(canonical, ensure_ascii=False)
        else:
            reply_to = cls._reply_to_from_content(message.get("content")) or 0
            text = cls._clean_reply_text(content)
            return json.dumps([{
                "type": "message",
                "reply_to": int(reply_to) if str(reply_to).isdigit() else 0,
                "text": text,
            }], ensure_ascii=False)

    @staticmethod
    def _reply_to_from_content(content):
        match = REPLY_RE.match(str(content or "").strip())
        return match.group(0)[len("[reply:"):-1] if match else 0

    def _trim_messages(self, messages):
        max_chars = self._int_config("max_prompt_chars", 12000, minimum=1000)
        while len(messages) > 2 and self._messages_chars(messages) > max_chars:
            remove_at = next((i for i, item in enumerate(messages) if item.get("role") != "system"), None)
            if remove_at is None:
                break
            messages.pop(remove_at)
        return messages

    @staticmethod
    def _messages_chars(messages):
        return sum(len(str(item.get("content") or "")) for item in messages)

    def _self_user_note(self, trigger_message=None):
        user = {}
        if isinstance(trigger_message, dict) and isinstance(trigger_message.get("self_user"), dict):
            user.update(trigger_message.get("self_user") or {})
        getter = getattr(self.ctx, "get_self_user", None)
        if callable(getter):
            try:
                fetched = getter()
                if isinstance(fetched, dict):
                    user.update({k: v for k, v in fetched.items() if v is not None and str(v).strip()})
            except Exception as e:
                self.ctx.log(f"get_self_user failed: {e}")
        uid = str(user.get("user_id") or user.get("uid") or user.get("uin") or "").strip()
        name = str(user.get("name") or user.get("nickname") or user.get("nick") or "").strip()
        if not uid and not name:
            return ""
        fields = []
        if uid:
            fields.append(f"user_id={uid}")
        if name:
            fields.append(f"name={name}")
        return "Current account identity. This is you, the QQ account the LLM controls: " + ", ".join(fields)

    def _context_note(self, chat_id):
        members = self._chat_members(chat_id)
        if not members:
            return ""
        limit = self._int_config("member_context_limit", 80, minimum=1)
        rows = []
        for member in members[:limit]:
            rows.append(self._member_label(member))
        return "Known chat users. Use qid/user_id for @[qq_number] mentions when needed:\n" + "\n".join(rows)

    def _chat_members(self, chat_id):
        store = getattr(getattr(self.ctx, "manager", None), "store", None)
        details = getattr(store, "_group_member_details", {}) if store else {}
        members = details.get(chat_id) if isinstance(details, dict) else None
        if isinstance(members, list) and members:
            return [item for item in members if isinstance(item, dict)]
        simple = getattr(store, "_group_members", {}) if store else {}
        mapping = simple.get(chat_id) if isinstance(simple, dict) else None
        if isinstance(mapping, dict):
            return [{"user_id": uid, "display_name": name} for uid, name in mapping.items()]
        return []

    @staticmethod
    def _member_label(member):
        keys = ("display_name", "user_id", "qid", "card", "remark", "name", "nick", "nickname", "tag", "title")
        fields = []
        for key in keys:
            value = member.get(key)
            if value is not None and str(value).strip():
                fields.append(f"{key}={value}")
        return "- " + ", ".join(fields)

    @staticmethod
    def _sender_label(message):
        fields = []
        sender_name = str(message.get("sender_name") or "").strip()
        sender_id = str(message.get("sender_id") or "").strip()
        user_id = str(message.get("user_id") or "").strip()
        if sender_name:
            fields.append(f"name={sender_name}")
        if sender_id:
            fields.append(f"user_id={sender_id}")
        elif user_id:
            fields.append(f"user_id={user_id}")
        return "User(" + ", ".join(fields) + ")" if fields else "User"

    def _runtime_guidance(self, chat_id=None):
        raw = self.ctx.config.get("runtime_guidance")
        if not isinstance(raw, list):
            return []
        guidance = []
        chat_id = str(chat_id or "")
        for item in raw:
            if isinstance(item, str):
                text = item.strip()
                if text and not chat_id:
                    guidance.append({"time": 0, "chat_id": "", "text": text})
                continue
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            item_chat_id = str(item.get("chat_id") or "")
            if chat_id and item_chat_id != chat_id:
                continue
            guidance.append({
                "time": item.get("time") or 0,
                "chat_id": item_chat_id,
                "text": text,
            })
        return guidance

    def _save_config(self):
        manager = getattr(self.ctx, "manager", None)
        state = (getattr(manager, "_plugins", {}) or {}).get(self.ctx.plugin_id) if manager else None
        plugin_path = state.get("path") if isinstance(state, dict) else None
        if not plugin_path:
            self.ctx.log("cannot save runtime guidance: plugin path unavailable")
            return False
        config_path = plugin_path / "config.json"
        try:
            with open(config_path, "w", encoding="utf-8") as f:
                json.dump(self.ctx.config, f, ensure_ascii=False, indent=2)
                f.write("\n")
            return True
        except Exception as e:
            self.ctx.log(f"cannot save runtime guidance: {e}")
            return False

    async def _call_llm(self, api_key, messages):
        base_url = str(self.ctx.config.get("base_url") or "https://api.openai.com/v1").rstrip("/") + "/"
        url = urljoin(base_url, "chat/completions")
        payload = {
            "model": str(self.ctx.config.get("model") or "").strip(),
            "messages": messages,
            "temperature": self._float_config("temperature", 0.7),
            "max_tokens": self._int_config("max_tokens", 800, minimum=1),
        }
        if not payload["model"]:
            raise ValueError("model is missing")

        timeout = aiohttp.ClientTimeout(total=self._float_config("timeout_seconds", 60, minimum=1))
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    raise RuntimeError(self._error_text(data, resp.status))
        return self._extract_response_text(data)

    async def _run_oracle_action(self, llm_messages, action):
        question = self._clean_reply_text(action.get("question") or "")
        if not question:
            return {"role": "system", "content": "Oracle request ignored: empty question."}
        if not self.ctx.config.get("oracle_enabled"):
            return {"role": "system", "content": "Oracle is disabled. Answer without oracle help."}
        if not await self._try_acquire_oracle_slot():
            return {"role": "system", "content": "Oracle is busy. Answer without oracle help for now."}
        try:
            answer = await self._call_oracle(question, llm_messages)
            return {
                "role": "system",
                "content": f"Oracle answer for internal question:\n{question}\n\nOracle answer:\n{answer}",
            }
        except Exception as e:
            self.ctx.log(f"oracle request failed: {e}")
            return {
                "role": "system",
                "content": f"Oracle request failed for internal question:\n{question}\n\nError: {e}",
            }
        finally:
            await self._release_oracle_slot()

    async def _call_oracle(self, question, llm_messages):
        api_key = str(self.ctx.config.get("oracle_api_key") or "").strip()
        base_url = str(self.ctx.config.get("oracle_base_url") or self.ctx.config.get("base_url") or "").strip()
        model = str(self.ctx.config.get("oracle_model") or "").strip()
        if not api_key:
            raise ValueError("oracle_api_key is missing")
        if not base_url:
            raise ValueError("oracle_base_url is missing")
        if not model:
            raise ValueError("oracle_model is missing")

        messages = self._build_oracle_messages(question, llm_messages)
        payload = {
            "model": model,
            "messages": messages,
            "temperature": self._float_config("oracle_temperature", 0.2),
            "max_tokens": self._int_config("oracle_max_tokens", 2000, minimum=1),
        }
        timeout = aiohttp.ClientTimeout(total=self._float_config("oracle_timeout_seconds", 120, minimum=1))
        url = urljoin(base_url.rstrip("/") + "/", "chat/completions")
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(url, json=payload, headers=headers) as resp:
                data = await resp.json(content_type=None)
                if resp.status >= 400:
                    raise RuntimeError(self._error_text(data, resp.status))
        answer = self._extract_response_text(data)
        if not answer:
            raise RuntimeError("empty oracle response")
        return answer

    def _build_oracle_messages(self, question, llm_messages):
        messages = []
        oracle_prompt = str(self.ctx.config.get("oracle_prompt") or "").strip()
        if oracle_prompt:
            messages.append({"role": "system", "content": oracle_prompt})
        oracle_persona = str(self.ctx.config.get("oracle_persona_prompt") or "").strip()
        if oracle_persona:
            messages.append({"role": "system", "content": oracle_persona})

        context = "\n".join(
            str(item.get("content") or "")
            for item in llm_messages
            if item.get("role") != "system" and item.get("content")
        )
        if context:
            messages.append({"role": "user", "content": f"Recent chat context:\n{context}"})
        messages.append({"role": "user", "content": f"Internal oracle question:\n{question}"})
        return messages

    @staticmethod
    def _extract_response_text(data):
        choices = data.get("choices") if isinstance(data, dict) else None
        if not choices:
            return ""
        first = choices[0] if isinstance(choices[0], dict) else {}
        message = first.get("message") if isinstance(first.get("message"), dict) else {}
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    text = item.get("text") or item.get("content")
                    if isinstance(text, str):
                        parts.append(text)
            return "".join(parts).strip()
        return ""

    def _parse_actions(self, text, allow_plain_fallback=False):
        parsed = None
        try:
            parsed = json.loads(text)
        except Exception:
            pass
        if isinstance(parsed, list):
            actions = []
            for item in parsed:
                action = self._normalize_action(item)
                if action:
                    actions.append(action)
        elif parsed is None:
            actions = self._parse_concatenated_json_arrays(text)
            if actions:
                return actions[:5]
            if not allow_plain_fallback:
                return []
            actions = []
            for line in str(text).splitlines():
                actions.extend(self._actions_from_leaked_reply_text(line))
        else:
            if not allow_plain_fallback:
                return []
            actions = []
            for line in str(text).splitlines():
                actions.extend(self._actions_from_leaked_reply_text(line))
        return actions[:5]

    @classmethod
    def _parse_concatenated_json_arrays(cls, text):
        decoder = json.JSONDecoder()
        pos = 0
        actions = []
        text = str(text or "")
        while pos < len(text):
            while pos < len(text) and text[pos].isspace():
                pos += 1
            if pos >= len(text):
                break
            if text[pos] != "[":
                return []
            try:
                parsed, pos = decoder.raw_decode(text, pos)
            except json.JSONDecodeError:
                return []
            if not isinstance(parsed, list):
                return []
            for item in parsed:
                action = cls._normalize_action(item)
                if action:
                    actions.append(action)
        return actions

    @staticmethod
    def _looks_non_json(text):
        text = str(text or "").lstrip()
        return bool(text) and not text.startswith("[")

    @classmethod
    def _normalize_action(cls, item):
        if isinstance(item, str):
            text = cls._clean_reply_text(item)
            return {"type": "message", "reply_to": 0, "text": text} if text else None
        if not isinstance(item, dict):
            text = cls._clean_reply_text(item)
            return {"type": "message", "reply_to": 0, "text": text} if text else None
        action_type = str(item.get("type") or ("message" if item.get("text") is not None else "")).strip().lower()
        if action_type == "oracle":
            question = cls._clean_reply_text(item.get("question") or item.get("text") or item.get("content") or "")
            if not question:
                return None
            return {"type": "oracle", "question": question}
        text = cls._clean_reply_text(item.get("text") or item.get("content") or item.get("message") or "")
        if not text:
            return None
        reply_to = item.get("reply_to", 0)
        try:
            reply_to = int(reply_to)
        except (TypeError, ValueError):
            reply_to = 0
        if reply_to <= 0:
            leaked = LEAKED_MESSAGE_ID_RE.search(str(item.get("text") or item.get("content") or item.get("message") or ""))
            if leaked:
                reply_to = int(leaked.group(1))
        return {"type": "message", "reply_to": str(reply_to) if reply_to > 0 else 0, "text": text}

    @staticmethod
    def _clean_reply_text(text):
        return LEAKED_MESSAGE_ID_RE.sub("", str(text).strip()).strip()

    @classmethod
    def _actions_from_leaked_reply_text(cls, text):
        text = str(text or "").strip()
        if not text:
            return []
        matches = list(LEAKED_MESSAGE_ID_RE.finditer(text))
        if not matches:
            cleaned = cls._clean_reply_text(text)
            return [{"type": "message", "reply_to": 0, "text": cleaned}] if cleaned else []

        actions = []
        prefix = text[:matches[0].start()].strip()
        if prefix:
            actions.append({"type": "message", "reply_to": 0, "text": cls._clean_reply_text(prefix)})

        for index, match in enumerate(matches):
            start = match.end()
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            content = cls._clean_reply_text(text[start:end])
            if content:
                actions.append({"type": "message", "reply_to": match.group(1), "text": content})
        return actions

    @staticmethod
    def _error_text(data, status):
        if isinstance(data, dict):
            error = data.get("error")
            if isinstance(error, dict):
                return str(error.get("message") or error)
            if error:
                return str(error)
            if data.get("message"):
                return str(data["message"])
        return f"HTTP {status}"

    def _int_config(self, key, default, minimum=None):
        try:
            value = int(self.ctx.config.get(key, default))
        except (TypeError, ValueError):
            value = default
        if minimum is not None:
            value = max(minimum, value)
        return value

    def _float_config(self, key, default, minimum=None):
        try:
            value = float(self.ctx.config.get(key, default))
        except (TypeError, ValueError):
            value = default
        if minimum is not None:
            value = max(minimum, value)
        return value
