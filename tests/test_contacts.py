import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import webqq_app.api as api
from webqq_app.napcat import NapCatConnection
from webqq_app.request_store import ContactRequestStore


class ContactRequestStoreTests(unittest.TestCase):
    def test_requests_persist_and_deduplicate(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            event = {
                "request_type": "friend", "sub_type": "add", "flag": "123",
                "user_id": 42, "comment": "hello", "time": 100,
            }
            first, created = store.upsert(event)
            duplicate, duplicate_created = store.upsert(event)
            self.assertTrue(created)
            self.assertFalse(duplicate_created)
            self.assertEqual(first["id"], duplicate["id"])
            store.update(first["id"], status="approved", decision="approve")

            restored = ContactRequestStore(tmp)
            self.assertEqual(restored.pending_count(), 0)
            self.assertEqual(restored.get(first["id"])["status"], "approved")

    def test_backfill_does_not_reset_handled_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            item, _ = store.upsert({"request_type": "group", "sub_type": "invite", "flag": "9"})
            store.update(item["id"], status="rejected", decision="reject")
            restored, created = store.upsert({
                "request_type": "group", "sub_type": "invite", "flag": "9", "group_name": "Test",
            }, source="backfill")
            self.assertFalse(created)
            self.assertEqual(restored["status"], "rejected")
            self.assertEqual(restored["group_name"], "Test")


class ContactRequestNapCatTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_mode_records_without_approving(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            connection = NapCatConnection("", "", SimpleNamespace(), config={
                "auto_approve_requests": False,
            }, request_store=store)
            calls = []
            broadcasts = []

            async def request(action, params, timeout=10):
                calls.append((action, params))
                return {"status": "ok"}

            async def broadcast(payload):
                broadcasts.append(payload)

            connection._request = request
            connection._broadcast = broadcast
            await connection._handle_request({
                "request_type": "friend", "sub_type": "add", "flag": "100", "user_id": 42,
            })
            self.assertEqual(calls, [])
            self.assertEqual(store.pending_count(), 1)
            self.assertEqual(broadcasts[-1]["type"], "contact_request_update")

    async def test_auto_mode_preserves_existing_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            connection = NapCatConnection("", "", SimpleNamespace(), config={
                "auto_approve_requests": True,
            }, request_store=store)
            calls = []

            async def request(action, params, timeout=10):
                calls.append((action, params))
                return {"status": "ok"}

            async def broadcast(payload):
                return None

            connection._request = request
            connection._broadcast = broadcast
            await connection._handle_request({"request_type": "friend", "sub_type": "add", "flag": "1"})
            await connection._handle_request({"request_type": "group", "sub_type": "invite", "flag": "2"})
            await connection._handle_request({"request_type": "group", "sub_type": "add", "flag": "3"})
            self.assertEqual(calls, [
                ("set_friend_add_request", {"flag": "1", "approve": True}),
                ("set_group_add_request", {"flag": "2", "approve": True}),
            ])
            statuses = {item["flag"]: item["status"] for item in store.list()}
            self.assertEqual(statuses, {"1": "approved", "2": "approved", "3": "pending"})

    async def test_manual_actions_use_4182_payloads_and_record_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            friend, _ = store.upsert({"request_type": "friend", "sub_type": "add", "flag": "10"})
            group, _ = store.upsert({"request_type": "group", "sub_type": "add", "flag": "11"})
            connection = NapCatConnection("", "", SimpleNamespace(), request_store=store)
            calls = []

            async def request(action, params, timeout=10):
                calls.append((action, params, timeout))
                if action == "set_group_add_request":
                    return {"status": "failed", "message": "expired"}
                return {"status": "ok"}

            async def broadcast(payload):
                return None

            connection._request = request
            connection._broadcast = broadcast
            await connection.act_on_contact_request(friend["id"], True, remark="Work")
            failed, response = await connection.act_on_contact_request(group["id"], False, reason="No")
            self.assertEqual(calls[0], ("set_friend_add_request", {
                "flag": "10", "approve": True, "remark": "Work",
            }, 30))
            self.assertEqual(calls[1], ("set_group_add_request", {
                "flag": "11", "approve": False, "reason": "No",
            }, 30))
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["error"], "expired")
            self.assertEqual(response["status"], "failed")

    async def test_profile_friend_adapters_use_4182_actions(self):
        connection = NapCatConnection("", "", SimpleNamespace())
        calls = []

        async def request(action, params, timeout=10):
            calls.append((action, params, timeout))
            if action == "get_friends_with_category":
                return {"status": "ok", "data": [{
                    "categoryId": 1, "categoryName": "Work", "buddyList": [{"user_id": 7}],
                }]}
            return {"status": "ok", "data": {}}

        connection._request = request
        friends = await connection.get_friends_with_categories()
        await connection.update_self_profile("New name", "Signature")
        await connection.delete_friend("7")
        await connection.set_self_avatar(Path("/tmp/avatar.png"))
        self.assertEqual(friends["categories"][0]["name"], "Work")
        by_action = {name: (params, timeout) for name, params, timeout in calls}
        self.assertEqual(by_action["set_qq_profile"][0], {"nickname": "New name", "personal_note": "Signature"})
        self.assertEqual(by_action["delete_friend"][0], {"user_id": "7"})
        self.assertTrue(by_action["set_qq_avatar"][0]["file"].startswith("file://"))

    async def test_doubtful_friend_backfill_uses_dedicated_4182_action(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            item, _ = store.upsert({
                "request_type": "friend", "sub_type": "add", "flag": "doubt-1",
            }, source="doubt_backfill")
            connection = NapCatConnection("", "", SimpleNamespace(), request_store=store)
            calls = []

            async def request(action, params, timeout=10):
                calls.append((action, params))
                return {"status": "ok"}

            async def broadcast(payload):
                return None

            connection._request = request
            connection._broadcast = broadcast
            await connection.act_on_contact_request(item["id"], True)
            self.assertEqual(calls, [("set_doubt_friends_add_request", {
                "flag": "doubt-1", "approve": True,
            })])


class ContactApiTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def request(app, body=None, match_info=None, query=None):
        async def request_json():
            return dict(body or {})
        return SimpleNamespace(
            app=app, match_info=match_info or {}, query=query or {}, cookies={}, headers={}, remote="",
            json=request_json,
        )

    async def test_setting_is_persisted_without_approving_pending_requests(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            store.upsert({"request_type": "friend", "sub_type": "add", "flag": "1"})
            broadcasts = []

            async def broadcast(payload):
                broadcasts.append(payload)

            app = {
                "config": {"web_token": "", "auto_approve_requests": False},
                "request_store": store,
                "napcat": SimpleNamespace(_broadcast=broadcast),
            }
            request = self.request(app, {"auto_approve_requests": True})
            with patch.object(api, "save_config") as save:
                response = await api.handle_contact_settings_update(request)
            body = json.loads(response.text)
            self.assertTrue(body["auto_approve_requests"])
            self.assertEqual(store.pending_count(), 1)
            save.assert_called_once_with(app["config"])
            self.assertEqual(broadcasts[0]["type"], "contact_settings_update")

    async def test_request_filters_and_action_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ContactRequestStore(tmp)
            item, _ = store.upsert({"request_type": "friend", "sub_type": "add", "flag": "1"})
            app = {"config": {"web_token": ""}, "request_store": store}
            listing = await api.handle_contact_requests(self.request(app, query={"status": "pending"}))
            self.assertEqual(len(json.loads(listing.text)["requests"]), 1)
            invalid = await api.handle_contact_request_action(self.request(
                app, {"approve": "yes"}, match_info={"request_id": item["id"]},
            ))
            self.assertEqual(invalid.status, 400)

    async def test_delete_friend_rejects_napcat_valid_false(self):
        async def delete_friend(user_id):
            return {"status": "ok", "data": {"valid": False, "message": "not a friend"}}

        async def broadcast(payload):
            raise AssertionError("failed deletion must not broadcast")

        app = {
            "config": {"web_token": ""},
            "napcat": SimpleNamespace(delete_friend=delete_friend, _broadcast=broadcast),
        }
        response = await api.handle_friend_delete(self.request(app, match_info={"user_id": "42"}))
        self.assertEqual(response.status, 400)
        self.assertEqual(json.loads(response.text)["error"], "not a friend")

    async def test_profile_update_returns_confirmed_identity(self):
        calls = []

        async def update(nickname, note):
            calls.append((nickname, note))
            return {"status": "ok"}

        async def profile():
            return {"user_id": 42, "nickname": "Confirmed", "long_nick": "Confirmed note"}

        async def broadcast(payload):
            calls.append(payload)

        class Store:
            _self_user = {"user_id": "self", "name": "You"}

            def set_self_user(self, user_id, name):
                self._self_user = {"user_id": str(user_id), "name": name}

        app = {
            "config": {"web_token": ""}, "store": Store(),
            "napcat": SimpleNamespace(update_self_profile=update, get_self_profile=profile, _broadcast=broadcast),
        }
        response = await api.handle_profile_update(self.request(app, {
            "nickname": "Requested", "personal_note": "Requested note",
        }))
        body = json.loads(response.text)
        self.assertEqual(body["profile"]["user_id"], "42")
        self.assertEqual(body["profile"]["nickname"], "Confirmed")
        self.assertEqual(app["store"]._self_user["user_id"], "42")
