import tempfile
import unittest
from webqq_app.store import MessageStore


class MessageFixTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = MessageStore(data_dir=self.directory.name)

    def test_equal_timestamp_pagination(self):
        self.store._data['group_1'].extend({'message_id': i, 'time': 100} for i in range(60))
        page = self.store.get_messages('group_1', limit=50)
        older = self.store.get_messages('group_1', before_message_id=page[0]['message_id'])
        self.assertEqual(len(older), 10)
        with self.assertRaises(ValueError):
            self.store.get_messages('group_1', before_message_id=999)

    def test_chat_scope_and_ambiguity(self):
        for chat in ('group_1', 'group_2'):
            self.store._data[chat].append({'message_id': -1})
        self.assertIsNone(self.store.find_message(-1))
        self.assertIsNone(self.store.find_message(-1, chat_id='group_3'))
        self.store.mark_recalled(-1, chat_id='group_1')
        self.assertTrue(self.store._data['group_1'][0]['recalled'])
        self.assertNotIn('recalled', self.store._data['group_2'][0])

    def test_repeated_removal_and_add(self):
        self.store._data['group_1'].append({'message_id': -1, 'reactions': [{'emoji_id': '5', 'count': 2, 'users': [{'user_id': 1}, {'user_id': 2}]}]})
        for _ in range(2):
            result = self.store.apply_reactions(-1, [{'emoji_id': '5', 'count': 1}], chat_id='group_1', notice_user={'user_id': 2}, operation='remove')
            self.assertEqual(result['reactions'][0]['count'], 1)
            self.assertEqual(result['reactions'][0]['users'], [{'user_id': 1}])
        result = self.store.apply_reactions(-1, [{'emoji_id': '5', 'count': 1}], chat_id='group_1', notice_user={'user_id': 1}, operation='remove')
        self.assertEqual(result['reactions'], [])
