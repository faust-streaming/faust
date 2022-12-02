from unittest.mock import Mock

import pytest

from faust import Event
from faust.stores.memory import Store
from faust.types import TP


class Test_Store:
    @pytest.fixture
    def store(self, *, app):
        return Store(url="memory://", app=app, table=Mock(name="table"))

    def test_clear(self, *, store):
        store.data["foo"] = 1
        store._clear()
        assert not store.data

    def test_apply_changelog_batch(self, *, store):
        event, to_key, to_value = self.mock_event_to_key_value()
        store.apply_changelog_batch([event], to_key=to_key, to_value=to_value)

        to_key.assert_called_once_with(b"key")
        to_value.assert_called_once_with(b"value")
        assert store.data[to_key()] == to_value()

    def test_apply_changelog_batch__deletes_key_for_None_value(self, *, store):
        self.test_apply_changelog_batch(store=store)
        event2, to_key, to_value = self.mock_event_to_key_value(value=None)

        assert to_key() in store.data
        store.apply_changelog_batch([event2], to_key=to_key, to_value=to_value)

        assert to_key() not in store.data

    def test_apply_changelog_batch__deletes_key_and_reassign_it(self, *, store):
        self.test_apply_changelog_batch__deletes_key_for_None_value(store=store)

        events = [self.mock_event(value=value) for value in ("v1", None, "v2")]
        to_key, to_value = self.mock_to_key_value(events[0])

        store.apply_changelog_batch(events, to_key=to_key, to_value=to_value)
        assert to_key() in store.data

    def test_apply_changelog_batch__different_partitions(self, *, store):
        events = [
            self.mock_event(key=f"key-{i}".encode(), partition=i) for i in range(2)
        ]
        to_key, to_value = self.mock_to_key_value_multi(events)

        store.apply_changelog_batch(events, to_key=to_key, to_value=to_value)

        assert to_key.call_args_list[0][0][0] in store.data
        assert to_key.call_args_list[1][0][0] in store.data

        assert store._key_partition.get(to_key.call_args_list[0][0][0]) == 0
        assert store._key_partition.get(to_key.call_args_list[1][0][0]) == 1

    def test_apply_changelog_batch__different_partitions_deletion(self, *, store):
        self.test_apply_changelog_batch__different_partitions(store=store)

        events = [
            self.mock_event(key=f"key-{i}".encode(), value=None, partition=i)
            for i in range(2)
        ]
        to_key, to_value = self.mock_to_key_value_multi(events)

        store.apply_changelog_batch(events, to_key=to_key, to_value=to_value)

        assert not store._key_partition
        assert not store.data

    @pytest.mark.asyncio
    async def test_apply_changelog_batch__different_partitions_repartition_single(
        self, *, store
    ):
        self.test_apply_changelog_batch__different_partitions(store=store)

        await store.on_recovery_completed({TP("foo", 0)}, set())

        assert len(store.data) == 1
        assert len(store._key_partition) == 1

    @pytest.mark.asyncio
    async def test_apply_changelog_batch__different_partitions_repartition_multi(
        self, *, store
    ):
        self.test_apply_changelog_batch__different_partitions(store=store)

        await store.on_recovery_completed({TP("foo", 0), TP("foo", 1)}, set())

        assert len(store.data) == 2
        assert len(store._key_partition) == 2

    def mock_event_to_key_value(self, key=b"key", value=b"value", partition=0):
        event = self.mock_event(key=key, value=value, partition=partition)
        to_key, to_value = self.mock_to_key_value(event)
        return event, to_key, to_value

    def mock_event(self, key=b"key", value=b"value", partition=0):
        event = Mock(name="event", autospec=Event)
        event.key = key
        event.value = value
        event.message.key = key
        event.message.value = value
        event.message.partition = partition
        return event

    def mock_to_key_value(self, event):
        to_key = Mock(name="to_key")
        to_key.return_value = event.key
        to_value = Mock(name="to_value")
        to_value.return_value = event.value
        return to_key, to_value

    def mock_to_key_value_multi(self, events):
        to_key = Mock(name="to_key")
        to_key.side_effect = [e.key for e in events]
        to_value = Mock(name="to_value")
        to_value.side_effect = [e.value for e in events]
        return to_key, to_value

    def test_persisted_offset(self, *, store):
        assert store.persisted_offset(TP("foo", 0)) is None

    def test_reset_state(self, *, store):
        store.reset_state()
