import datetime
from unittest.mock import Mock, patch

import pytest

import faust
from faust.events import Event
from faust.tables.wrappers import WindowSet, WindowWrapper
from faust.types import Message


class TableKey(faust.Record):
    value: str


class TableValue(faust.Record):
    id: str
    num: int


KEY1 = TableKey("foo")
VALUE1 = TableValue("id", 3)

WINDOW1 = faust.HoppingWindow(size=10, step=2, expires=3600.0)


def event():
    message = Message(
        topic="test-topic",
        key="key",
        value="value",
        partition=3,
        offset=0,
        checksum=None,
        timestamp=datetime.datetime.now().timestamp(),
        timestamp_type=0,
        headers={},
    )
    return Event(
        app="test-app",
        key="key",
        value="value",
        headers={},
        message=message,
    )


class Test_Table:
    @pytest.fixture
    def table(self, *, app):
        return self.create_table(app, name="foo", default=int)

    @pytest.fixture
    def strict_table(self, *, app):
        return self.create_table(app, name="strict")

    def create_table(
        self, app, *, name="foo", key_type=TableKey, value_type=TableValue, **kwargs
    ):
        return app.Table(name, key_type=key_type, value_type=value_type, **kwargs)

    @patch("faust.tables.wrappers.current_event", return_value=event())
    def test_using_window(self, patch_current, *, table):
        with_wrapper = table.using_window(WINDOW1)
        self.assert_wrapper(with_wrapper, table, WINDOW1)
        self.assert_current(with_wrapper, patch_current)

    @patch("faust.tables.wrappers.current_event", return_value=event())
    def test_hopping(self, patch_current, *, table):
        with_wrapper = table.hopping(10, 2, 3600)
        self.assert_wrapper(with_wrapper, table)
        self.assert_current(with_wrapper, patch_current)

    @patch("faust.tables.wrappers.current_event", return_value=event())
    def test_tumbling(self, patch_current, *, table):
        with_wrapper = table.tumbling(10, 3600)
        self.assert_wrapper(with_wrapper, table)
        self.assert_current(with_wrapper, patch_current)

    def assert_wrapper(self, wrapper, table, window=None):
        assert wrapper.table is table
        t = wrapper.table
        if window is not None:
            assert t.window is window
        assert t._changelog_compacting
        assert t._changelog_deleting
        assert t._changelog_topic is None
        assert isinstance(wrapper, WindowWrapper)

    def assert_current(self, wrapper, patch_current):
        value = wrapper["test"]
        assert isinstance(value, WindowSet)
        patch_current.asssert_called_once_with()
        assert value.current() == 0

    def test_missing__when_default(self, *, table):
        assert table["foo"] == 0
        table.data["foo"] = 3
        assert table["foo"] == 3

    def test_missing__no_default(self, *, strict_table):
        with pytest.raises(KeyError):
            strict_table["foo"]
        strict_table.data["foo"] = 3
        assert strict_table["foo"] == 3

    def test_has_key(self, *, table):
        assert not table._has_key("foo")
        table.data["foo"] = 3
        assert table._has_key("foo")

    def test_get_key(self, *, table):
        assert table._get_key("foo") == 0
        table.data["foo"] = 3
        assert table._get_key("foo") == 3

    def test_set_key(self, *, table):
        with patch("faust.tables.base.current_event") as current_event:
            event = current_event.return_value
            partition = event.message.partition
            table.send_changelog = Mock(name="send_changelog")
            table._set_key("foo", "val")
            table.send_changelog.asssert_called_once_with(partition, "foo", "val")
            assert table["foo"] == "val"

    def test_del_key(self, *, table):
        with patch("faust.tables.base.current_event") as current_event:
            event = current_event.return_value
            partition = event.message.partition
            table.send_changelog = Mock(name="send_changelog")
            table.data["foo"] = 3
            table._del_key("foo")
            table.send_changelog.asssert_called_once_with(partition, "foo", None)
            assert "foo" not in table.data

    def test_as_ansitable(self, *, table):
        table.data["foo"] = "bar"
        table.data["bar"] = "baz"
        assert table.as_ansitable(sort=True)
        assert table.as_ansitable(sort=False)

    def test_on_key_set__no_event(self, *, table):
        with patch("faust.tables.base.current_event") as ce:
            ce.return_value = None
            with pytest.raises(TypeError):
                table.on_key_set("k", "v")

    def test_on_key_del__no_event(self, *, table):
        with patch("faust.tables.base.current_event") as ce:
            ce.return_value = None
            with pytest.raises(TypeError):
                table.on_key_del("k")
