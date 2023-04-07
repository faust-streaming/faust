from pathlib import Path
from typing import List, Mapping, Tuple
from unittest.mock import Mock, call, patch

import pytest
from yarl import URL

from faust.exceptions import ImproperlyConfigured
from faust.stores import rocksdb
from faust.stores.rocksdb import RocksDBOptions, Store
from faust.types import TP
from tests.helpers import AsyncMock

TP1 = TP("foo", 0)
TP2 = TP("foo", 1)
TP3 = TP("bar", 2)
TP4 = TP("baz", 3)


class MockIterator(Mock):
    @classmethod
    def from_values(cls, values):
        it = cls()
        it.values = values
        return it

    def __iter__(self):
        return iter(self.values)


class TestRocksDBOptions:
    @pytest.mark.parametrize(
        "arg",
        [
            "max_open_files",
            "write_buffer_size",
            "max_write_buffer_number",
            "target_file_size_base",
            "block_cache_size",
            "block_cache_compressed_size",
            "bloom_filter_size",
        ],
    )
    def test_init(self, arg):
        opts = RocksDBOptions(**{arg: 30})
        assert getattr(opts, arg) == 30

    def test_defaults(self):
        opts = RocksDBOptions()
        assert opts.max_open_files == rocksdb.DEFAULT_MAX_OPEN_FILES
        assert opts.write_buffer_size == rocksdb.DEFAULT_WRITE_BUFFER_SIZE
        assert opts.max_write_buffer_number == rocksdb.DEFAULT_MAX_WRITE_BUFFER_NUMBER
        assert opts.target_file_size_base == rocksdb.DEFAULT_TARGET_FILE_SIZE_BASE
        assert opts.block_cache_size == rocksdb.DEFAULT_BLOCK_CACHE_SIZE
        assert (
            opts.block_cache_compressed_size
            == rocksdb.DEFAULT_BLOCK_CACHE_COMPRESSED_SIZE
        )
        assert opts.bloom_filter_size == rocksdb.DEFAULT_BLOOM_FILTER_SIZE

    def test_open_rocksdb(self):
        with patch("faust.stores.rocksdb.rocksdb", Mock()) as rocks:
            opts = RocksDBOptions(use_rocksdict=False)
            db = opts.open(Path("foo.db"), read_only=True)
            rocks.DB.assert_called_once_with(
                "foo.db", opts.as_options(), read_only=True
            )
            assert db is rocks.DB()

    @pytest.mark.skip("Need to make mock BlockBasedOptions")
    def test_open_rocksdict(self):
        with patch("faust.stores.rocksdb.rocksdict", Mock()) as rocks:
            opts = RocksDBOptions(use_rocksdict=True)
            db = opts.open(Path("foo.db"), read_only=True)
            rocks.DB.assert_called_once_with(
                "foo.db", opts.as_options(), read_only=True
            )
            assert db is rocks.DB()


class Test_Store_RocksDB:
    @pytest.fixture()
    def table(self):
        table = Mock(name="table")
        table.name = "table1"
        return table

    @pytest.fixture()
    def rocks(self):
        with patch("faust.stores.rocksdb.rocksdb") as rocks:
            yield rocks

    @pytest.fixture()
    def rocksdict(self):
        with patch("faust.stores.rocksdb.rocksdict") as rocksdict:
            yield rocksdict

    @pytest.fixture()
    def no_rocks(self):
        with patch("faust.stores.rocksdb.rocksdb", None) as rocks:
            yield rocks

    @pytest.fixture()
    def no_rocksdict(self):
        with patch("faust.stores.rocksdb.rocksdict", None) as rocksdict:
            yield rocksdict

    @pytest.fixture()
    def store(self, *, app, rocks, table):
        return Store("rocksdb://", app, table, driver="python-rocksdb")

    @pytest.fixture()
    def db_for_partition(self, *, store):
        dfp = store._db_for_partition = Mock(name="db_for_partition")
        return dfp

    def test_default_key_index_size(self, *, store):
        assert store.key_index_size == store.app.conf.table_key_index_size

    def test_set_key_index_size(self, *, app, rocks, table):
        s = Store("rocksdb://", app, table, key_index_size=12341)

        assert s.key_index_size == 12341

    def test_no_rocksdb(self, *, app, table, no_rocks, no_rocksdict):
        with pytest.raises(ImproperlyConfigured):
            Store("rocksdb://", app, table)

    def test_url_without_path_adds_table_name(self, *, store):
        assert store.url == URL("rocksdb:table1")

    def test_url_having_path(self, *, app, rocks, table):
        store = Store("rocksdb://foobar/", app, table)
        assert store.url == URL("rocksdb://foobar/")

    def test_init(self, *, store, app):
        assert isinstance(store.rocksdb_options, RocksDBOptions)
        assert store.key_index_size == app.conf.table_key_index_size
        assert store._dbs == {}
        assert store._key_index is not None

    def test_persisted_offset(self, *, store, db_for_partition):
        db_for_partition.return_value.get.return_value = "300"
        assert store.persisted_offset(TP1) == 300
        db_for_partition.assert_called_once_with(TP1.partition)
        db_for_partition.return_value.get.assert_called_once_with(store.offset_key)

        db_for_partition.return_value.get.return_value = None
        assert store.persisted_offset(TP1) is None

    def test_set_persisted_offset(self, *, store, db_for_partition):
        store.set_persisted_offset(TP1, 3003)
        db_for_partition.assert_called_once_with(TP1.partition)
        db_for_partition.return_value.put.assert_called_once_with(
            store.offset_key,
            b"3003",
        )

    @pytest.mark.asyncio
    async def test_need_active_standby_for(self, *, store, db_for_partition):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            db_for_partition.side_effect = KeyError("lock acquired")
            assert not await store.need_active_standby_for(TP1)

    @pytest.mark.asyncio
    async def test_need_active_standby_for__raises(self, *, store, db_for_partition):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            db_for_partition.side_effect = KeyError("oh no")
            with pytest.raises(KeyError):
                await store.need_active_standby_for(TP1)

    @pytest.mark.asyncio
    async def test_need_active_standby_for__active(self, *, store, db_for_partition):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            assert await store.need_active_standby_for(TP1)

    def test_apply_changelog_batch(self, *, store, rocks, db_for_partition):
        def new_event(name, tp: TP, offset, key, value) -> Mock:
            return Mock(
                name="event1",
                message=Mock(
                    tp=tp,
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset,
                    key=key,
                    value=value,
                ),
            )

        events = [
            new_event("event1", TP1, 1001, "k1", "v1"),
            new_event("event2", TP2, 2002, "k2", "v2"),
            new_event("event3", TP3, 3003, "k3", "v3"),
            new_event("event4", TP4, 4004, "k4", "v4"),
            new_event("event5", TP4, 4005, "k5", None),
        ]

        dbs = {
            TP1.partition: Mock(name="db1"),
            TP2.partition: Mock(name="db2"),
            TP3.partition: Mock(name="db3"),
            TP4.partition: Mock(name="db4"),
        }
        db_for_partition.side_effect = dbs.get

        store.set_persisted_offset = Mock(name="set_persisted_offset")

        store.apply_changelog_batch(events, None, None)

        rocks.WriteBatch.return_value.delete.assert_called_once_with("k5")
        rocks.WriteBatch.return_value.put.assert_has_calls(
            [
                call("k1", "v1"),
                call("k2", "v2"),
                call("k3", "v3"),
                call("k4", "v4"),
            ]
        )

        for db in dbs.values():
            db.write.assert_called_once_with(rocks.WriteBatch())

        store.set_persisted_offset.assert_has_calls(
            [
                call(TP1, 1001),
                call(TP2, 2002),
                call(TP3, 3003),
                call(TP4, 4005),
            ]
        )

    @pytest.fixture()
    def current_event(self):
        with patch("faust.stores.rocksdb.current_event") as current_event:
            yield current_event.return_value

    def test__set(self, *, store, db_for_partition, current_event):
        store._set(b"key", b"value")
        db_for_partition.assert_called_once_with(current_event.message.partition)
        assert store._key_index[b"key"] == current_event.message.partition
        db_for_partition.return_value.put.assert_called_once_with(
            b"key",
            b"value",
        )

    def test_db_for_partition(self, *, store):
        ofp = store._open_for_partition = Mock(name="open_for_partition")

        assert store._db_for_partition(1) is ofp.return_value
        assert store._dbs[1] is ofp.return_value

        assert store._db_for_partition(1) is ofp.return_value

        ofp.assert_called_once_with(1)

    def test_open_for_partition(self, *, store):
        open = store.rocksdb_options.open = Mock(name="options.open")
        assert store._open_for_partition(1) is open.return_value
        open.assert_called_once_with(store.partition_path(1), read_only=False)

    def test__get__missing(self, *, store):
        store._get_bucket_for_key = Mock(name="get_bucket_for_key")
        store._get_bucket_for_key.return_value = None
        assert store._get(b"key") is None

    def test__get(self, *, store):
        # Tests scenario where event is None, (db, value) contains value
        db = Mock(name="db")
        value = b"foo"
        store._get_bucket_for_key = Mock(name="get_bucket_for_key")
        store._get_bucket_for_key.return_value = (db, value)

        assert store._get(b"key") == value

    def test__get__dbvalue_is_None(self, *, store):
        db = Mock(name="db")
        store._get_bucket_for_key = Mock(name="get_bucket_for_key")
        store._get_bucket_for_key.return_value = (db, None)

        db.key_may_exist.return_value = [False]
        assert store._get(b"key") is None

        db.key_may_exist.return_value = [True]
        db.get.return_value = None
        assert store._get(b"key") is None

        db.get.return_value = b"bar"
        assert store._get(b"key") == b"bar"

    def test__get__has_event(self, *, store, current_event):
        partition = 1
        message = Mock(name="message")
        message.partition.return_value = partition

        current_event.return_value = message

        db = Mock(name="db")
        store._db_for_partition = Mock("_db_for_partition")
        store._db_for_partition.return_value = db
        db.get.return_value = b"value"
        store.table = Mock(name="table")
        store.table.is_global = False
        store.table.synchronize_all_active_partitions = False
        store.table.use_partitioner = False

        assert store._get(b"key") == b"value"

        db.get.return_value = None
        assert store._get(b"key2") is None

    def test__get__has_event_value_diff_partition(self, *, store, current_event):
        partition = 1
        message = Mock(name="message")
        message.partition.return_value = partition

        current_event.return_value = message

        dbs = {}
        event_partition = current_event.message.partition
        next_partition = event_partition + 1
        dbs[event_partition] = Mock(name="db")
        dbs[next_partition] = Mock(name="db")

        dbs[event_partition].get.return_value = None
        dbs[next_partition].get.return_value = b"value"

        store._db_for_partition = Mock("_db_for_partition")
        store._db_for_partition.return_value = dbs[current_event.message.partition]

        store._dbs.update(dbs)
        store._get_bucket_for_key = Mock(name="get_bucket_for_key")
        store._get_bucket_for_key.return_value = (dbs[next_partition], b"value")

        store.table = Mock(name="table")
        store.table.is_global = False
        store.table.synchronize_all_active_partitions = False
        store.table.use_partitioner = False

        # A _get call from a stream, to a non-global, non-partitioner, table
        # uses partition of event
        # Which in this intentional case, is the wrong partition
        assert store._get(b"key") is None

        store.table.is_global = True
        store.table.synchronize_all_active_partitions = True
        store.table.use_partitioner = False

        # A global table ignores the event partition and pulls from the proper db
        assert store._get(b"key") == b"value"

        store.table.is_global = False
        store.table.use_partitioner = True

        # A custom-partitioned table also ignores the event partition
        assert store._get(b"key") == b"value"

        store.table.is_global = True
        store.table.use_partitioner = True

        # A global, custom-partitioned table will also ignore the event partition
        assert store._get(b"key") == b"value"

    def test_get_bucket_for_key__is_in_index(self, *, store):
        store._key_index[b"key"] = 30
        db = store._dbs[30] = Mock(name="db-p30")

        db.key_may_exist.return_value = [False]
        assert store._get_bucket_for_key(b"key") is None

        db.key_may_exist.return_value = [True]
        db.get.return_value = None
        assert store._get_bucket_for_key(b"key") is None

        db.get.return_value = b"value"
        assert store._get_bucket_for_key(b"key") == (db, b"value")

    def test_get_bucket_for_key__no_dbs(self, *, store):
        assert store._get_bucket_for_key(b"key") is None

    def new_db(self, name, exists=False):
        db = Mock(name=name)
        db.key_may_exist.return_value = [exists]
        db.get.return_value = name
        return db

    def test_get_bucket_for_key__not_in_index(self, *, store):
        dbs = {
            1: self.new_db(name="db1"),
            2: self.new_db(name="db2"),
            3: self.new_db(name="db3", exists=True),
            4: self.new_db(name="db4", exists=True),
        }
        store._dbs.update(dbs)

        assert store._get_bucket_for_key(b"key") == (dbs[3], "db3")

    def test__del(self, *, store):
        dbs = store._dbs_for_key = Mock(
            return_value=[
                Mock(name="db1"),
                Mock(name="db2"),
                Mock(name="db3"),
            ]
        )
        store._del(b"key")
        for db in dbs.return_value:
            db.delete.assert_called_once_with(b"key")

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, store, table):
        store.revoke_partitions = Mock(name="revoke_partitions")
        store.assign_partitions = AsyncMock(name="assign_partitions")

        assigned = {TP1, TP2}
        revoked = {TP3}
        newly_assigned = {TP2}
        generation_id = 1
        await store.on_rebalance(
            assigned, revoked, newly_assigned, generation_id=generation_id
        )

        store.revoke_partitions.assert_called_once_with(table, revoked)
        store.assign_partitions.assert_called_once_with(
            table, newly_assigned, generation_id
        )

    def test_revoke_partitions(self, *, store, table):
        table.changelog_topic.topics = {TP1.topic, TP3.topic}
        store._dbs[TP3.partition] = Mock(name="db")

        store.revoke_partitions(table, {TP1, TP2, TP3, TP4})
        assert not store._dbs

    @pytest.mark.asyncio
    async def test_assign_partitions(self, *, store, app, table):
        app.assignor.assigned_standbys = Mock(return_value={TP4})
        table.changelog_topic.topics = list({tp.topic for tp in (TP1, TP2, TP4)})

        store._try_open_db_for_partition = AsyncMock()
        generation_id = 1
        await store.assign_partitions(table, {TP1, TP2, TP3, TP4}, generation_id)
        store._try_open_db_for_partition.assert_has_calls(
            [
                call(TP2.partition, generation_id=generation_id),
                call(TP1.partition, generation_id=generation_id),
            ],
            any_order=True,
        )

    @pytest.mark.asyncio
    async def test_assign_partitions__empty_assignment(self, *, store, app, table):
        app.assignor.assigned_standbys = Mock(return_value={TP4})
        table.changelog_topic.topics = list({tp.topic for tp in (TP1, TP2, TP4)})
        await store.assign_partitions(table, set())

    @pytest.mark.asyncio
    async def test_open_db_for_partition(self, *, store, db_for_partition):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            assert (
                await store._try_open_db_for_partition(3)
                is db_for_partition.return_value
            )

    @pytest.mark.asyncio
    async def test_open_db_for_partition_max_retries(self, *, store, db_for_partition):
        store.sleep = AsyncMock(name="sleep")
        store._dbs = {"test": None}
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            db_for_partition.side_effect = KeyError("lock already")
            with pytest.raises(KeyError):
                await store._try_open_db_for_partition(3)
        assert store.sleep.call_count == 29
        assert len(store._dbs) == 0

    @pytest.mark.asyncio
    async def test_open_db_for_partition__raises_unexpected_error(
        self, *, store, db_for_partition
    ):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):
            db_for_partition.side_effect = RuntimeError()
            with pytest.raises(RuntimeError):
                await store._try_open_db_for_partition(3)

    @pytest.mark.asyncio
    async def test_open_db_for_partition_retries_recovers(
        self, *, store, db_for_partition
    ):
        with patch("faust.stores.rocksdb.rocksdb.errors.RocksIOError", KeyError):

            def on_call(partition):
                if db_for_partition.call_count < 3:
                    raise KeyError("lock already")

            db_for_partition.side_effect = on_call

            await store._try_open_db_for_partition(3)

    def test__contains(self, *, store):
        db1 = self.new_db("db1", exists=False)
        db2 = self.new_db("db2", exists=True)
        dbs = {b"key": [db1, db2]}
        store._dbs_for_key = Mock(side_effect=dbs.get)

        db2.get.return_value = None
        assert not store._contains(b"key")

        db2.get.return_value = b"value"
        assert store._contains(b"key")

    def test__contains__has_event(self, *, store, current_event):
        # Test "happy-path", call comes in from stream on same partition as key
        partition = 1
        message = Mock(name="message")
        message.partition.return_value = partition

        current_event.return_value = message

        store.table = Mock(name="table")
        store.table.is_global = False
        store.table.use_partitioner = False

        dbs = {}
        event_partition = current_event.message.partition
        next_partition = event_partition + 1
        dbs[event_partition] = Mock(name="db")
        dbs[next_partition] = Mock(name="db")

        dbs[event_partition].get.return_value = b"value"
        dbs[event_partition].key_may_exist.return_value = (True,)
        dbs[next_partition].get.return_value = False
        dbs[next_partition].key_may_exist.return_value = (False,)

        store._db_for_partition = Mock("_db_for_partition")
        store._db_for_partition.return_value = dbs[current_event.message.partition]

        # A _get call from a stream, to a non-global, non-partitioner, table
        # uses partition of event
        # Which in this intentional case, is the wrong partition
        assert store._contains(b"key")

    def test__contains__has_event_value_diff_partition(self, *, store, current_event):
        partition = 1
        message = Mock(name="message")
        message.partition.return_value = partition

        current_event.return_value = message

        dbs = {}
        event_partition = current_event.message.partition
        next_partition = event_partition + 1
        dbs[event_partition] = Mock(name="db")
        dbs[next_partition] = Mock(name="db")

        dbs[event_partition].get.return_value = None
        dbs[event_partition].key_may_exist.return_value = False
        dbs[next_partition].get.return_value = b"value"
        dbs[next_partition].key_may_exist.return_value = (True,)

        store._db_for_partition = Mock("_db_for_partition")
        store._db_for_partition.return_value = dbs[current_event.message.partition]

        store._dbs.update(dbs)
        store._dbs_for_key = Mock(name="_dbs_for_key")
        store._dbs_for_key.return_value = [dbs[next_partition]]

        store.table = Mock(name="table")
        store.table.is_global = False
        store.table.use_partitioner = False

        # A _get call from a stream, to a non-global, non-partitioner, table
        # uses partition of event
        # Which in this intentional case, is the wrong partition
        assert not store._contains(b"key")

        store.table.is_global = True
        store.table.use_partitioner = False

        # A global table ignores the event partition and pulls from the proper db
        assert store._contains(b"key")

        store.table.is_global = False
        store.table.use_partitioner = True

        # A custom-partitioned table also ignores the event partition
        assert store._contains(b"key")

        store.table.is_global = True
        store.table.use_partitioner = True

        # A global, custom-partitioned table will also ignore the event partition
        assert store._contains(b"key")

    def test__dbs_for_key(self, *, store):
        dbs = store._dbs = {
            1: self.new_db("db1"),
            2: self.new_db("db2"),
            3: self.new_db("db3"),
        }
        store._key_index[b"key"] = 2

        assert list(store._dbs_for_key(b"other")) == list(dbs.values())
        assert list(store._dbs_for_key(b"key")) == [dbs[2]]

    def test__dbs_for_actives(self, *, store, table):
        table.changelog_topic_name = "clog"
        store.app.assignor.assigned_actives = Mock(
            return_value=[
                TP("clog", 1),
                TP("clog", 2),
            ]
        )
        dbs = store._dbs = {
            1: self.new_db("db1"),
            2: self.new_db("db2"),
            3: self.new_db("db3"),
        }

        # Normal Table
        table.is_global = False
        assert list(store._dbs_for_actives()) == [dbs[1], dbs[2]]

        # Global Table
        table.is_global = True
        assert list(store._dbs_for_actives()) == [dbs[1], dbs[2], dbs[3]]

        # Global Global Table
        table.is_global = True
        table.synchronize_all_active_partitions = True
        assert list(store._dbs_for_actives()) == [dbs[1], dbs[2], dbs[3]]

    def test__size(self, *, store):
        dbs = self._setup_keys(
            db1=[
                store.offset_key,
                b"foo",
                b"bar",
            ],
            db2=[
                b"baz",
                store.offset_key,
                b"xuz",
                b"xaz",
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)
        assert store._size() == 5

    def test__iterkeys(self, *, store):
        dbs = self._setup_keys(
            db1=[
                store.offset_key,
                b"foo",
                b"bar",
            ],
            db2=[
                b"baz",
                store.offset_key,
                b"xuz",
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iterkeys()) == [
            b"foo",
            b"bar",
            b"baz",
            b"xuz",
        ]

        for db in dbs:
            db.iterkeys.assert_called_once_with()
            db.iterkeys().seek_to_first.assert_called_once_with()

    def _setup_keys(self, **dbs: Mapping[str, List[bytes]]):
        return [self._setup_keys_db(name, values) for name, values in dbs.items()]

    def _setup_keys_db(self, name: str, values: List[bytes]):
        db = self.new_db(name)
        db.iterkeys.return_value = MockIterator.from_values(values)
        db.keys.return_value = MockIterator.from_values(values)  # supports rocksdict
        return db

    def test__itervalues(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b"1001"),
                (b"k1", b"foo"),
                (b"k2", b"bar"),
            ],
            db2=[
                (b"k3", b"baz"),
                (store.offset_key, b"2002"),
                (b"k4", b"xuz"),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._itervalues()) == [
            b"foo",
            b"bar",
            b"baz",
            b"xuz",
        ]

        for db in dbs:
            db.iteritems.assert_called_once_with()
            db.iteritems().seek_to_first.assert_called_once_with()

    def _setup_items(self, **dbs: Mapping[str, List[Tuple[bytes, bytes]]]):
        return [self._setup_items_db(name, values) for name, values in dbs.items()]

    def _setup_items_db(self, name: str, values: List[Tuple[bytes, bytes]]):
        db = self.new_db(name)
        db.iteritems.return_value = MockIterator.from_values(values)
        db.items.return_value = MockIterator.from_values(values)  # supports rocksdict
        return db

    def test__iteritems(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b"1001"),
                (b"k1", b"foo"),
                (b"k2", b"bar"),
            ],
            db2=[
                (b"k3", b"baz"),
                (store.offset_key, b"2002"),
                (b"k4", b"xuz"),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iteritems()) == [
            (b"k1", b"foo"),
            (b"k2", b"bar"),
            (b"k3", b"baz"),
            (b"k4", b"xuz"),
        ]

        for db in dbs:
            db.iteritems.assert_called_once_with()
            db.iteritems().seek_to_first.assert_called_once_with()

    def test_clear(self, *, store):
        with pytest.raises(NotImplementedError):
            store._clear()

    def test_reset_state(self, *, store):
        with patch("shutil.rmtree") as rmtree:
            store.reset_state()
            rmtree.assert_called_once_with(store.path.absolute())


class Test_Store_Rocksdict(Test_Store_RocksDB):
    @pytest.fixture()
    def store(self, *, app, rocks, table):
        return Store("rocksdb://", app, table, driver="rocksdict")

    def new_db(self, name, exists=False):
        db = Mock(name=name)
        db.key_may_exist.return_value = exists
        db.get.return_value = name
        return db

    def test__contains(self, *, store):
        db1 = self.new_db("db1", exists=False)
        db2 = self.new_db("db2", exists=True)
        dbs = {b"key": [db1, db2]}
        store._dbs_for_key = Mock(side_effect=dbs.get)

        db2.get.return_value = None
        assert not store._contains(b"key")

        db2.get.return_value = b"value"
        assert store._contains(b"key")

    def test__iteritems(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b"1001"),
                (b"k1", b"foo"),
                (b"k2", b"bar"),
            ],
            db2=[
                (b"k3", b"baz"),
                (store.offset_key, b"2002"),
                (b"k4", b"xuz"),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iteritems()) == [
            (b"k1", b"foo"),
            (b"k2", b"bar"),
            (b"k3", b"baz"),
            (b"k4", b"xuz"),
        ]

        for db in dbs:
            # iteritems not available in rocksdict yet
            db.items.assert_called_once_with()

    def test__iterkeys(self, *, store):
        dbs = self._setup_keys(
            db1=[
                store.offset_key,
                b"foo",
                b"bar",
            ],
            db2=[
                b"baz",
                store.offset_key,
                b"xuz",
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._iterkeys()) == [
            b"foo",
            b"bar",
            b"baz",
            b"xuz",
        ]

        for db in dbs:
            # iterkeys not available in rocksdict yet
            db.keys.assert_called_once_with()

    def test__itervalues(self, *, store):
        dbs = self._setup_items(
            db1=[
                (store.offset_key, b"1001"),
                (b"k1", b"foo"),
                (b"k2", b"bar"),
            ],
            db2=[
                (b"k3", b"baz"),
                (store.offset_key, b"2002"),
                (b"k4", b"xuz"),
            ],
        )
        store._dbs_for_actives = Mock(return_value=dbs)

        assert list(store._itervalues()) == [
            b"foo",
            b"bar",
            b"baz",
            b"xuz",
        ]

        for db in dbs:
            # items must be used instead of iteritems for now
            # TODO: seek_to_first() should be called once rocksdict is updated
            db.items.assert_called_once_with()

    def test__get__dbvalue_is_None(self, *, store):
        db = Mock(name="db")
        store._get_bucket_for_key = Mock(name="get_bucket_for_key")
        store._get_bucket_for_key.return_value = (db, None)

        db.key_may_exist.return_value = False
        assert store._get(b"key") is None

        db.key_may_exist.return_value = True
        db.get.return_value = None
        assert store._get(b"key") is None

        db.get.return_value = b"bar"
        assert store._get(b"key") == b"bar"

    def test_get_bucket_for_key__is_in_index(self, *, store):
        store._key_index[b"key"] = 30
        db = store._dbs[30] = Mock(name="db-p30")

        db.key_may_exist.return_value = False
        assert store._get_bucket_for_key(b"key") is None

        db.key_may_exist.return_value = True
        db.get.return_value = None
        assert store._get_bucket_for_key(b"key") is None

        db.get.return_value = b"value"
        assert store._get_bucket_for_key(b"key") == (db, b"value")

    def test_apply_changelog_batch(self, *, store, rocksdict, db_for_partition):
        def new_event(name, tp: TP, offset, key, value) -> Mock:
            return Mock(
                name="event1",
                message=Mock(
                    tp=tp,
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset,
                    key=key,
                    value=value,
                ),
            )

        events = [
            new_event("event1", TP1, 1001, "k1", "v1"),
            new_event("event2", TP2, 2002, "k2", "v2"),
            new_event("event3", TP3, 3003, "k3", "v3"),
            new_event("event4", TP4, 4004, "k4", "v4"),
            new_event("event5", TP4, 4005, "k5", None),
        ]

        dbs = {
            TP1.partition: Mock(name="db1"),
            TP2.partition: Mock(name="db2"),
            TP3.partition: Mock(name="db3"),
            TP4.partition: Mock(name="db4"),
        }
        db_for_partition.side_effect = dbs.get

        store.set_persisted_offset = Mock(name="set_persisted_offset")

        store.apply_changelog_batch(events, None, None)

        rocksdict.WriteBatch.return_value.delete.assert_called_once_with("k5")
        rocksdict.WriteBatch.return_value.put.assert_has_calls(
            [
                call("k1", "v1"),
                call("k2", "v2"),
                call("k3", "v3"),
                call("k4", "v4"),
            ]
        )

        for db in dbs.values():
            db.write.assert_called_once_with(rocksdict.WriteBatch(raw_mode=True))

        store.set_persisted_offset.assert_has_calls(
            [
                call(TP1, 1001),
                call(TP2, 2002),
                call(TP3, 3003),
                call(TP4, 4005),
            ]
        )
