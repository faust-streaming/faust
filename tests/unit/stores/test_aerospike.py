import sys
from unittest.mock import MagicMock, patch

import pytest

import faust
from faust.stores.aerospike import AeroSpikeStore

try:
    from aerospike.exception import RecordNotFound
except ImportError:

    class RecordNotFound(Exception):
        ...

    m1 = MagicMock()
    m2 = MagicMock()
    sys.modules["aerospike"] = m1
    m1.exception = m2
    m2.RecordNotFound = RecordNotFound


class TestAerospikeStore:
    @pytest.fixture()
    def aero(self):
        with patch("faust.stores.aerospike.aerospike") as aero:
            yield aero

    @pytest.mark.asyncio
    async def test_get_aerospike_client_new(self, aero):
        client_mock = MagicMock()
        aero.client = MagicMock(return_value=client_mock)
        client_mock.connect = MagicMock()
        faust.stores.aerospike.aerospike_client = None
        config = {"k": "v"}
        return_value = AeroSpikeStore.get_aerospike_client(config)
        assert aero.client.called
        assert client_mock.connect.called
        assert return_value == client_mock

    @pytest.mark.asyncio
    async def test_get_aerospike_client_error(self, aero):
        client_mock = MagicMock()
        aero.client = MagicMock(return_value=client_mock)
        client_mock.connect = MagicMock(side_effect=Exception)
        faust.stores.aerospike.aerospike_client = None
        config = {"k": "v"}
        with pytest.raises(Exception):
            AeroSpikeStore.get_aerospike_client(config)

    @pytest.mark.asyncio
    async def test_get_aerospike_client_instantiated(self, aero):
        client_mock = MagicMock()
        aero.client = MagicMock(return_value=client_mock)
        faust.stores.aerospike.aerospike_client = MagicMock()
        config = {"k": "v"}
        AeroSpikeStore.get_aerospike_client(config)
        assert not aero.client.called

    @pytest.fixture()
    def store(self):
        sys.modules["aerospike"] = MagicMock()
        with patch("faust.stores.aerospike.aerospike", MagicMock()):
            options = {}
            options[AeroSpikeStore.HOSTS_KEY] = "localhost"
            options[AeroSpikeStore.USERNAME_KEY] = "USERNAME"
            options[AeroSpikeStore.PASSWORD_KEY] = "PASSWORD"
            options[AeroSpikeStore.TTL_KEY] = -1
            options[AeroSpikeStore.POLICIES_KEY] = {}
            store = AeroSpikeStore(
                "aerospike://", MagicMock(), MagicMock(), options=options
            )
            store.namespace = "test_ns"
            store.client = MagicMock()
            store.app.conf.aerospike_sleep_seconds_between_retries_on_exception = 0
            store.app.conf.aerospike_retries_on_exception = 2
            store.app.conf.crash_app_on_aerospike_exception = True
            return store

    def test_get_correct_value(self, store):
        bin = {"value_key": "value"}
        store.client.get = MagicMock(return_value=(MagicMock(), MagicMock(), bin))
        ret = store._get(b"test_get")
        assert ret == bin[AeroSpikeStore.BIN_KEY]

    def test_get_none_value(self, store):
        store.client.get = MagicMock(return_value=(MagicMock(), MagicMock(), None))
        ret = store._get(b"test_get")
        assert ret is None

    def test_get_exception(self, store):
        store.client.get = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            store._get(b"test_get")

    def test_set_success(
        self,
        store,
    ):
        with patch("faust.stores.aerospike.aerospike", MagicMock()) as aero:

            store.client.put = MagicMock()
            key = b"key"
            value = b"value"
            vt = {store.BIN_KEY: value}
            store._set(key, value)
            putkey = (store.namespace, store.table_name, key)
            store.client.put.assert_called_with(
                key=putkey,
                bins=vt,
                meta={"ttl": store.ttl},
                policy={
                    "exists": aero.POLICY_EXISTS_IGNORE,
                    "key": aero.POLICY_KEY_SEND,
                },
            )

    def test_set_exception(self, store):
        store.client.put = MagicMock(side_effect=Exception)
        key = b"key"
        value = b"value"
        with pytest.raises(Exception):
            store._set(key, value)

    def test_persisted_offset(self, store):
        return_value = store.persisted_offset(MagicMock())
        assert return_value is None

    def test_del_success(self, store):
        key = b"key"
        del_key = (store.namespace, store.table_name, key)
        store.client.remove = MagicMock()
        store._del(key)
        store.client.remove.assert_called_with(key=del_key)

    def test_del_exception(self, store):
        key = b"key"
        store.client.remove = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            store._del(key)

    def test_iterkeys_error(self, store):
        scan = MagicMock()
        store.client.scan = MagicMock(side_effect=Exception)
        scan.results = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            list(store._iterkeys())

    def test_iterkeys_success(self, store):
        scan = MagicMock()
        store.client.scan = MagicMock(return_value=scan)
        scan_result = {
            (("UUID1", "t1", "key1"), MagicMock(), MagicMock()),
            (("UUID2", "t2", "key2"), MagicMock(), MagicMock()),
        }
        scan.results = MagicMock(return_value=scan_result)
        a = {"key1", "key2"}
        result = a - set(store._iterkeys())
        assert len(result) == 0
        assert scan.results.called
        store.client.scan.assert_called_with(
            namespace=store.namespace, set=store.table_name
        )

    def test_itervalues_success(self, store):
        with patch("faust.stores.aerospike.aerospike", MagicMock()):
            scan = MagicMock()
            store.client.scan = MagicMock(return_value=scan)
            scan_result = [
                (MagicMock(), {"ttl": 4294967295, "gen": 4}, {"value_key": "value1"}),
                (MagicMock(), {"ttl": 4294967295, "gen": 4}, None),
            ]
            scan.results = MagicMock(return_value=scan_result)
            a = {None, "value1"}
            result = a - set(store._itervalues())
            assert len(result) == 0
            scan.results.assert_called_with()
            store.client.scan.assert_called_with(
                namespace=store.namespace, set=store.table_name
            )

    def test_itervalues_error(self, store):
        store.client.scan = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            set(store._itervalues())

    def test_iteritems_error(self, store):
        store.client.scan = MagicMock(side_effect=Exception)
        with pytest.raises(Exception):
            set(store._iteritems())

    def test_iteritems_success(self, store):
        with patch("faust.stores.aerospike.aerospike", MagicMock()):

            scan = MagicMock()
            store.client.scan = MagicMock(return_value=scan)
            scan_result = [
                (
                    ("UUID1", "t1", MagicMock(), "key1"),
                    {"ttl": 4294967295, "gen": 4},
                    {"value_key": "value1"},
                ),
                (
                    ("UUID2", "t2", MagicMock(), "key2"),
                    {"ttl": 4294967295, "gen": 4},
                    {"value_key": "value2"},
                ),
            ]
            scan.results = MagicMock(return_value=scan_result)
            a = {("key1", "value1"), ("key2", "value2")}
            result = a - set(store._iteritems())
            assert len(result) == 0
            assert scan.results.assert_called
            store.client.scan.assert_called_with(
                namespace=store.namespace, set=store.table_name
            )

    def test_contains_error(self, store):
        store.client.exists = MagicMock(side_effect=Exception)
        key = b"key"
        with pytest.raises(Exception):
            store._contains(key)

    def test_contains_does_not_exist(self, store):
        store.client.exists = MagicMock(return_value=(None, None))
        key = b"key"
        assert store._contains(key) is False
        exist_key = (store.namespace, store.table_name, key)
        store.client.exists.assert_called_with(key=exist_key)

    def test_contains_does_exists(self, store):
        store.client.exists = MagicMock(return_value=(MagicMock(), MagicMock))
        key = b"key"
        assert store._contains(key) is True
        exist_key = (store.namespace, store.table_name, key)
        store.client.exists.assert_called_with(key=exist_key)
