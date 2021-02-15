from unittest.mock import MagicMock, patch

import aerospike
import pytest

import faust
from faust.stores.aerospike import AeroSpikeStore

class TestAerospikeStore:


    # @pytest.fixture()
    # def aero(self):
    #     with patch("faust.stores.aerospike.aerospike") as aero:
    #         yield aero

    @pytest.mark.asyncio
    async def test_get_aerospike_client_new(self):
        client_mock = MagicMock()
        aerospike.client = MagicMock(return_value=client_mock)
        client_mock.connect = MagicMock()
        faust.stores.aerospike.aerospike_client = None
        config = {"k": "v"}
        return_value = AeroSpikeStore.get_aerospike_client(config)
        assert aerospike.client.called
        assert client_mock.connect.called
        assert return_value == client_mock

    @pytest.mark.asyncio
    async def test_get_aerospike_client_error(self):
        client_mock = MagicMock()
        aerospike.client = MagicMock(return_value=client_mock)
        client_mock.connect = MagicMock(side_effect=Exception)
        faust.stores.aerospike.aerospike_client = None
        config = {"k": "v"}
        with pytest.raises(Exception):
            AeroSpikeStore.get_aerospike_client(config)



    @pytest.mark.asyncio
    async def test_get_aerospike_client_instantiated(self):
        client_mock = MagicMock()
        aerospike.client = MagicMock(return_value=client_mock)
        faust.stores.aerospike.aerospike_client = MagicMock()
        config = {"k": "v"}
        AeroSpikeStore.get_aerospike_client(config)
        assert not aerospike.client.called

    @pytest.fixture()
    def store(self):
        store = AeroSpikeStore("aerospike://", MagicMock(), MagicMock(), 'test', 'test')
        store.namespace = 'test_ns'
        store.client = MagicMock()
        return store

    def test_get_correct_value(self, store) :
        bin = {'value_key' : 'value'}
        store.client.get = MagicMock(return_value=(MagicMock(), MagicMock(), bin))
        ret = store._get(b'test_get')
        assert ret == bin[AeroSpikeStore.BIN_KEY]

    def test_get_none_value(self, store) :
        store.client.get = MagicMock(return_value=(MagicMock(), MagicMock(), None))
        ret = store._get(b'test_get')
        assert ret is None

    def test_get_keyerror(self, store) :
        store.client.get = MagicMock(side_effect=aerospike.exception.RecordNotFound)
        with pytest.raises(KeyError):
            store._get(b'test_get')

    def test_get_exception(self, store) :
        store.client.get = MagicMock(side_effect=Exception)
        with pytest.raises(Exception) :
            store._get(b'test_get')

    def test_set_success(self, store, ) :
        store.client.put = MagicMock()
        key = b'key'
        value = b'value'
        vt = {store.BIN_KEY : value}
        store._set(key, value)
        putkey = (store.namespace, store.table_name, key)
        store.client.put.assert_called_with(key=putkey, bins=vt, meta={"ttl": aerospike.TTL_NEVER_EXPIRE}, policy={
                    "exists": aerospike.POLICY_EXISTS_IGNORE,
                    "key": aerospike.POLICY_KEY_SEND,
                })

    def test_set_exception(self, store) :
        store.client.put = MagicMock(side_effect=Exception)
        key = b'key'
        value = b'value'
        vt = {store.BIN_KEY : value}
        with pytest.raises(Exception):
            store._set(key, value)


    def test_persisted_offset(self, store):
        return_value = store.persisted_offset(MagicMock())
        assert return_value is None

    def test_del_success(self, store):
        key = b'key'
        del_key = (store.namespace, store.table_name, key)
        store.client.remove = MagicMock()
        store._del(key)
        store.client.remove.assert_called_with(key=del_key)

    def test_del_exception(self, store):
        key = b'key'
        del_key = (store.namespace, store.table_name, key)
        store.client.remove = MagicMock(side_effect=Exception)
        with pytest.raises(KeyError):
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
        scan_opts = {
            "concurrent" : True,
            "nobins" : True,
            "priority" : aerospike.SCAN_PRIORITY_MEDIUM,
        }
        scan_result = {(('UUID1', 't1', 'key1'), MagicMock(), MagicMock()), (('UUID2', 't2', 'key2'), MagicMock(), MagicMock())}
        scan.results = MagicMock(return_value=scan_result)
        a = {'key1', 'key2'}
        result = a - set(store._iterkeys())
        assert len(result) == 0
        scan.results.assert_called_with(policy=scan_opts)
        store.client.scan.assert_called_with(namespace=store.namespace, set=store.table_name)


    def test_itervalues_success(self, store):
        scan = MagicMock()
        store.client.scan = MagicMock(return_value=scan)
        scan_opts = {"concurrent" : True, "priority" : aerospike.SCAN_PRIORITY_MEDIUM}
        scan_result = {(MagicMock(), MagicMock(), (store.BIN_KEY, 'value1')), (MagicMock(), MagicMock(), None)}

        scan.results = MagicMock(return_value=scan_result)
        a = {None, None}
        result = a - set(store._itervalues())
        assert len(result) == 0
        scan.results.assert_called_with(policy=scan_opts)
        store.client.scan.assert_called_with(namespace=store.namespace, set=store.table_name)












