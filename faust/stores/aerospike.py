import typing
from typing import Any, Dict, Iterator, Optional, Tuple, Union

try:  # pragma: no cover
    import aerospike
except ImportError:  # pragma: no cover
    aerospike = None  # noqa

from yarl import URL

from faust.stores import base
from faust.types import TP, AppT, CollectionT

if typing.TYPE_CHECKING:  # pragma: no cover
    from aerospike import SCAN_PRIORITY_MEDIUM, TTL_NEVER_EXPIRE, Client
else:

    class Client:  # noqa
        """Dummy Client."""

    TTL_NEVER_EXPIRE = -1
    SCAN_PRIORITY_MEDIUM = 2


if typing.TYPE_CHECKING:  # pragma: no cover
    import aerospike.exception.RecordNotFound
else:

    class RecordNotFound:  # noqa
        """Dummy Client."""


aerospike_client: Client = None


class AeroSpikeStore(base.SerializedStore):
    """Aerospike table storage."""

    client: Client
    ttl: int
    policies: typing.Mapping[str, Any]
    BIN_KEY = "value_key"
    USERNAME_KEY = "username"
    HOSTS_KEY = "hosts"
    PASSWORD_KEY = "password"  # nosec
    NAMESPACE_KEY = "namespace"
    TTL_KEY = "ttl"
    POLICIES_KEY = "policies"
    CLIENT_OPTIONS_KEY = "client"

    def __init__(
        self,
        url: Union[str, URL],
        app: AppT,
        table: CollectionT,
        options: typing.Mapping[str, Any] = None,
        **kwargs: Any,
    ) -> None:
        try:
            self.client = AeroSpikeStore.get_aerospike_client(options)
            self.namespace = options.get(self.NAMESPACE_KEY, "")
            self.ttl = options.get(self.TTL_KEY, aerospike.TTL_NEVER_EXPIRE)
            self.policies = options.get(self.POLICIES_KEY, None)
        except Exception as ex:
            self.logger.error(f"Error configuring aerospike client {ex}")
            raise ex
        super().__init__(url, app, table, **kwargs)

    @staticmethod
    def get_aerospike_client(aerospike_config: Dict[Any, Any]) -> Client:
        global aerospike_client
        if aerospike_client:
            return aerospike_client
        else:
            client = aerospike.client(
                aerospike_config.get(AeroSpikeStore.CLIENT_OPTIONS_KEY)
            )
            try:
                client.connect(
                    aerospike_config.get(AeroSpikeStore.USERNAME_KEY),
                    aerospike_config.get(AeroSpikeStore.PASSWORD_KEY),
                )
                aerospike_client = client
                return client
            except Exception as e:
                raise e

    def _get(self, key: bytes) -> Optional[bytes]:
        key = (self.namespace, self.table_name, key)
        try:
            (key, meta, bins) = self.client.get(key=key)
            if bins:
                return bins[self.BIN_KEY]
            return None
        except aerospike.exception.RecordNotFound as ex:
            self.log.debug(f"key not found {key} exception {ex}")
            raise KeyError(f"key not found {key}")
        except Exception as ex:
            self.log.error(f"Error in set for table {self.table_name} exception {ex}")
            raise ex

    def _set(self, key: bytes, value: Optional[bytes]) -> None:
        try:
            key = (self.namespace, self.table_name, key)
            vt = {self.BIN_KEY: value}
            self.client.put(
                key=key,
                bins=vt,
                meta={"ttl": self.ttl},
                policy={
                    "exists": aerospike.POLICY_EXISTS_IGNORE,
                    "key": aerospike.POLICY_KEY_SEND,
                },
            )
        except Exception as ex:
            self.log.error(f"Error in set for table {self.table_name} exception {ex}")
            raise ex

    def _del(self, key: bytes) -> None:
        try:
            key = (self.namespace, self.table_name, key)
            self.client.remove(key=key)
        except aerospike.exception.RecordNotFound as ex:
            self.log.error(
                f"Error in delete for table {self.table_name} exception {ex}"
            )
        except Exception as ex:
            self.log.error(
                f"Error in delete for table {self.table_name} exception {ex}"
            )
            raise ex

    def _iterkeys(self) -> Iterator[bytes]:
        try:
            scan: aerospike.Scan = self.client.scan(
                namespace=self.namespace, set=self.table_name
            )
            for result in scan.results():
                yield result[0][2]
        except Exception as ex:
            self.log.error(
                f"Error in _iterkeys for table {self.table_name} exception {ex}"
            )
            raise ex

    def _itervalues(self) -> Iterator[bytes]:
        try:
            scan: aerospike.Scan = self.client.scan(
                namespace=self.namespace, set=self.table_name
            )
            for result in scan.results():
                (key, meta, bins) = result
                if bins:
                    yield bins[self.BIN_KEY]
                else:
                    yield None
        except Exception as ex:
            self.log.error(
                f"Error in _itervalues for table {self.table_name} exception {ex}"
            )
            raise ex

    def _iteritems(self) -> Iterator[Tuple[bytes, bytes]]:
        try:

            scan: aerospike.Scan = self.client.scan(
                namespace=self.namespace, set=self.table_name
            )
            for result in scan.results():
                (key_data, meta, bins) = result
                (ns, set, policy, key) = key_data

                if bins:
                    bins = bins[self.BIN_KEY]
                yield key, bins
        except Exception as ex:
            self.log.error(
                f"Error in _iteritems for table {self.table_name} exception {ex}"
            )
            raise ex

    def _size(self) -> int:
        return 0

    def _contains(self, key: bytes) -> bool:
        try:
            if self.app.conf.store_check_exists:
                key = (self.namespace, self.table_name, key)
                (key, meta) = self.client.exists(key=key)
                if meta:
                    return True
                else:
                    return False
            else:
                return True
        except Exception as ex:
            self.log.error(
                f"Error in _contains for table {self.table_name} exception {ex}"
            )
            raise ex

    def _clear(self) -> None:
        pass

    def reset_state(self) -> None:
        pass

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the persisted offset.

        This always returns :const:`None` when using the aerospike store.
        """
        return None
