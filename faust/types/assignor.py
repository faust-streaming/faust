import abc
import typing
from typing import List, MutableMapping, Set
from mode import ServiceT
from .topics import TP


TopicToPartitionMap = MutableMapping[str, List[int]]
HostToPartitionMap = MutableMapping[str, TopicToPartitionMap]


if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...      # noqa


class PartitionAssignorT(abc.ABC):

    replicas: int
    app: AppT

    @abc.abstractmethod
    def assigned_standbys(self) -> Set[TP]:
        ...

    @abc.abstractmethod
    def assigned_actives(self) -> Set[TP]:
        ...

    @abc.abstractmethod
    def is_active(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def is_standby(self, tp: TP) -> bool:
        ...

    @abc.abstractmethod
    def key_store(self, topic: str, key: bytes) -> str:
        ...

    @abc.abstractmethod
    def table_metadata(self, topic: str) -> HostToPartitionMap:
        ...

    @abc.abstractmethod
    def tables_metadata(self) -> HostToPartitionMap:
        ...


class LeaderAssignorT(ServiceT):

    app: AppT

    @abc.abstractmethod
    def is_leader(self) -> bool:
        ...
