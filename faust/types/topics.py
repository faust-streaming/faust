import abc
import asyncio
import typing
from typing import (
    Any, Mapping, MutableSet, Optional, Pattern, Sequence, Set, Union,
)
from mode import Seconds, ServiceT
from .channels import ChannelT
from .codecs import CodecArg
from .tuples import TP
from ..utils.futures import ThrowableQueue

if typing.TYPE_CHECKING:
    from .app import AppT
    from .models import ModelArg
    from .streams import StreamT
    from .transports import ConsumerT, TPorTopicSet
else:
    class AppT: ...             # noqa
    class ModelArg: ...         # noqa
    class StreamT: ...          # noqa
    class ConsumerT: ...        # noqa
    class TPorTopicSet: ...     # noqa

__all__ = ['TopicT', 'ConductorT']


class TopicT(ChannelT):
    topics: Sequence[str]
    pattern: Pattern
    retention: Seconds
    compacting: bool
    deleting: bool
    replicas: int
    config: Mapping[str, Any]
    acks: bool
    internal: bool

    @abc.abstractmethod
    def __init__(self, app: AppT,
                 *,
                 topics: Sequence[str] = None,
                 pattern: Union[str, Pattern] = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 is_iterator: bool = False,
                 partitions: int = None,
                 retention: Seconds = None,
                 compacting: bool = None,
                 deleting: bool = None,
                 replicas: int = None,
                 acks: bool = True,
                 internal: bool = False,
                 config: Mapping[str, Any] = None,
                 queue: ThrowableQueue = None,
                 key_serializer: CodecArg = None,
                 value_serializer: CodecArg = None,
                 maxsize: int = 1,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        ...

    @abc.abstractmethod
    async def pause(self) -> None:
        ...

    @abc.abstractmethod
    async def resume(self) -> None:
        ...

    @property
    @abc.abstractmethod
    def pattern(self) -> Optional[Pattern]:
        ...

    @pattern.setter
    def pattern(self, pattern: Union[str, Pattern]) -> None:
        ...

    @property
    @abc.abstractmethod
    def partitions(self) -> int:
        ...

    @partitions.setter
    def partitions(self, partitions: int) -> None:
        ...

    @property
    @abc.abstractmethod
    def replicas(self) -> int:
        ...

    @replicas.setter
    def replicas(self, replicas: int) -> None:
        ...

    @abc.abstractmethod
    def derive(self, **kwargs: Any) -> ChannelT:
        ...

    @abc.abstractmethod
    def derive_topic(self,
                     *,
                     topics: Sequence[str] = None,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
                     partitions: int = None,
                     retention: Seconds = None,
                     compacting: bool = None,
                     deleting: bool = None,
                     internal: bool = False,
                     config: Mapping[str, Any] = None,
                     prefix: str = '',
                     suffix: str = '',
                     **kwargs: Any) -> 'TopicT':
        ...


class ConductorT(ServiceT, MutableSet[ChannelT]):

    app: AppT

    @abc.abstractmethod
    def __init__(self, app: AppT, **kwargs: Any) -> None:
        ...

    @abc.abstractmethod
    def acks_enabled_for(self, topic: str) -> bool:
        ...

    @abc.abstractmethod
    async def commit(self, topics: TPorTopicSet) -> bool:
        ...

    @abc.abstractmethod
    async def wait_for_subscriptions(self) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        ...

    @abc.abstractmethod
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        ...
