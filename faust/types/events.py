import abc
import typing
from typing import (
    Any,
    AsyncContextManager,
    Awaitable,
    Generic,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

from .codecs import CodecArg
from .core import HeadersArg, K, V
from .tuples import Message, MessageSentCallback, RecordMetadata

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
    from .channels import ChannelT as _ChannelT
    from .serializers import SchemaT as _SchemaT
else:

    class _AppT: ...  # noqa

    class _ChannelT: ...  # noqa

    class _SchemaT: ...  # noqa


T = TypeVar("T")


class EventT(Generic[T], AsyncContextManager):
    app: _AppT
    key: K
    value: T
    headers: Mapping
    message: Message
    acked: bool

    __slots__ = ("app", "key", "value", "headers", "message", "acked")

    @abc.abstractmethod
    def __init__(
        self,
        app: _AppT,
        key: K,
        value: V,
        headers: Optional[HeadersArg],
        message: Message,
    ) -> None: ...

    @abc.abstractmethod
    async def send(
        self,
        channel: Union[str, _ChannelT],
        key: Optional[K] = None,
        value: Optional[V] = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: Optional[HeadersArg] = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: Optional[CodecArg] = None,
        value_serializer: Optional[CodecArg] = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]: ...

    @abc.abstractmethod
    async def forward(
        self,
        channel: Union[str, _ChannelT],
        key: Optional[Any] = None,
        value: Optional[Any] = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: Optional[HeadersArg] = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: Optional[CodecArg] = None,
        value_serializer: Optional[CodecArg] = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]: ...

    @abc.abstractmethod
    def ack(self) -> bool: ...
