import abc
import typing
from typing import Any, Awaitable, Generic, Mapping, Optional, TypeVar, Union

from mode.utils.typing import AsyncContextManager

from .codecs import CodecArg
from .core import HeadersArg, K, V
from .tuples import Message, MessageSentCallback, RecordMetadata

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
    from .channels import ChannelT as _ChannelT
    from .serializers import SchemaT as _SchemaT
else:

    class _AppT:
        ...  # noqa

    class _ChannelT:
        ...  # noqa

    class _SchemaT:
        ...  # noqa


T = TypeVar("T")


class EventT(Generic[T], AsyncContextManager):

    app: _AppT
    key: K
    value: V
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
    ) -> None:
        ...

    @abc.abstractmethod
    async def send(
        self,
        channel: Union[str, _ChannelT],
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    async def forward(
        self,
        channel: Union[str, _ChannelT],
        key: Any = None,
        value: Any = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
        force: bool = False,
    ) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def ack(self) -> bool:
        ...
