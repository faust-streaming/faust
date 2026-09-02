import asyncio
import threading
import typing
from collections import defaultdict
from time import time
from typing import (
    Any,
    Awaitable,
    Callable,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    Union,
    cast,
)

from .codecs import CodecArg
from .core import HeadersArg, K, OpenHeadersArg, V

if typing.TYPE_CHECKING:
    from .channels import ChannelT as _ChannelT
    from .transports import ConsumerT as _ConsumerT
else:

    class _ChannelT: ...  # noqa

    class _ConsumerT: ...  # noqa


__all__ = [
    "ConsumerMessage",
    "FutureMessage",
    "Message",
    "MessageSentCallback",
    "PendingMessage",
    "RecordMetadata",
    "TP",
    "tp_set_to_map",
]

MessageSentCallback = Callable[["FutureMessage"], Union[None, Awaitable[None]]]


class TP(NamedTuple):
    topic: str
    partition: int


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    topic_partition: TP
    offset: int
    timestamp: Optional[float] = None
    timestamp_type: Optional[int] = None


class PendingMessage(NamedTuple):
    channel: _ChannelT
    key: K
    value: V
    partition: Optional[int]
    timestamp: Optional[float]
    headers: Optional[OpenHeadersArg]
    key_serializer: CodecArg
    value_serializer: CodecArg
    callback: Optional[MessageSentCallback]
    topic: Optional[str] = None
    offset: Optional[int] = None
    generation_id: Optional[int] = None


def _PendingMessage_to_Message(p: PendingMessage) -> "Message":
    # CPython3.6.0 does not support methods on NamedTuple [ask]

    # In-memory channel.send uses this to convert
    # PendingMessage to Message.
    topic = cast(str, p.topic)
    partition = cast(int, p.partition) or 0
    tp = TP(topic, partition)
    timestamp = cast(float, p.timestamp) or time()
    timestamp_type = 1 if p.timestamp else 0
    return Message(
        topic,
        partition,
        -1,
        timestamp=timestamp,
        timestamp_type=timestamp_type,
        headers=p.headers,
        key=p.key,
        value=p.value,
        checksum=None,
        tp=tp,
        generation_id=p.generation_id,
    )


class FutureMessage(asyncio.Future, Awaitable[RecordMetadata]):
    message: PendingMessage

    def __init__(self, message: PendingMessage) -> None:
        self.message = message
        super().__init__()

    def set_result(self, result: RecordMetadata) -> None:
        super().set_result(result)


def _get_len(s: Optional[bytes]) -> int:
    return len(s) if s is not None and isinstance(s, bytes) else 0


#: Serializes the acknowledgement transition: the joint read-modify-write of
#: ``Message.acked`` and ``Message.refcount`` together with the final-ack
#: bookkeeping it triggers in the consumer.
#:
#: Not a free-threading concern alone.  ``self.refcount = self.refcount - n``
#: compiles to LOAD_ATTR / BINARY_OP / STORE_ATTR, and the GIL is released
#: between bytecodes, so two threads acking the same message can read the same
#: refcount and both store ``n - 1``.  Measured on GIL-enabled CPython 3.11,
#: 32 threads acking one message: 9 of 300 trials lost a decrement, leaving
#: the final ack to fire twice or never.  Removing the GIL widens that window
#: rather than opening it.
#:
#: The lock is process-wide rather than per-message because the state it
#: guards is: the final ack mutates the consumer's ``_acked_index``,
#: ``_acked``, ``_n_acked`` and ``_unacked_messages``, which every message
#: shares.  A per-message lock would leave all of that unprotected.
#:
#: Reentrant because the transition nests -- ``Message.ack`` calls
#: ``ConsumerMessage.on_final_ack``, which calls ``Consumer.ack``, which takes
#: the same lock to guard the bookkeeping when reached on its own.
#:
#: Uncontended in the ordinary case: faust acks from the event loop thread, so
#: this is a single uncontended acquire per ack, against the dict and set
#: operations the same section already performs.  It matters when
#: ``Event.ack`` is called from another thread, which is public API.
ack_lock = threading.RLock()


class Message:
    __slots__ = (
        "topic",
        "partition",
        "offset",
        "timestamp",
        "timestamp_type",
        "headers",
        "key",
        "value",
        "checksum",
        "serialized_key_size",
        "serialized_value_size",
        "acked",
        "refcount",
        "time_in",
        "time_out",
        "time_total",
        "tp",
        "tracked",
        "span",
        "__weakref__",
        "generation_id",
    )

    use_tracking: bool = False

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: int,
        timestamp: float,
        timestamp_type: int,
        headers: Optional[HeadersArg],
        key: Optional[bytes],
        value: Optional[bytes],
        checksum: Optional[bytes],
        serialized_key_size: Optional[int] = None,
        serialized_value_size: Optional[int] = None,
        tp: Optional[TP] = None,
        time_in: Optional[float] = None,
        time_out: Optional[float] = None,
        time_total: Optional[float] = None,
        generation_id: Optional[int] = None,
    ) -> None:
        self.topic: str = topic
        self.partition: int = partition
        self.offset: int = offset
        self.timestamp: float = timestamp
        self.timestamp_type: int = timestamp_type
        self.headers: Optional[HeadersArg] = headers
        self.key: Optional[bytes] = key
        self.value: Optional[bytes] = value
        self.checksum: Optional[bytes] = checksum
        self.serialized_key_size: int = (
            _get_len(key) if serialized_key_size is None else serialized_key_size
        )
        self.serialized_value_size: int = (
            _get_len(value) if serialized_value_size is None else serialized_value_size
        )
        self.acked: bool = False
        self.refcount: int = 0
        self.tp = tp if tp is not None else TP(topic, partition)
        self.tracked: bool = not self.use_tracking

        #: Monotonic timestamp of when the consumer received this message.
        self.time_in: Optional[float] = time_in
        #: Monotonic timestamp of when the consumer acknowledged this message.
        self.time_out: Optional[float] = time_out
        #: Total processing time (in seconds), or None if the event is
        #: still processing.
        self.time_total: Optional[float] = time_total

        # In some edge cases a message can slip through to the stream from before a
        # rebalance occured if it gets stuck in the conductor or somewhere else. We
        # track the generation_id when the message is fetched so we can discard if
        # needed.
        self.generation_id: Optional[int] = generation_id

    def ack(self, consumer: _ConsumerT, n: int = 1) -> bool:
        # The whole decision is one critical section, not just the decrement.
        # `acked` and `refcount` are read, compared and written together, and
        # the final-ack bookkeeping downstream keys off the result, so a
        # thread switch anywhere between them loses acks or runs the final ack
        # twice.  See `ack_lock`.
        with ack_lock:
            if not self.acked:
                # if no more references, mark offset as safe-to-commit in
                # Consumer.
                if not self.decref(n):
                    return self.on_final_ack(consumer)
            return False

    def on_final_ack(self, consumer: _ConsumerT) -> bool:
        self.acked = True
        return True

    def incref(self, n: int = 1) -> None:
        with ack_lock:
            self.refcount += n

    def decref(self, n: int = 1) -> int:
        with ack_lock:
            refcount = self.refcount = max(self.refcount - n, 0)
            return refcount

    @classmethod
    def from_message(cls, message: Any, tp: TP) -> "Message":
        return cls(
            message.topic,
            message.partition,
            message.offset,
            message.timestamp,
            message.timestamp_type,
            message.headers,
            message.key,
            message.value,
            message.checksum,
            message.serialized_key_size,
            message.serialized_value_size,
            tp,
        )

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {self.tp} offset={self.offset}>"


class ConsumerMessage(Message):
    """Message type used by Kafka Consumer."""

    use_tracking = True

    def on_final_ack(self, consumer: _ConsumerT) -> bool:
        return consumer.ack(self)


def tp_set_to_map(tps: Set[TP]) -> MutableMapping[str, Set[TP]]:
    # convert revoked/assigned to mapping of topic to partitions
    tpmap: MutableMapping[str, Set[TP]] = defaultdict(set)
    for tp in tps:
        tpmap[tp.topic].add(tp)
    return tpmap


# XXX See top of module! We redefine this with final FutureMessage
# for Sphinx as it cannot read non-final types.
MessageSentCallback = Callable[  # type: ignore
    [FutureMessage], Union[None, Awaitable[None]]
]
