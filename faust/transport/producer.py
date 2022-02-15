"""Producer.

The Producer is responsible for:

   - Holds reference to the transport that created it
   - ... and the app via ``self.transport.app``.
   - Sending messages.
"""
import asyncio
import time
from typing import Any, Awaitable, Mapping, Optional, cast

from mode import Seconds, Service, get_logger
from mode.threads import ServiceThread

from faust.types import AppT, HeadersArg
from faust.types.transports import ProducerBufferT, ProducerT, TransportT
from faust.types.tuples import TP, FutureMessage, RecordMetadata

__all__ = ["Producer"]
logger = get_logger(__name__)


class ProducerBuffer(Service, ProducerBufferT):
    app: Optional[AppT] = None
    max_messages = 100
    queue: Optional[asyncio.Queue] = None

    def __post_init__(self) -> None:
        self.pending = asyncio.Queue()
        self.message_sent = asyncio.Event()

    def put(self, fut: FutureMessage) -> None:
        """Add message to buffer.

        The message will be eventually produced, you can await
        the future to wait for that to happen.
        """
        if self.app.conf.producer_threaded:
            if not self.queue:
                self.queue = self.threaded_producer.event_queue
            asyncio.run_coroutine_threadsafe(
                self.queue.put(fut), self.threaded_producer.thread_loop
            )
        else:
            self.pending.put_nowait(fut)

    async def on_stop(self) -> None:
        await self.flush()

    async def flush(self) -> None:
        """Flush all messages (draining the buffer)."""
        await self.flush_atmost(None)

    async def flush_atmost(self, max_messages: Optional[int]) -> int:
        """Flush at most ``n`` messages."""
        flushed_messages = 0
        while True:
            if self.state != "running" and self.size:
                raise RuntimeError("Cannot flush: Producer not Running")
            if self.size != 0 and (
                (max_messages is None or flushed_messages < max_messages)
            ):
                self.message_sent.clear()
                await self.message_sent.wait()
                flushed_messages += 1
            else:
                return flushed_messages

    async def _send_pending(self, fut: FutureMessage) -> None:
        await fut.message.channel.publish_message(fut, wait=False)

    async def wait_until_ebb(self) -> None:
        """Wait until buffer is of an acceptable size.

        Modifying a table key is using the Python dictionary API,
        and as ``__getitem__`` is synchronous we have to add
        pending messages to a buffer.

        The ``__getitem__`` method cannot drain the buffer as doing
        so requires trampolining into the event loop.

        To solve this, we have the conductor wait until the buffer
        is of an acceptable size before resuming stream processing flow.
        """
        if self.size > self.max_messages:
            logger.warning(f"producer buffer full size {self.size}")
            start_time = time.time()
            await self.flush_atmost(self.max_messages)
            end_time = time.time()
            logger.info(f"producer flush took {end_time-start_time}")

    @Service.task
    async def _handle_pending(self) -> None:
        get_pending = self.pending.get
        send_pending = self._send_pending
        while not self.should_stop:
            msg = await get_pending()
            await send_pending(msg)
            self.message_sent.set()

    @property
    def size(self) -> int:
        """Current buffer size (messages waiting to be produced)."""
        if self.app.conf.producer_threaded:
            if not self.queue:
                return 0
            queue_items = self.queue._queue  # type: ignore
        else:
            queue_items = self.pending._queue
        queue_items = cast(list, queue_items)
        return len(queue_items)


class Producer(Service, ProducerT):
    """Base Producer."""

    app: AppT

    _api_version: str
    threaded_producer: Optional[ServiceThread] = None

    def __init__(
        self,
        transport: TransportT,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ) -> None:
        self.transport = transport
        self.app = self.transport.app
        conf = self.transport.app.conf
        self.client_id = conf.broker_client_id
        self.linger_ms = int(conf.producer_linger * 1000)
        self.max_batch_size = conf.producer_max_batch_size
        self.acks = conf.producer_acks
        self.max_request_size = conf.producer_max_request_size
        self.compression_type = conf.producer_compression_type
        self.request_timeout = conf.producer_request_timeout
        self.ssl_context = conf.ssl_context
        self.credentials = conf.broker_credentials
        self.partitioner = conf.producer_partitioner
        api_version = self._api_version = conf.producer_api_version
        assert api_version is not None
        super().__init__(loop=loop or self.transport.loop, **kwargs)
        self.buffer = ProducerBuffer(loop=self.loop, beacon=self.beacon)
        if conf.producer_threaded:
            self.threaded_producer = self.create_threaded_producer()
            self.buffer.threaded_producer = self.threaded_producer
        self.buffer.app = self.app

    async def on_start(self) -> None:
        await self.add_runtime_dependency(self.buffer)

    async def send(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: Optional[str] = None,
    ) -> Awaitable[RecordMetadata]:
        """Schedule message to be sent by producer."""
        raise NotImplementedError()

    def send_soon(self, fut: FutureMessage) -> None:
        self.buffer.put(fut)

    async def send_and_wait(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: Optional[str] = None,
    ) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        raise NotImplementedError()

    async def flush(self) -> None:
        """Flush all in-flight messages."""
        # XXX subclasses must call self.buffer.flush() here.
        ...

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Optional[Mapping[str, Any]] = None,
        timeout: Seconds = 1000.0,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        ensure_created: bool = False,
    ) -> None:
        """Create/declare topic on server."""
        raise NotImplementedError()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Hash key to determine partition."""
        raise NotImplementedError()

    async def begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id."""
        raise NotImplementedError()

    async def commit_transaction(self, transactional_id: str) -> None:
        """Commit transaction by id."""
        raise NotImplementedError()

    async def abort_transaction(self, transactional_id: str) -> None:
        """Abort and rollback transaction by id."""
        raise NotImplementedError()

    async def stop_transaction(self, transactional_id: str) -> None:
        """Stop transaction by id."""
        raise NotImplementedError()

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id, if not already started."""
        raise NotImplementedError()

    async def commit_transactions(
        self,
        tid_to_offset_map: Mapping[str, Mapping[TP, int]],
        group_id: str,
        start_new_transaction: bool = True,
    ) -> None:
        """Commit transactions."""
        raise NotImplementedError()

    def supports_headers(self) -> bool:
        """Return :const:`True` if headers are supported by this transport."""
        return False
