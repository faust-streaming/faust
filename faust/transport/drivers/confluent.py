"""Message transport using :pypi:`confluent_kafka`."""
import asyncio
import os
import struct
import typing
import weakref
from collections import defaultdict
from time import monotonic
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Type,
    cast,
)

import confluent_kafka
from confluent_kafka import KafkaException, TopicPartition as _TopicPartition
from confluent_kafka.admin import AdminClient
from mode import Service, get_logger
from mode.threads import QueueServiceThread
from mode.utils.futures import notify
from mode.utils.times import Seconds, want_seconds
from yarl import URL

from faust.exceptions import ConsumerNotStarted, ProducerSendError
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TP,
    ensure_TPset,
)
from faust.types import TP, AppT, ConsumerMessage, HeadersArg, RecordMetadata
from faust.types.transports import ConsumerT, ProducerT

if typing.TYPE_CHECKING:
    from confluent_kafka import (
        Consumer as _Consumer,
        Message as _Message,
        Producer as _Producer,
    )
else:

    class _Consumer:
        ...  # noqa

    class _Producer:
        ...  # noqa

    class _Message:
        ...  # noqa


__all__ = ["Consumer", "Producer", "Transport"]


logger = get_logger(__name__)


def server_list(urls: List[URL], default_port: int) -> str:
    default_host = "127.0.0.1"
    return ",".join(
        [f"{u.host or default_host}:{u.port or default_port}" for u in urls]
    )


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`confluent_kafka`."""

    logger = logger

    def _new_consumer_thread(self) -> ConsumerThread:
        return ConfluentConsumerThread(self, loop=self.loop, beacon=self.beacon)

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Mapping[str, Any] = None,
        timeout: Seconds = 30.0,
        retention: Seconds = None,
        compacting: bool = None,
        deleting: bool = None,
        ensure_created: bool = False,
    ) -> None:
        """Create topic on broker."""
        if self.app.conf.topic_allow_declare:
            await self._thread.create_topic(
                topic,
                partitions,
                replication,
                config=config,
                timeout=int(want_seconds(timeout) * 1000.0),
                retention=int(want_seconds(retention) * 1000.0),
                compacting=compacting,
                deleting=deleting,
                ensure_created=ensure_created,
            )
        else:
            logger.warning(f"Topic creation disabled! Can't create topic {topic}")

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        # convert timestamp to seconds from int milliseconds.
        timestamp_type: int
        timestamp: Optional[int]
        timestamp_type, timestamp = record.timestamp()
        timestamp_s: float = cast(float, None)
        if timestamp is not None:
            timestamp_s = timestamp / 1000.0
        key = record.key()
        key_size = len(key) if key is not None else 0
        value = record.value()
        value_size = len(value) if value is not None else 0
        return ConsumerMessage(
            record.topic(),
            record.partition(),
            record.offset(),
            timestamp_s,
            timestamp_type,
            [],  # headers
            key,
            value,
            None,
            key_size,
            value_size,
            tp,
        )

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        return cast(TP, _TopicPartition(topic, partition))

    async def on_stop(self) -> None:
        """Call when consumer is stopping."""
        await super().on_stop()
        transport = cast(Transport, self.transport)
        # transport._topic_waiters.clear()

    def verify_event_path(self, now: float, tp: TP) -> None:
        return self._thread.verify_event_path(now, tp)


class AsyncConsumer:
    def __init__(self,
                 config, logger=None, callback=None, loop=None,
                 on_partitions_revoked=None,
                 on_partitions_assigned=None,
                 beacon=None,
                 ):
        """Construct a Consumer usable within asyncio.

        :param config: A configuration dict for this Consumer
        :param logger: A python logger instance.

        # Taken from https://github.com/stephan-hof/confluent-kafka-python/blob/69b79ad1b53d5e9058710ced63c42ebb1da2d9ec/examples/linux_asyncio_consumer.py
        """
        self.consumer = confluent_kafka.Consumer(**config)
        self.callback = callback
        self.on_partitions_revoked = on_partitions_revoked
        self.on_partitions_assigned = on_partitions_assigned
        self.beacon = beacon

        self.eventfd = os.eventfd(0, os.EFD_CLOEXEC | os.EFD_NONBLOCK)

        # This is the channel how the consumer notifies asyncio.
        self.loop = loop
        if loop is None:
            self.loop = asyncio.get_running_loop()
            self.loop.add_reader(self.eventfd, self.__eventfd_ready)
        self.consumer.io_event_enable(self.eventfd, struct.pack("@q", 1))

        self.waiters = set()

        # Close eventfd and remove it from reader if
        # self is not referenced anymore.
        self.__close_eventfd = weakref.finalize(
            self, AsyncConsumer.close_eventd, self.loop, self.eventfd
        )

    @staticmethod
    def close_eventd(loop, eventfd):
        """Internal helper method. Not part of the public API."""
        loop.remove_reader(eventfd)
        os.close(eventfd)

    def close(self):
        self.consumer.close()

    def __eventfd_ready(self):
        os.eventfd_read(self.eventfd)

        for future in self.waiters:
            if not future.done():
                future.set_result(True)

    def subscribe(self, *args, **kwargs):
        self.consumer.subscribe(*args, **kwargs)

    def assign(self, *args, **kwargs):
        self.consumer.assign(*args, **kwargs)

    async def poll(self, timeout=0):
        """Consumes a single message, calls callbacks and returns events.

        It is defined a 'async def' and returns an awaitable object a
        caller needs to deal with to get the result.
        See https://docs.python.org/3/library/asyncio-task.html#awaitables

        Which makes it safe (and mandatory) to call it directly in an asyncio
        coroutine like this: `msg = await consumer.poll()`

        If timeout > 0: Wait at most X seconds for a message.
                        Returns `None` if no message arrives in time.
        If timeout <= 0: Endless wait for a message.
        """
        if timeout > 0:
            try:
                t = await asyncio.wait_for(self._poll_no_timeout(), timeout)
                if self.callback:
                    await self.callback(t)
            except asyncio.TimeoutError:
                return None
        else:
            return self._poll_no_timeout()

    async def _poll_no_timeout(self):
        while not (msg := await self._single_poll()):
            pass
        return msg

    async def _single_poll(self):
        if (msg := self.consumer.poll(timeout=0)) is not None:
            return msg

        awaitable = self.loop.create_future()
        self.waiters.add(awaitable)
        try:
            # timeout=2 is there for two reasons:
            # 1) self.consumer.poll needs to be called reguraly for other
            #    activities like: log callbacks.
            # 2) Ensures progress even if something with eventfd
            #    notification goes wrong.
            await asyncio.wait_for(awaitable, timeout=2)
        except asyncio.TimeoutError:
            return None
        finally:
            self.waiters.discard(awaitable)

    def assignment(self) -> Set[TP]:
        return self.consumer.assignment()



class ConfluentConsumerThread(ConsumerThread):
    """Thread managing underlying :pypi:`confluent_kafka` consumer."""

    _consumer: Optional[AsyncConsumer] = None
    _assigned: bool = False

    # _pending_rebalancing_spans: Deque[opentracing.Span]

    tp_last_committed_at: MutableMapping[TP, float]
    time_started: float

    tp_fetch_request_timeout_secs: float
    tp_fetch_response_timeout_secs: float
    tp_stream_timeout_secs: float
    tp_commit_timeout_secs: float

    async def on_start(self) -> None:
        self._consumer = self._create_consumer(loop=self.thread_loop)
        self.time_started = monotonic()
        # await self._consumer.start()

    def _create_consumer(self, loop: asyncio.AbstractEventLoop) -> AsyncConsumer:
        transport = cast(Transport, self.transport)
        if self.app.client_only:
            return self._create_client_consumer(transport, loop=loop)
        else:
            return self._create_worker_consumer(transport, loop=loop)

    def _create_worker_consumer(
        self, transport: "Transport", loop: asyncio.AbstractEventLoop
    ) -> AsyncConsumer:
        conf = self.app.conf
        self._assignor = self.app.assignor

        # XXX parition.assignment.strategy is string
        # need to write C wrapper for this
        # 'partition.assignment.strategy': [self._assignor]
        return AsyncConsumer(
            {
                "bootstrap.servers": server_list(transport.url, transport.default_port),
                "group.id": conf.id,
                "client.id": conf.broker_client_id,
                "default.topic.config": {
                    "auto.offset.reset": "earliest",
                },
                "enable.auto.commit": False,
                "fetch.max.bytes": conf.consumer_max_fetch_size,
                "request.timeout.ms": int(conf.broker_request_timeout * 1000.0),
                "check.crcs": conf.broker_check_crcs,
                "session.timeout.ms": int(conf.broker_session_timeout * 1000.0),
                "heartbeat.interval.ms": int(conf.broker_heartbeat_interval * 1000.0),
            },
            self.logger,
        )

    def _create_client_consumer(
        self, transport: "Transport", loop: asyncio.AbstractEventLoop
    ) -> AsyncConsumer:
        conf = self.app.conf
        return AsyncConsumer(
            {
                "bootstrap.servers": server_list(transport.url, transport.default_port),
                "client.id": conf.broker_client_id,
                "enable.auto.commit": True,
                "default.topic.config": {
                    "auto.offset.reset": "earliest",
                },
            },
            self.logger,
        )

    def close(self) -> None:
        ...

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        await self.cast_thread(
            self._ensure_consumer().subscribe,
            topics=list(topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

        while not self._assigned:
            self.log.info("Still waiting for assignment...")
            self._ensure_consumer().poll(timeout=1)

    def _on_assign(self, consumer: _Consumer, assigned: List[_TopicPartition]) -> None:
        self._assigned = True
        self.thread_loop.create_task(
            self.on_partitions_assigned({TP(tp.topic, tp.partition) for tp in assigned})
        )

    def _on_revoke(self, consumer: _Consumer, revoked: List[_TopicPartition]) -> None:
        self.thread_loop.create_task(
            self.on_partitions_revoked({TP(tp.topic, tp.partition) for tp in revoked})
        )

    async def seek_to_committed(self) -> Mapping[TP, int]:
        return await self.call_thread(self._seek_to_committed)

    async def _seek_to_committed(self) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        assignment = consumer.assignment()
        committed = consumer.committed(assignment)
        for tp in committed:
            consumer.seek(tp)
        return {ensure_TP(tp): tp.offset for tp in committed}

    async def _committed_offsets(self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        committed = consumer.committed(
            [_TopicPartition(tp[0], tp[1]) for tp in partitions]
        )
        return {TP(tp.topic, tp.partition): tp.offset for tp in committed}

    async def commit(self, tps: Mapping[TP, int]) -> bool:
        await self.call_thread(
            self._ensure_consumer().commit,
            offsets=[
                _TopicPartition(tp.topic, tp.partition, offset=offset)
                for tp, offset in tps.items()
            ],
            asynchronous=False,
        )
        return True

    async def position(self, tp: TP) -> Optional[int]:
        return await self.call_thread(self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        await self.call_thread(self._ensure_consumer().seek_to_beginning, *partitions)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        consumer = self._ensure_consumer()
        await self.call_thread(self._seek_wait, consumer, partitions)

    async def _seek_wait(
        self, consumer: Consumer, partitions: Mapping[TP, int]
    ) -> None:
        for tp, offset in partitions.items():
            self.log.dev("SEEK %r -> %r", tp, offset)
            await consumer.seek(tp, offset)
        await asyncio.gather(*[consumer.position(tp) for tp in partitions])

    def seek(self, partition: TP, offset: int) -> None:
        self._ensure_consumer().seek(partition, offset)

    def assignment(self) -> Set[TP]:
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        _, hw = self._ensure_consumer().get_watermark_offsets(
            _TopicPartition(tp.topic, tp.partition), cached=True
        )
        return hw

    def topic_partitions(self, topic: str) -> Optional[int]:
        # XXX NotImplemented
        return None

    async def earliest_offsets(self, *partitions: TP) -> MutableMapping[TP, int]:
        if not partitions:
            return {}
        return await self.call_thread(self._earliest_offsets, partitions)

    async def _earliest_offsets(self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        return {
            tp: consumer.get_watermark_offsets(_TopicPartition(tp[0], tp[1]))[0]
            for tp in partitions
        }

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        if not partitions:
            return {}
        return await self.call_thread(self._highwaters, partitions)

    async def _highwaters(self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        return {
            tp: consumer.get_watermark_offsets(_TopicPartition(tp[0], tp[1]))[1]
            for tp in partitions
        }

    def _ensure_consumer(self) -> AsyncConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted("Consumer thread not yet started")
        return self._consumer

    async def getmany(
        self, active_partitions: Optional[Set[TP]], timeout: float
    ) -> RecordMap:
        # Implementation for the Fetcher service.
        _consumer = self._ensure_consumer()
        messages = await self.call_thread(
            _consumer.consume,
            num_messages=10000,
            timeout=timeout,
        )
        records: RecordMap = defaultdict(list)
        for message in messages:
            tp = TP(message.topic(), message.partition())
            records[tp].append(message)
        return records

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Mapping[str, Any] = None,
        timeout: Seconds = 30.0,
        retention: Seconds = None,
        compacting: bool = None,
        deleting: bool = None,
        ensure_created: bool = False,
    ) -> None:
        return  # XXX

    def key_partition(
        self, topic: str, key: Optional[bytes], partition: int = None
    ) -> Optional[int]:
        metadata = self._consumer.list_topics(topic)
        partition_count = len(metadata.topics[topic]["partitions"])

        # Calculate the partition number based on the key hash
        key_bytes = str(key).encode("utf-8")
        return abs(hash(key_bytes)) % partition_count


class ProducerProduceFuture(asyncio.Future):
    def set_from_on_delivery(self, err: Optional[BaseException], msg: _Message) -> None:
        if err:
            # XXX Not sure what err' is here, hopefully it's an exception
            # object and not a string [ask].
            self.set_exception(err)
        else:
            metadata: RecordMetadata = self.message_to_metadata(msg)
            self.set_result(metadata)

    def message_to_metadata(self, message: _Message) -> RecordMetadata:
        topic, partition = tp = TP(message.topic(), message.partition())
        return RecordMetadata(topic, partition, tp, message.offset())


class ProducerThread(QueueServiceThread):
    """Thread managing underlying :pypi:`confluent_kafka` producer."""

    app: AppT
    producer: "Producer"
    transport: "Transport"
    _producer: Optional[_Producer] = None
    _flush_soon: Optional[asyncio.Future] = None

    def __init__(self, producer: "Producer", **kwargs: Any) -> None:
        self.producer = producer
        self.transport = cast(Transport, self.producer.transport)
        self.app = self.transport.app
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self._producer = confluent_kafka.Producer(
            {
                "bootstrap.servers": server_list(
                    self.transport.url, self.transport.default_port
                ),
                "client.id": self.app.conf.broker_client_id,
                "max.in.flight.requests.per.connection": 1,
            }
        )

    async def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    async def on_thread_stop(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        partition: int,
        on_delivery: Callable,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("Producer not started")
        if partition is not None:
            self._producer.produce(
                topic,
                key,
                value,
                partition,
                on_delivery=on_delivery,
            )
        else:
            self._producer.produce(
                topic,
                key,
                value,
                on_delivery=on_delivery,
            )
        notify(self._flush_soon)

    @Service.task
    async def _background_flush(self) -> None:
        producer = cast(_Producer, self._producer)
        _size = producer.__len__
        _flush = producer.flush
        _poll = producer.poll
        _sleep = self.sleep
        _create_future = self.loop.create_future
        while not self.should_stop:
            if not _size():
                flush_soon = self._flush_soon
                if flush_soon is None:
                    flush_soon = self._flush_soon = _create_future()
                stopped: bool = False
                try:
                    stopped = await self.wait_for_stopped(flush_soon, timeout=1.0)
                finally:
                    self._flush_soon = None
                if not stopped:
                    _flush(timeout=100)
                    _poll(timeout=1)
                    await _sleep(0)


class Producer(base.Producer):
    """Kafka producer using :pypi:`confluent_kafka`."""

    logger = logger

    _producer_thread: ProducerThread
    _admin: AdminClient
    _quick_produce: Any = None

    def __post_init__(self) -> None:
        self._producer_thread = ProducerThread(self, loop=self.loop, beacon=self.beacon)
        self._quick_produce = self._producer_thread.produce

    async def _on_irrecoverable_error(self, exc: BaseException) -> None:
        consumer = self.transport.app.consumer
        if consumer is not None:
            await consumer.crash(exc)
        await self.crash(exc)

    async def on_restart(self) -> None:
        """Call when producer is restarting."""
        self.on_init()

    async def create_topic(
        self,
        topic: str,
        partitions: int,
        replication: int,
        *,
        config: Mapping[str, Any] = None,
        timeout: Seconds = 20.0,
        retention: Seconds = None,
        compacting: bool = None,
        deleting: bool = None,
        ensure_created: bool = False,
    ) -> None:
        """Create topic on broker."""
        return  # XXX
        _retention = int(want_seconds(retention) * 1000.0) if retention else None
        await cast(Transport, self.transport)._create_topic(
            self,
            self._producer.client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    async def on_start(self) -> None:
        """Call when producer is starting."""
        await self._producer_thread.start()
        await self.sleep(0.5)  # cannot remember why, necessary? [ask]

    async def on_stop(self) -> None:
        """Call when producer is stopping."""
        await self._producer_thread.stop()

    async def send(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: str = None,
    ) -> Awaitable[RecordMetadata]:
        """Send message for future delivery."""
        fut = ProducerProduceFuture(loop=self.loop)
        self._quick_produce(
            topic,
            value,
            key,
            partition,
            on_delivery=fut.set_from_on_delivery,
        )
        return cast(Awaitable[RecordMetadata], fut)
        try:
            return cast(
                Awaitable[RecordMetadata],
                await self._producer.send(topic, value, key=key, partition=partition),
            )
        except KafkaException as exc:
            raise ProducerSendError(f"Error while sending: {exc!r}") from exc

    async def send_and_wait(
        self,
        topic: str,
        key: Optional[bytes],
        value: Optional[bytes],
        partition: Optional[int],
        timestamp: Optional[float],
        headers: Optional[HeadersArg],
        *,
        transactional_id: str = None,
    ) -> RecordMetadata:
        """Send message and wait for it to be delivered to broker(s)."""
        fut = await self.send(
            topic,
            key,
            value,
            partition,
            timestamp,
            headers,
        )
        return await fut

    async def flush(self) -> None:
        """Flush producer buffer.

        This will wait until the producer has written
        all buffered up messages to any connected brokers.
        """
        await self._producer_thread.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Return topic and partition destination for key."""
        # Get the partition count for the topic
        metadata = self._producer_thread.producer.list_topics(topic)
        partition_count = len(metadata.topics[topic].partitions)

        # Calculate the partition number based on the key hash
        key_bytes = str(key).encode("utf-8")
        partition = abs(hash(key_bytes)) % partition_count

        return TP(topic, partition)


class Transport(base.Transport):
    """Kafka transport using :pypi:`confluent_kafka`."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    driver_version = f"confluent_kafka={confluent_kafka.__version__}"

    def _topic_config(
        self, retention: int = None, compacting: bool = None, deleting: bool = None
    ) -> MutableMapping[str, Any]:
        config: MutableMapping[str, Any] = {}
        cleanup_flags: Set[str] = set()
        if compacting:
            cleanup_flags |= {"compact"}
        if deleting:
            cleanup_flags |= {"delete"}
        if cleanup_flags:
            config["cleanup.policy"] = ",".join(sorted(cleanup_flags))
        if retention:
            config["retention.ms"] = retention
        return config
