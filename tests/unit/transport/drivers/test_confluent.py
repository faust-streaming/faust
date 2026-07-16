"""Unit tests for the :pypi:`confluent_kafka` transport driver.

These tests exercise the driver's real surface -- the thin async wrappers
around :pypi:`confluent_kafka` -- with the underlying ``confluent_kafka``
``Consumer``/``Producer`` mocked out, so no broker is required.
"""

from unittest.mock import Mock, patch

import pytest
from confluent_kafka import TopicPartition
from mode.utils.futures import done_future

from faust.transport.drivers import confluent as mod
from faust.transport.drivers.confluent import (
    AsyncConsumer,
    ConfluentConsumerThread,
    Consumer,
    ConsumerNotStarted,
    Producer,
    ProducerProduceFuture,
    ProducerThread,
    Transport,
    server_list,
)
from faust.types import TP
from faust.types.tuples import RecordMetadata
from tests.helpers import AsyncMock

TP1 = TP("topic", 0)
TP2 = TP("topic", 3)
TP3 = TP("topix", 1)

TESTED_MODULE = "faust.transport.drivers.confluent"


async def _passthrough_call_thread(fn, *args, **kwargs):
    """Stand-in for ConsumerThread.call_thread that runs inline.

    The real implementation dispatches ``fn`` onto the consumer's dedicated
    thread; for unit tests we just await/run it in place.
    """
    result = fn(*args, **kwargs)
    if hasattr(result, "__await__"):
        return await result
    return result


@pytest.fixture()
def callback():
    return AsyncMock(name="callback")


@pytest.fixture()
def on_partitions_revoked():
    return AsyncMock(name="on_partitions_revoked")


@pytest.fixture()
def on_partitions_assigned():
    return AsyncMock(name="on_partitions_assigned")


@pytest.fixture()
def consumer(app, callback, on_partitions_revoked, on_partitions_assigned):
    consumer = Consumer(
        app.transport,
        callback=callback,
        on_partitions_revoked=on_partitions_revoked,
        on_partitions_assigned=on_partitions_assigned,
    )
    consumer._thread = Mock(
        name="thread",
        create_topic=AsyncMock(),
        verify_event_path=Mock(),
    )
    return consumer


@pytest.fixture()
def cthread(consumer):
    return ConfluentConsumerThread(consumer)


@pytest.fixture()
def underlying():
    """A mock of the low-level confluent_kafka.Consumer."""
    return Mock(name="confluent_kafka.Consumer")


@pytest.fixture()
def _consumer(underlying):
    """A mock AsyncConsumer as held by ConfluentConsumerThread._consumer."""
    _consumer = Mock(name="AsyncConsumer")
    _consumer.consumer = underlying
    return _consumer


class Test_server_list:
    def test_single(self):
        from yarl import URL

        assert server_list([URL("kafka://localhost:9092")], 9092) == "localhost:9092"

    def test_multiple_and_default_port(self):
        from yarl import URL

        urls = [URL("kafka://h1:9092"), URL("kafka://h2")]
        assert server_list(urls, 9092) == "h1:9092,h2:9092"


class TestConsumer:
    def test__new_consumer_thread(self, *, consumer):
        thread = Consumer._new_consumer_thread(consumer)
        assert isinstance(thread, ConfluentConsumerThread)

    @pytest.mark.asyncio
    async def test_create_topic__allowed(self, *, consumer):
        consumer.app.conf.topic_allow_declare = True
        await consumer.create_topic(
            "topic", 3, 1, retention=3000.0, compacting=True, deleting=False
        )
        consumer._thread.create_topic.assert_called_once_with(
            "topic",
            3,
            1,
            config=None,
            timeout=30000,
            retention=3000000,
            compacting=True,
            deleting=False,
            ensure_created=False,
        )

    @pytest.mark.asyncio
    async def test_create_topic__not_allowed(self, *, consumer):
        consumer.app.conf.topic_allow_declare = False
        with patch(TESTED_MODULE + ".logger") as logger:
            await consumer.create_topic("topic", 3, 1, retention=3000.0)
        consumer._thread.create_topic.assert_not_called()
        logger.warning.assert_called_once()

    def test__to_message(self, *, consumer):
        record = Mock(name="record")
        record.timestamp.return_value = (1, 1000)
        record.key.return_value = b"key"
        record.value.return_value = b"value"
        record.topic.return_value = "topic"
        record.partition.return_value = 0
        record.offset.return_value = 7

        msg = consumer._to_message(TP1, record)

        assert msg.topic == "topic"
        assert msg.partition == 0
        assert msg.offset == 7
        assert msg.key == b"key"
        assert msg.value == b"value"
        assert msg.timestamp == 1.0
        assert msg.timestamp_type == 1
        assert msg.tp == TP1

    def test__to_message__no_timestamp_no_key(self, *, consumer):
        record = Mock(name="record")
        record.timestamp.return_value = (0, None)
        record.key.return_value = None
        record.value.return_value = None
        record.topic.return_value = "topic"
        record.partition.return_value = 0
        record.offset.return_value = 0

        msg = consumer._to_message(TP1, record)

        assert msg.key is None
        assert msg.value is None

    def test__new_topicpartition(self, *, consumer):
        tp = consumer._new_topicpartition("topic", 3)
        assert tp.topic == "topic"
        assert tp.partition == 3

    def test_verify_event_path(self, *, consumer):
        consumer.verify_event_path(303.3, TP1)
        consumer._thread.verify_event_path.assert_called_once_with(303.3, TP1)


class TestAsyncConsumer:
    @pytest.fixture()
    def async_consumer(self, callback):
        with patch.object(mod.confluent_kafka, "Consumer") as MockConsumer:
            ac = AsyncConsumer({"group.id": "g"}, callback=callback)
            assert ac.consumer is MockConsumer.return_value
            yield ac

    def test_construct_does_not_call_io_event_enable(self):
        with patch.object(mod.confluent_kafka, "Consumer") as MockConsumer:
            AsyncConsumer({"group.id": "g"})
        # io_event_enable was the abandoned-fork-only API; it must not be used.
        assert not MockConsumer.return_value.io_event_enable.called

    def test_close(self, *, async_consumer):
        async_consumer.close()
        async_consumer.consumer.close.assert_called_once_with()

    def test_subscribe(self, *, async_consumer):
        async_consumer.subscribe(["topic"], on_assign=1)
        async_consumer.consumer.subscribe.assert_called_once_with(
            ["topic"], on_assign=1
        )

    def test_assign(self, *, async_consumer):
        async_consumer.assign([TP1])
        async_consumer.consumer.assign.assert_called_once_with([TP1])

    def test_assignment(self, *, async_consumer):
        async_consumer.consumer.assignment.return_value = {TP1}
        assert async_consumer.assignment() == {TP1}

    @pytest.mark.asyncio
    async def test_poll__message(self, *, async_consumer, callback):
        message = Mock(name="message")
        async_consumer.consumer.poll.return_value = message
        result = await async_consumer.poll(timeout=5.0)
        assert result is message
        async_consumer.consumer.poll.assert_called_once_with(timeout=5.0)
        callback.assert_called_once_with(message)

    @pytest.mark.asyncio
    async def test_poll__no_message(self, *, async_consumer, callback):
        async_consumer.consumer.poll.return_value = None
        result = await async_consumer.poll(timeout=5.0)
        assert result is None
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll__coerces_nonpositive_timeout(self, *, async_consumer):
        async_consumer.consumer.poll.return_value = None
        await async_consumer.poll(timeout=0)
        async_consumer.consumer.poll.assert_called_once_with(timeout=1.0)


class TestConfluentConsumerThread:
    def test__ensure_consumer__not_started(self, *, cthread):
        cthread._consumer = None
        with pytest.raises(ConsumerNotStarted):
            cthread._ensure_consumer()

    def test__ensure_consumer(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        assert cthread._ensure_consumer() is _consumer

    def test__create_worker_consumer(self, *, cthread):
        transport = cthread.transport
        with patch(TESTED_MODULE + ".AsyncConsumer") as AC:
            result = cthread._create_worker_consumer(transport, loop=None)
        assert result is AC.return_value
        config = AC.call_args[0][0]
        assert config["group.id"] == cthread.app.conf.id
        assert config["enable.auto.commit"] is False
        assert "bootstrap.servers" in config
        assert cthread._assignor is cthread.app.assignor

    def test__create_client_consumer(self, *, cthread):
        transport = cthread.transport
        with patch(TESTED_MODULE + ".AsyncConsumer") as AC:
            result = cthread._create_client_consumer(transport, loop=None)
        assert result is AC.return_value
        config = AC.call_args[0][0]
        assert config["enable.auto.commit"] is True
        assert "bootstrap.servers" in config

    @pytest.mark.asyncio
    async def test_getmany(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        m1, m2, m3 = (self._message(TP1), self._message(TP1), self._message(TP3))
        cthread.call_thread = AsyncMock(return_value=[m1, m2, m3])

        records = await cthread.getmany({TP1}, timeout=1.0)

        cthread.call_thread.assert_called_once_with(
            _consumer.consumer.consume, num_messages=10000, timeout=1.0
        )
        assert records[TP1] == [m1, m2]
        assert records[TP3] == [m3]

    @staticmethod
    def _message(tp):
        message = Mock(name="message")
        message.topic.return_value = tp.topic
        message.partition.return_value = tp.partition
        return message

    @pytest.mark.asyncio
    async def test_commit(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.call_thread = AsyncMock()
        assert await cthread.commit({TP1: 100}) is True
        cthread.call_thread.assert_called_once()

    @pytest.mark.asyncio
    async def test_position(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.call_thread = AsyncMock(return_value=42)
        assert await cthread.position(TP1) == 42
        cthread.call_thread.assert_called_once_with(_consumer.consumer.position, TP1)

    def test_assignment(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        _consumer.assignment.return_value = {TopicPartition(TP1.topic, TP1.partition)}
        assert cthread.assignment() == {TP1}

    def test_highwater(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        _consumer.consumer.get_watermark_offsets.return_value = (5, 13)
        assert cthread.highwater(TP1) == 13

    def test_seek(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.seek(TP1, 300)
        _consumer.consumer.seek.assert_called_once_with(TP1, 300)

    @pytest.mark.asyncio
    async def test_seek_to_beginning(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.call_thread = AsyncMock()
        part = TopicPartition(TP1.topic, TP1.partition)
        await cthread.seek_to_beginning(part)
        cthread.call_thread.assert_called_once_with(
            _consumer.consumer.seek_to_beginning, part
        )

    @pytest.mark.asyncio
    async def test_earliest_offsets(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.call_thread = _passthrough_call_thread
        _consumer.consumer.get_watermark_offsets.return_value = (2, 20)
        offsets = await cthread.earliest_offsets(TP1)
        assert offsets[TP1] == 2

    @pytest.mark.asyncio
    async def test_highwaters(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        cthread.call_thread = _passthrough_call_thread
        _consumer.consumer.get_watermark_offsets.return_value = (2, 20)
        offsets = await cthread.highwaters(TP1)
        assert offsets[TP1] == 20

    def test__on_assign(self, *, cthread):
        cthread.thread_loop = Mock(name="thread_loop")
        cthread.on_partitions_assigned = Mock(name="on_partitions_assigned")
        assigned = [TopicPartition(TP1.topic, TP1.partition)]
        cthread._on_assign(Mock(name="consumer"), assigned)
        assert cthread._assigned is True
        cthread.on_partitions_assigned.assert_called_once_with({TP1})
        cthread.thread_loop.create_task.assert_called_once()

    def test__on_revoke(self, *, cthread):
        cthread.thread_loop = Mock(name="thread_loop")
        cthread.on_partitions_revoked = Mock(name="on_partitions_revoked")
        revoked = [TopicPartition(TP1.topic, TP1.partition)]
        cthread._on_revoke(Mock(name="consumer"), revoked)
        cthread.on_partitions_revoked.assert_called_once_with({TP1})
        cthread.thread_loop.create_task.assert_called_once()

    def test_key_partition(self, *, cthread, _consumer):
        cthread._consumer = _consumer
        metadata = Mock(name="metadata")
        metadata.topics = {"topic": {"partitions": {0: 1, 1: 1, 2: 1}}}
        _consumer.consumer.list_topics.return_value = metadata
        partition = cthread.key_partition("topic", b"key")
        assert 0 <= partition < 3

    def test_topic_partitions(self, *, cthread):
        assert cthread.topic_partitions("topic") is None


class TestProducer:
    @pytest.fixture()
    def producer(self, app):
        producer = Producer(app.transport)
        producer._producer_thread = Mock(
            name="producer_thread",
            start=AsyncMock(),
            stop=AsyncMock(),
            flush=AsyncMock(),
        )
        producer.sleep = AsyncMock()
        return producer

    def test___post_init__(self, *, producer):
        assert producer._producer_thread is not None
        assert producer._quick_produce is not None

    @pytest.mark.asyncio
    async def test_on_start(self, *, producer):
        await producer.on_start()
        producer._producer_thread.start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_stop(self, *, producer):
        await producer.on_stop()
        producer._producer_thread.stop.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        producer._quick_produce = Mock(name="quick_produce")
        fut = await producer.send("topic", b"key", b"value", 0, None, None)
        assert isinstance(fut, ProducerProduceFuture)
        producer._quick_produce.assert_called_once()
        args, kwargs = producer._quick_produce.call_args
        assert args[0] == "topic"
        assert "on_delivery" in kwargs

    @pytest.mark.asyncio
    async def test_send_and_wait(self, *, producer, event_loop):
        expected = Mock(name="metadata")
        producer.send = AsyncMock(return_value=done_future(expected, loop=event_loop))
        result = await producer.send_and_wait("topic", b"k", b"v", 0, None, None)
        assert result is expected

    @pytest.mark.asyncio
    async def test_flush(self, *, producer):
        await producer.flush()
        producer._producer_thread.flush.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_create_topic__is_noop(self, *, producer):
        # create_topic short-circuits (XXX) -- must not raise.
        assert await producer.create_topic("topic", 3, 1) is None

    def test_key_partition(self, *, producer):
        metadata = Mock(name="metadata")
        topic_meta = Mock()
        topic_meta.partitions = {0: 1, 1: 1}
        metadata.topics = {"topic": topic_meta}
        producer._producer_thread.producer = Mock()
        producer._producer_thread.producer.list_topics.return_value = metadata
        tp = producer.key_partition("topic", b"key")
        assert tp.topic == "topic"
        assert 0 <= tp.partition < 2


class TestProducerThread:
    @pytest.fixture()
    def producer_thread(self, app):
        producer = Mock(name="producer")
        producer.transport = app.transport
        producer.loop = app.loop
        producer.beacon = Mock(name="beacon")
        return ProducerThread(producer, loop=app.loop, beacon=producer.beacon)

    @pytest.mark.asyncio
    async def test_on_start(self, *, producer_thread):
        with patch.object(mod.confluent_kafka, "Producer") as P:
            await producer_thread.on_start()
        assert producer_thread._producer is P.return_value

    @pytest.mark.asyncio
    async def test_flush(self, *, producer_thread):
        producer_thread._producer = Mock(name="_producer")
        await producer_thread.flush()
        producer_thread._producer.flush.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_flush__no_producer(self, *, producer_thread):
        producer_thread._producer = None
        await producer_thread.flush()  # must not raise

    @pytest.mark.asyncio
    async def test_on_thread_stop(self, *, producer_thread):
        producer_thread._producer = Mock(name="_producer")
        await producer_thread.on_thread_stop()
        producer_thread._producer.flush.assert_called_once_with()

    def test_produce__with_partition(self, *, producer_thread):
        producer_thread._producer = Mock(name="_producer")
        on_delivery = Mock()
        producer_thread.produce("topic", b"key", b"value", 0, on_delivery)
        producer_thread._producer.produce.assert_called_once_with(
            "topic", b"key", b"value", 0, on_delivery=on_delivery
        )

    def test_produce__without_partition(self, *, producer_thread):
        producer_thread._producer = Mock(name="_producer")
        on_delivery = Mock()
        producer_thread.produce("topic", b"key", b"value", None, on_delivery)
        producer_thread._producer.produce.assert_called_once_with(
            "topic", b"key", b"value", on_delivery=on_delivery
        )

    def test_produce__not_started(self, *, producer_thread):
        producer_thread._producer = None
        with pytest.raises(RuntimeError):
            producer_thread.produce("topic", b"key", b"value", 0, Mock())


class TestProducerProduceFuture:
    def test_set_from_on_delivery__success(self, event_loop):
        fut = ProducerProduceFuture(loop=event_loop)
        message = Mock(name="message")
        message.topic.return_value = "topic"
        message.partition.return_value = 3
        message.offset.return_value = 9
        fut.set_from_on_delivery(None, message)
        assert fut.done()
        metadata = fut.result()
        assert isinstance(metadata, RecordMetadata)
        assert metadata.topic == "topic"
        assert metadata.partition == 3
        assert metadata.offset == 9

    def test_set_from_on_delivery__error(self, event_loop):
        fut = ProducerProduceFuture(loop=event_loop)
        exc = KeyError("boom")
        fut.set_from_on_delivery(exc, None)
        assert fut.done()
        with pytest.raises(KeyError):
            fut.result()

    def test_message_to_metadata(self, event_loop):
        fut = ProducerProduceFuture(loop=event_loop)
        message = Mock(name="message")
        message.topic.return_value = "topic"
        message.partition.return_value = 1
        message.offset.return_value = 5
        metadata = fut.message_to_metadata(message)
        assert metadata.topic == "topic"
        assert metadata.partition == 1
        assert metadata.offset == 5


class TestTransport:
    @pytest.fixture()
    def transport(self, app):
        return Transport(url=["kafka://localhost:9092"], app=app)

    def test_attributes(self, *, transport):
        assert transport.Consumer is Consumer
        assert transport.Producer is Producer
        assert transport.default_port == 9092
        assert "confluent_kafka" in transport.driver_version

    def test__topic_config__empty(self, *, transport):
        assert transport._topic_config() == {}

    def test__topic_config__retention(self, *, transport):
        assert transport._topic_config(retention=3000)["retention.ms"] == 3000

    def test__topic_config__compacting_and_deleting(self, *, transport):
        config = transport._topic_config(compacting=True, deleting=True)
        assert config["cleanup.policy"] == "compact,delete"
