"""Smoke tests proving faust can talk to a real Kafka broker.

The first test uses :pypi:`aiokafka` directly and is the robust anchor that
proves the CI Kafka service is up.  The second drives a real faust ``App``
end to end (produce -> agent consumes -> ack/commit), which is what we
actually want to be able to exercise for broker-dependent bugs.
"""

import asyncio

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import faust

# The suite escalates ResourceWarning to an error (see pyproject.toml), but
# aiokafka / faust legitimately deal with sockets and background tasks that
# can emit ResourceWarning during teardown of a live-broker test.  Relax that
# here so a leaked-socket warning doesn't mask the actual assertion.
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
]

# Generous timeouts: a cold broker + first rebalance can take a while in CI.
ASSIGN_TIMEOUT = 60.0
RECV_TIMEOUT = 60.0


async def test_aiokafka_roundtrip(kafka_bootstrap, unique_topic):
    """Produce and consume one message straight through aiokafka."""
    producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await producer.start()
    try:
        await producer.send_and_wait(unique_topic, b"ping")
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        unique_topic,
        bootstrap_servers=kafka_bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=RECV_TIMEOUT)
    finally:
        await consumer.stop()
    assert msg.value == b"ping"


async def _wait_for_assignment(app: faust.App, timeout: float) -> None:
    async def _poll() -> None:
        while not app.consumer.assignment():
            await asyncio.sleep(0.5)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def test_faust_app_roundtrip(broker_url, unique_topic, unique_group):
    """Drive a real faust App: send a value and have an agent receive it."""
    app = faust.App(
        unique_group,
        broker=broker_url,
        store="memory://",
        topic_partitions=1,
        topic_replication_factor=1,
        web_enabled=False,
        consumer_auto_offset_reset="earliest",
    )
    topic = app.topic(unique_topic, value_type=bytes, value_serializer="raw")

    received: "asyncio.Queue[bytes]" = asyncio.Queue()

    @app.agent(topic)
    async def process(stream):
        async for value in stream:
            await received.put(value)
            yield value

    app.finalize()
    await app.start()
    try:
        # Wait until the agent's consumer owns the partition, otherwise the
        # send could race ahead of the subscription.
        await _wait_for_assignment(app, timeout=ASSIGN_TIMEOUT)
        await topic.send(value=b"ping")
        value = await asyncio.wait_for(received.get(), timeout=RECV_TIMEOUT)
    finally:
        await app.stop()

    assert value == b"ping"
