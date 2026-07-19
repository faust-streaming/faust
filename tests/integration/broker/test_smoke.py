"""Smoke tests proving faust can talk to a real Kafka broker.

Coverage is split so a failure points at the layer that broke:

* raw :pypi:`aiokafka` and :pypi:`confluent-kafka` round-trips -- the robust
  anchors that prove the CI broker is up and each client library works;
* a real faust ``App`` round-trip (produce -> agent consumes -> ack), run
  against *both* transport drivers (``kafka://`` = aiokafka and
  ``confluent://`` = confluent-kafka), which is what we actually want to
  exercise for broker-dependent bugs.

``confluent-kafka`` is an optional dependency; its tests skip when it is not
installed.
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

# Enough for a cold broker + first rebalance in CI, but short enough that a
# genuine hang fails fast instead of sitting at the timeout.
ASSIGN_TIMEOUT = 30.0
RECV_TIMEOUT = 20.0


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


async def test_confluent_roundtrip(kafka_bootstrap, unique_topic, unique_group):
    """Produce and consume one message straight through confluent-kafka."""
    pytest.importorskip("confluent_kafka")
    from confluent_kafka import Consumer, Producer

    producer = Producer({"bootstrap.servers": kafka_bootstrap})
    producer.produce(unique_topic, b"ping")
    producer.flush(RECV_TIMEOUT)

    consumer = Consumer(
        {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": unique_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([unique_topic])
    loop = asyncio.get_event_loop()
    deadline = loop.time() + RECV_TIMEOUT
    value = None
    try:
        while loop.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = msg.value()
            break
    finally:
        consumer.close()
    assert value == b"ping"


async def _wait_for_assignment(app: faust.App, timeout: float) -> None:
    async def _poll() -> None:
        while not app.consumer.assignment():
            await asyncio.sleep(0.5)

    await asyncio.wait_for(_poll(), timeout=timeout)


@pytest.mark.parametrize("scheme", ["kafka", "confluent"])
async def test_faust_app_roundtrip(kafka_bootstrap, unique_topic, unique_group, scheme):
    """Drive a real faust App on each transport: send a value, agent receives.

    ``kafka://`` selects the aiokafka driver, ``confluent://`` the
    confluent-kafka driver.
    """
    if scheme == "confluent":
        pytest.importorskip("confluent_kafka")
    app = faust.App(
        unique_group,
        broker=f"{scheme}://{kafka_bootstrap}",
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
        # Flow control starts suspended; a normal `faust worker` resumes it on
        # partition assignment.  When embedding the app ourselves we have to
        # resume it explicitly or the agent's stream never pulls messages.
        app.flow_control.resume()
        await topic.send(value=b"ping")
        value = await asyncio.wait_for(received.get(), timeout=RECV_TIMEOUT)
    finally:
        await app.stop()

    assert value == b"ping"
