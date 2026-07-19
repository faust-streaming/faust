"""Smoke tests proving the CI Kafka broker is up and reachable.

These are the foundation for broker-dependent regression tests: a raw
:pypi:`aiokafka` round-trip and a raw :pypi:`confluent-kafka` round-trip,
one per transport-client library faust ships drivers for.  They stay green
on developer machines and the broker-less CI jobs (the ``broker_url`` fixture
skips when no Kafka is reachable) and run for real in the dedicated
integration job.

A full faust ``App`` end-to-end test (start an app, drive an agent, shut it
down) is intentionally left to a follow-up: embedding the app lifecycle in
pytest needs its own hardening and shouldn't gate this foundation.

``confluent-kafka`` is an optional dependency; its test skips when it is not
installed.
"""

import asyncio

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# The suite escalates ResourceWarning to an error (see pyproject.toml), but
# aiokafka / confluent-kafka legitimately deal with sockets that can emit
# ResourceWarning during teardown of a live-broker test.  Relax that here so a
# leaked-socket warning doesn't mask the actual assertion.
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
]

# Enough for a cold broker + first fetch in CI, short enough that a genuine
# hang fails fast instead of sitting at the timeout.
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
