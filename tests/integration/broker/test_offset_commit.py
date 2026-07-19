"""Live-broker regression tests for offset-commit correctness (issue #606).

Issue #606: :meth:`Consumer._new_offset` could return an offset *past* a
message that was still in-flight (tracked but not yet acked), so a subsequent
commit would persist a committed offset beyond an unprocessed message.  If the
worker then crashed, that message was silently skipped on restart -- data loss.

These tests drive faust's real aiokafka consumer against a live broker: they
start an :class:`~faust.App`, let it get a real partition assignment, seed the
consumer's ack bookkeeping to reproduce the in-flight scenario, run the actual
``commit()`` path, and read the committed offset back from the broker with an
admin client.  They assert faust never persists an offset past the smallest
in-flight message.  Without the #606 fix the first test fails (the broker ends
up with the too-far offset).

The tests skip automatically when no Kafka broker is reachable (see the
``broker_url`` fixture in ``conftest.py``); the dedicated CI integration job
runs them against a real broker.
"""

import asyncio

import pytest
from aiokafka import AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

import faust
from faust.types import TP
from faust.types.tuples import Message

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
]

# Generous enough for a cold broker + group join in CI, short enough that a
# genuine hang fails fast rather than sitting at the job timeout.
ASSIGN_TIMEOUT = 30.0
OP_TIMEOUT = 30.0
N_MESSAGES = 5


def _message(topic: str, tp: TP, offset: int) -> Message:
    """Build a minimal Message for a given topic-partition/offset."""
    return Message(
        topic=topic,
        partition=tp.partition,
        offset=offset,
        timestamp=0.0,
        timestamp_type=0,
        headers=[],
        key=None,
        value=b"x",
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=1,
        tp=tp,
    )


async def _produce(bootstrap: str, topic: str, n: int) -> None:
    """Produce ``n`` messages, which also auto-creates the topic."""
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await producer.start()
    try:
        for i in range(n):
            await producer.send_and_wait(topic, b"m%d" % i)
    finally:
        await producer.stop()


async def _broker_committed(bootstrap: str, group: str, tp: TP):
    """Read a group's committed offset for ``tp`` straight from the broker.

    Uses the admin API so it does not join the consumer group (the faust app
    is still a live member while this runs).  Returns ``None`` when nothing is
    committed.
    """
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        offsets = await admin.list_consumer_group_offsets(group)
        meta = offsets.get(TopicPartition(tp.topic, tp.partition))
        if meta is None or meta.offset < 0:
            return None
        return meta.offset
    finally:
        await admin.close()


async def _start_assigned_app(broker_url: str, topic: str, group: str):
    """Start an app whose consumer is assigned ``topic``-0, and return it.

    The trivial agent gives the app a topology to subscribe to (and thus a real
    partition assignment) without acking anything itself once the backlog is
    drained.  Auto-commit is effectively disabled via a huge commit interval so
    the only commit is the one the test triggers.
    """
    app = faust.App(
        group,
        broker=broker_url,
        store="memory://",
        value_serializer="raw",
        topic_allow_declare=True,
        topic_disable_leader=True,
        web_enabled=False,
        broker_commit_interval=9999.0,
    )
    source = app.topic(topic, partitions=1)

    @app.agent(source)
    async def _process(stream):
        async for _ in stream:  # pragma: no cover - drains the backlog
            pass

    await asyncio.wait_for(app.start(), timeout=OP_TIMEOUT)
    tp = TP(topic, 0)
    deadline_loops = int(ASSIGN_TIMEOUT / 0.5)
    for _ in range(deadline_loops):
        if tp in app.consumer.assignment():
            break
        await asyncio.sleep(0.5)
    assert tp in app.consumer.assignment(), "consumer never got a partition assignment"
    # Let the agent drain the produced backlog so it does not mutate the
    # ack bookkeeping while the test seeds its own scenario.
    await asyncio.sleep(3)
    return app


async def _stop_app(app) -> None:
    """Stop the app, clearing in-flight tracking first.

    ``wait_empty()`` blocks shutdown until ``_unacked_messages`` drains; a test
    that deliberately leaves a message in-flight must clear it or the app would
    hang on stop.
    """
    consumer = app.consumer
    consumer._unacked_messages = type(consumer._unacked_messages)()
    await asyncio.wait_for(app.stop(), timeout=OP_TIMEOUT)


async def test_does_not_commit_past_inflight_message(
    broker_url, kafka_bootstrap, unique_topic, unique_group
):
    """#606: a commit must not advance past an in-flight (unacked) message.

    Scenario: offset 2 is still being processed (tracked, unacked) while later
    offsets 3 and 4 have been acked out of order, and the last committed offset
    is 2.  The bug computed a new offset of 5 and committed it, skipping 2.
    The fix withholds the commit, so the broker's committed offset must stay at
    or below 2.
    """
    await _produce(kafka_bootstrap, unique_topic, N_MESSAGES)
    app = await _start_assigned_app(broker_url, unique_topic, unique_group)
    tp = TP(unique_topic, 0)
    try:
        consumer = app.consumer
        # last committed offset is 2 (offsets 0..1 already done).
        consumer._committed_offset[tp] = 2
        # offsets 3 and 4 acked out of order.
        consumer._acked[tp] = [3, 4]
        consumer._acked_index[tp] = {3, 4}
        # offset 2 is still in-flight (tracked, not acked).  A strong reference
        # keeps it alive in the WeakSet of unacked messages.
        inflight = _message(unique_topic, tp, 2)
        consumer._unacked_messages = type(consumer._unacked_messages)()
        consumer._unacked_messages.add(inflight)

        # The fix must refuse to advance past the in-flight offset.
        assert consumer._new_offset(tp) is None

        # Re-seed (the _new_offset call above mutates the acked list) and run
        # the real commit path against the broker.
        consumer._committed_offset[tp] = 2
        consumer._acked[tp] = [3, 4]
        consumer._acked_index[tp] = {3, 4}
        inflight = _message(unique_topic, tp, 2)
        consumer._unacked_messages = type(consumer._unacked_messages)()
        consumer._unacked_messages.add(inflight)
        await asyncio.wait_for(consumer.commit({tp}), timeout=OP_TIMEOUT)

        committed = await asyncio.wait_for(
            _broker_committed(kafka_bootstrap, unique_group, tp), timeout=OP_TIMEOUT
        )
        assert committed is None or committed <= 2, (
            f"committed offset {committed} advanced past the in-flight message "
            "at offset 2 (data loss)"
        )
    finally:
        await _stop_app(app)


async def test_commits_contiguous_acks(
    broker_url, kafka_bootstrap, unique_topic, unique_group
):
    """Positive control: with no in-flight message, the commit does advance.

    All of offsets 0..4 are acked contiguously and nothing is in-flight, so the
    real commit path must persist offset 5 (next-to-fetch) to the broker.  This
    proves the negative test's "did not commit" result is meaningful and not
    just a broken commit path.
    """
    await _produce(kafka_bootstrap, unique_topic, N_MESSAGES)
    app = await _start_assigned_app(broker_url, unique_topic, unique_group)
    tp = TP(unique_topic, 0)
    try:
        consumer = app.consumer
        consumer._committed_offset[tp] = 0
        consumer._acked[tp] = [0, 1, 2, 3, 4]
        consumer._acked_index[tp] = {0, 1, 2, 3, 4}
        consumer._unacked_messages = type(consumer._unacked_messages)()

        assert consumer._new_offset(tp) == 5

        consumer._committed_offset[tp] = 0
        consumer._acked[tp] = [0, 1, 2, 3, 4]
        consumer._acked_index[tp] = {0, 1, 2, 3, 4}
        did_commit = await asyncio.wait_for(consumer.commit({tp}), timeout=OP_TIMEOUT)
        assert did_commit

        committed = await asyncio.wait_for(
            _broker_committed(kafka_bootstrap, unique_group, tp), timeout=OP_TIMEOUT
        )
        assert committed == 5
    finally:
        await _stop_app(app)
