"""End-to-end parity between the two topic-conductor implementations.

``Conductor._build_handler`` returns one of two objects that are supposed to
behave identically:

* ``faust.transport._cython.conductor.ConductorHandler`` when the extension
  was built, and
* the ``on_message`` closure from ``ConductorCompiler.build`` otherwise.

Both take ``(conductor, tp, channels)`` and are awaited with a ``Message``, so
they can be driven over the same input and compared -- which is what every test
here does.  This is the per-message inner loop of a worker: fan-out to
subscribed channels, event reuse across channels with matching key/value types,
buffer-pressure callbacks, the full-queue path and decode-error propagation.

The existing conductor tests replace the handler with an ``AsyncMock`` and
assert it was called, so none of that logic was covered on either side.  The
duplication has already produced bugs that only differential testing catches --
see ``docs/developerguide/cython.rst``.

Note the converse, though: a differential test only finds *divergence*.  The
``on_topic_buffer_full`` defect (both implementations passed a channel where the
sensor wanted a ``TP``) kept these comparisons green the whole time it was
present, because both sides were wrong identically.  Shared mistakes need an
assertion about the behaviour itself, so a few tests below check what a value
*is* and not only that both implementations produce the same one.

Both implementations are driven against **the same** conductor and the same
``channels`` set, one after the other, rather than against two separately-built
environments.  ``channels`` is a set of ``Topic`` objects hashed by identity, so
two separately-built sets iterate in unrelated orders; anything order-sensitive
(which channel decodes first, which ones a mid-fan-out decode error reaches)
would then differ for reasons that have nothing to do with the implementations.
Sharing the set removes that variable, and ``reset()`` clears the queues and
recorded callbacks between runs.

Everything here is skipped when the extension is not built, since there is then
only one implementation and a "comparison" would run it against itself.
``FAUST_REQUIRE_CYTHON=1`` turns that skip into a failure.
"""

import asyncio
from typing import Any, Dict, List, Optional, Set

import pytest

from faust.exceptions import KeyDecodeError, ValueDecodeError
from faust.sensors import Monitor
from faust.transport.conductor import Conductor, ConductorHandler
from faust.types import TP, Message
from tests.helpers import AsyncMock

TP1 = TP("foo", 0)

#: The two implementations, in the order they are run.
IMPLS = ["cython", "python"]

requires_cython_conductor = pytest.mark.skipif(
    ConductorHandler is None,
    reason="conductor extension not built in place "
    "(USE_CYTHON=1 python setup.py build_ext --inplace)",
)


class Harness:
    """One conductor and its channels, drivable by either implementation."""

    def __init__(
        self, app: Any, n_channels: int = 1, heterogeneous: bool = False
    ) -> None:
        self.app = app

        # `app.producer` is a Mock in the unit fixture, so `buffer` is a Mock
        # attribute whose `wait_until_ebb()` returns something un-awaitable.
        app.producer.buffer.wait_until_ebb = AsyncMock()
        # Flow control gates the handler's first await; without this the
        # handler blocks forever rather than delivering.
        app.flow_control.resume()

        self.conductor = Conductor(app)
        # `heterogeneous` gives alternating channels a different `key_type`, so
        # their `(key_type, value_type)` pairs differ and the fan-out has to
        # deserialize per channel instead of reusing one event.  Both types
        # decode the same bytes payload, so only the reuse decision changes.
        self.channels: List[Any] = [
            app.topic(
                f"foo{i}",
                value_serializer="raw",
                key_type=bytes if (heterogeneous and i % 2) else None,
            )
            for i in range(n_channels)
        ]
        #: The single set both implementations iterate, so ordering matches.
        self.channel_set: Set[Any] = set(self.channels)

        # Sensors and consumer callbacks are recorded rather than asserted
        # individually, so a difference in *which* callbacks fire shows up as a
        # diff instead of being missed.
        self.buffer_full_sensor: List[Any] = []
        self.consumer_buffer_full: List[Any] = []
        self.consumer_buffer_drop: List[Any] = []
        self.key_decode_errors: List[Any] = []
        self.value_decode_errors: List[Any] = []
        self.decodes: List[str] = []

        app.sensors.on_topic_buffer_full = self.buffer_full_sensor.append
        app.consumer.on_buffer_full = self.consumer_buffer_full.append
        app.consumer.on_buffer_drop = self.consumer_buffer_drop.append

        for chan in self.channels:
            chan.on_key_decode_error = self._record(self.key_decode_errors, chan)
            chan.on_value_decode_error = self._record(self.value_decode_errors, chan)
            self._count_decodes(chan)

    def _record(self, into: List[Any], chan: Any):
        async def record(exc: BaseException, message: Message) -> None:
            into.append((chan.get_topic_name(), type(exc).__name__))

        return record

    def _count_decodes(self, chan: Any) -> None:
        """Make `decode` calls observable; event reuse is the whole point."""
        real_decode = chan.decode

        async def counting(message, propagate=False):
            self.decodes.append(chan.get_topic_name())
            return await real_decode(message, propagate=propagate)

        chan.decode = counting

    def fail_decode(self, exc: BaseException, only: Optional[str] = None) -> None:
        """Make decoding raise, for every channel or just one by name."""
        for chan in self.channels:
            if only is not None and chan.get_topic_name() != only:
                continue

            async def failing(message, propagate=False, *, _c=chan):
                self.decodes.append(_c.get_topic_name())
                raise exc

            chan.decode = failing

    def build(self, impl: str, tp: TP = TP1) -> Any:
        """The handler under test, built the way the conductor builds it."""
        if impl == "cython":
            assert ConductorHandler is not None
            return ConductorHandler(self.conductor, tp, self.channel_set)
        return self.conductor._compiler.build(self.conductor, tp, self.channel_set)

    def message(self, offset: int = 0, key: bytes = b"k", value: bytes = b"v") -> Any:
        return Message(
            "foo",
            0,
            offset,
            0.0,
            0,
            None,
            key,
            value,
            None,
            tp=TP1,
            generation_id=self.app.consumer_generation_id,
        )

    def reset(self) -> None:
        """Clear everything a previous run left behind."""
        for chan in self.channels:
            while not chan.queue.empty():
                chan.queue.get_nowait()
        for recorded in (
            self.buffer_full_sensor,
            self.consumer_buffer_full,
            self.consumer_buffer_drop,
            self.key_decode_errors,
            self.value_decode_errors,
            self.decodes,
        ):
            recorded.clear()

    def drain(self) -> Dict[str, List[Any]]:
        """Everything sitting in the channel queues, keyed by topic name."""
        out: Dict[str, List[Any]] = {}
        for chan in self.channels:
            got = []
            while not chan.queue.empty():
                event = chan.queue.get_nowait()
                got.append((event.key, event.value, event.message.offset))
            out[chan.get_topic_name()] = got
        return out

    def observations(self, message: Optional[Message] = None) -> Dict[str, Any]:
        """The full comparable record of a run."""
        delivered = self.drain()
        record: Dict[str, Any] = {
            "delivered": delivered,
            "n_delivered_total": sum(len(v) for v in delivered.values()),
            "n_decodes": len(self.decodes),
            # The arguments, not just the count: what gets passed to
            # `on_topic_buffer_full` is the metric's key.
            "buffer_full_sensor": list(self.buffer_full_sensor),
            "consumer_buffer_full": len(self.consumer_buffer_full),
            "consumer_buffer_drop": len(self.consumer_buffer_drop),
            "key_decode_errors": sorted(self.key_decode_errors),
            "value_decode_errors": sorted(self.value_decode_errors),
        }
        if message is not None:
            record["refcount"] = message.refcount
            record["acked"] = message.acked
        return record


@pytest.fixture()
def harness(app, request):
    param = getattr(request, "param", 1)
    if isinstance(param, tuple):
        n, heterogeneous = param
    else:
        n, heterogeneous = param, False
    return Harness(app, n_channels=n, heterogeneous=heterogeneous)


def assert_parity(results: Dict[str, Any]) -> None:
    cython, python = results["cython"], results["python"]
    assert cython == python, (
        f"the Cython conductor and the pure-Python conductor disagree.\n"
        f"  cython: {cython}\n"
        f"  python: {python}"
    )


async def run_both(harness: Harness, scenario) -> Dict[str, Any]:
    """Run `scenario(handler, harness)` under each implementation."""
    results = {}
    for impl in IMPLS:
        harness.reset()
        results[impl] = await scenario(harness.build(impl), harness)
    return results


# ------------------------------------------------------------------ delivery
@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [1, 2, 3], indirect=True)
async def test_parity__fan_out(harness) -> None:
    """Every subscribed channel gets the event, and refcount matches."""

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)
    # ...and the shared expectation, so a pair that agrees but is wrong fails.
    n = len(harness.channels)
    assert results["cython"]["refcount"] == n
    assert results["cython"]["n_delivered_total"] == n


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
async def test_parity__no_channels(harness) -> None:
    """A TP with no subscribers must not touch the message."""
    harness.channel_set = set()

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)
    assert results["cython"]["refcount"] == 0
    assert results["cython"]["n_decodes"] == 0


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [3], indirect=True)
async def test_parity__multiple_messages(harness) -> None:
    """A batch, to catch state carried between calls."""

    async def scenario(handler, h):
        for offset in range(5):
            await handler(h.message(offset=offset, key=f"k{offset}".encode()))
        return h.observations()

    results = await run_both(harness, scenario)
    assert_parity(results)
    assert results["cython"]["n_delivered_total"] == 15


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [2, 4], indirect=True)
async def test_parity__event_reuse_for_matching_keyid(harness) -> None:
    """Channels with the same (key_type, value_type) share one decode.

    Both implementations are supposed to deserialize the payload once and reuse
    the event for every channel whose key/value types match, so the decode
    count is observable behaviour and not an implementation detail: it is the
    per-message deserialization cost of a topic with several subscribers.
    """

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)
    # Identical key/value types across all channels: decode once, reuse.
    assert results["cython"]["n_decodes"] == 1, (
        f"expected one decode reused across "
        f"{len(harness.channels)} same-typed channels, got "
        f"{results['cython']['n_decodes']}"
    )


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [(2, True), (4, True)], indirect=True)
async def test_parity__no_reuse_for_differing_keyid(harness) -> None:
    """Channels with different (key_type, value_type) each decode their own.

    This is the branch the Cython conductor could never reach.  `event_keyid`
    stayed None, so the mismatch case was dead code -- and it was also wrong:
    `_decode` fell off the end returning a bare `None`, which unpacked into two
    names would have raised TypeError.  Fixing the reuse without this branch
    would have turned a silent inefficiency into a crash on any topic whose
    subscribers declare different key or value types.
    """

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)
    n = len(harness.channels)
    # Half the channels share the pinned event's keyid, half do not: one decode
    # for the pinned event plus one per mismatched channel.
    assert results["cython"]["n_decodes"] == 1 + n // 2
    assert results["cython"]["n_delivered_total"] == n


# -------------------------------------------------------------- decode errors
@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [1, 3], indirect=True)
@pytest.mark.parametrize(
    "exc_cls,bucket",
    [
        (KeyDecodeError, "key_decode_errors"),
        (ValueDecodeError, "value_decode_errors"),
    ],
)
async def test_parity__decode_error_propagates(harness, exc_cls, bucket) -> None:
    """A decode failure must reach every undelivered channel, and ack them.

    This is the branch that acks the message on behalf of the channels that
    never received it; getting that count wrong either stalls the commit or
    commits past an unprocessed message.
    """
    harness.fail_decode(exc_cls("boom"))

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)
    assert len(results["cython"][bucket]) == len(harness.channels)
    assert results["cython"]["n_delivered_total"] == 0


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True)
@pytest.mark.parametrize("harness", [3], indirect=True)
async def test_parity__decode_error_on_one_channel(harness) -> None:
    """One channel's decode fails; the rest of the fan-out must match.

    Which channels were already delivered when the failure lands depends on the
    iteration order of the shared `channels` set -- identical for both
    implementations here, which is the point of sharing it.
    """
    harness.fail_decode(ValueDecodeError("boom"), only="foo1")

    async def scenario(handler, h):
        message = h.message()
        await handler(message)
        return h.observations(message)

    results = await run_both(harness, scenario)
    assert_parity(results)


# ------------------------------------------------------------ buffer pressure
@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True, stream_buffer_maxsize=2)
async def test_parity__queue_full_path(harness) -> None:
    """When a channel queue is full the handler must await ``chan.put``.

    Filling the queue first drives ``_handle_full``, a separate branch in both
    implementations that also fires the ``on_topic_buffer_full`` sensor.

    The sensor argument is checked explicitly, not just for parity.  Both
    implementations used to pass the *channel* here, so they agreed with each
    other and this comparison stayed green while both were wrong -- a shared
    mistake is exactly what a differential test cannot see.
    """

    async def scenario(handler, h):
        chan = h.channels[0]
        for i in range(2):  # stream_buffer_maxsize
            chan.queue.put_nowait(f"filler{i}")
        assert chan.queue.full()

        message = h.message()
        # The put blocks until something drains the queue.
        pending = asyncio.ensure_future(handler(message))
        await asyncio.sleep(0)
        chan.queue.get_nowait()
        chan.queue.get_nowait()
        await asyncio.wait_for(pending, timeout=5)
        return {
            "buffer_full_sensor": list(h.buffer_full_sensor),
            "consumer_buffer_full": list(h.consumer_buffer_full),
            "consumer_buffer_drop": list(h.consumer_buffer_drop),
            "qsize": chan.queue.qsize(),
            "refcount": message.refcount,
        }

    results = await run_both(harness, scenario)
    assert_parity(results)
    reported = results["cython"]["buffer_full_sensor"]
    assert reported, "the full-queue path did not fire the on_topic_buffer_full sensor"
    assert all(arg == TP1 for arg in reported), (
        f"on_topic_buffer_full must be given the TP -- it is the key of "
        f"Monitor.topic_buffer_full, a Counter[TP], and the pressure-high path "
        f"already passes one.  Got: {reported}"
    )


@pytest.mark.asyncio
@pytest.mark.conf(stream_buffer_maxsize=2)
@pytest.mark.parametrize("impl", IMPLS)
async def test_monitor_counts_buffer_full_by_tp(app, impl) -> None:
    """``Monitor.topic_buffer_full`` must be keyed by TP, from either path.

    The counter is a ``Counter[TP]``, and two code paths report into it: the
    pressure-high callback (which always passed a TP) and the full-queue path
    (which passed the channel).  The same partition therefore accumulated under
    two different keys, so per-TP counts were split and ``/stats`` grew a second
    entry labelled by channel for the same partition.

    Unlike the parity tests, this asserts the behaviour rather than agreement:
    both implementations made the same mistake, so they agreed with each other
    throughout.  Runs against the pure-Python conductor too, since the defect
    was in both.
    """
    if impl == "cython" and ConductorHandler is None:
        pytest.skip("conductor extension not built in place")

    monitor = Monitor()
    app.sensors.add(monitor)

    h = Harness(app, n_channels=1)
    # Undo the harness's sensor stub: the real delegate is what is under test.
    app.sensors.on_topic_buffer_full = monitor.on_topic_buffer_full

    handler = h.build(impl)
    chan = h.channels[0]
    for i in range(2):  # stream_buffer_maxsize -> forces the full-queue path
        chan.queue.put_nowait(f"filler{i}")

    pending = asyncio.ensure_future(handler(h.message()))
    await asyncio.sleep(0)
    chan.queue.get_nowait()
    chan.queue.get_nowait()
    await asyncio.wait_for(pending, timeout=5)

    assert monitor.topic_buffer_full, "the full-queue path reported nothing"
    bad = [key for key in monitor.topic_buffer_full if not isinstance(key, TP)]
    assert not bad, (
        f"Monitor.topic_buffer_full is a Counter[TP], but the {impl} conductor "
        f"keyed it by {[type(k).__name__ for k in bad]}: {bad}"
    )
    assert monitor.topic_buffer_full[TP1] > 0


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.conf(cython_optimizations=True, stream_buffer_maxsize=8)
async def test_parity__pressure_callbacks(harness) -> None:
    """High-pressure and pressure-drop callbacks must fire identically.

    ``put_nowait_enhanced`` invokes ``on_pressure_high`` once the queue passes
    its pressure ratio; both implementations pass their own bound callbacks in,
    so this checks the wiring rather than the queue.
    """

    async def scenario(handler, h):
        chan = h.channels[0]
        for offset in range(8):
            await handler(h.message(offset=offset))
        # Drain, to drive the pressure-drop callback.
        while not chan.queue.empty():
            chan.queue.get_nowait()
        return {
            "buffer_full_sensor": len(h.buffer_full_sensor),
            "consumer_buffer_full": len(h.consumer_buffer_full),
            "consumer_buffer_drop": len(h.consumer_buffer_drop),
        }

    results = await run_both(harness, scenario)
    assert_parity(results)


@requires_cython_conductor
@pytest.mark.asyncio
@pytest.mark.parametrize("harness", [3], indirect=True)
async def test_event_reuse_is_off_by_default(harness) -> None:
    """Without the opt-in, the conductor behaves as the released versions do.

    `cython_optimizations` defaults to False, so the repaired reuse stays
    dormant and every channel deserializes its own event -- exactly as before
    the fix.  No `conf` marker here on purpose: this is what an unmodified app
    gets.

    This is also where the Cython and pure-Python conductors legitimately
    differ, so it is not a parity test.  That divergence is not new; the flag
    only makes it selectable.
    """
    assert harness.app.conf.cython_optimizations is False

    handler = harness.build("cython")
    message = harness.message()
    await handler(message)
    obs = harness.observations(message)

    n = len(harness.channels)
    assert obs["n_decodes"] == n, (
        f"expected one decode per channel with the optimizations off, got "
        f"{obs['n_decodes']} for {n} channels: reuse is no longer opt-in"
    )
    # Delivery itself is unchanged -- only how many times the payload is read.
    assert obs["n_delivered_total"] == n
    assert obs["refcount"] == n
