"""The acknowledgement transition must be atomic under concurrent acks.

``Message.ack`` reads ``acked``, decrements ``refcount`` and, on reaching
zero, runs the final-ack bookkeeping in the consumer.  Those are separate
bytecodes, and the interpreter can switch threads between any of them, so
without a lock two threads acking the same message can read the same
refcount and both write ``n - 1``: a decrement is lost and the final ack
either fires twice or never fires at all.

This is not specific to free-threading.  It reproduces on a GIL build --
`sys.setswitchinterval` makes it reliable -- because the GIL is released
between bytecodes.  Removing the GIL widens the window rather than opening
it, which is why these tests are not marked as requiring 3.13t.

``Event.ack()`` is public API, so a user thread can enter this path
directly, which is what makes the race reachable rather than theoretical.

Both paths are covered: ``Message.ack``, and the ``StreamIterator.after``
accelerator, which inlines the same transition instead of calling it and so
has to take the same lock independently.

The two differ in *when* they are exposed, which is worth knowing before
reading a green run as proof:

* The pure-Python path races on any build.  Measured on GIL-enabled 3.11,
  13 of 200 trials lost a decrement, and in 8 of 200 the final ack never
  ran at all -- an offset that never becomes safe to commit.
* The compiled path races only without the GIL.  Compiled code does not go
  back through the eval loop, so nothing switches threads inside that C
  function while a GIL is held, and the transition is atomic by accident.
  Remove the GIL and the accident goes away: on free-threaded 3.13t, 6 of
  50 trials lost an ack before the lock was added, against 0 of 50 after.

So `test_cython_after_does_not_lose_acks` passes with or without the fix on
a GIL build.  It is not redundant -- it is the only check that covers that
path at all -- but it can only *fail* on a free-threaded interpreter, which
the CI job provides.
"""

import sys
import threading
from typing import Any, List

import pytest

from faust.events import Event
from faust.types.tuples import ConsumerMessage, Message
from faust.windows import HoppingWindow, _PyHoppingWindow

#: See tests/unit/test_cython_parity.py: true when the compiled extensions
#: were built in place and imported, rather than silently falling back.
CYTHON_LOADED = HoppingWindow is not _PyHoppingWindow

requires_cython = pytest.mark.skipif(
    not CYTHON_LOADED,
    reason="extensions not built in place (USE_CYTHON=1 python setup.py "
    "build_ext --inplace)",
)

#: Enough threads to interleave reliably; more than the machine has cores is
#: fine and helps, since the failure needs a switch inside the window rather
#: than genuine parallelism.
THREADS = 32

#: The race is probabilistic.  Before the fix roughly 3% of trials lost a
#: decrement at this thread count, so a single trial proves little; this many
#: makes a regression essentially certain to be caught while keeping the test
#: well under a second.
TRIALS = 200


@pytest.fixture()
def fast_switching() -> Any:
    """Make the interpreter switch threads aggressively.

    Without this the GIL is handed over every 5ms by default, so a window a
    few bytecodes wide is almost never hit and the test would pass on broken
    code.  Restored afterwards because it is process-global.
    """
    previous = sys.getswitchinterval()
    sys.setswitchinterval(1e-9)
    try:
        yield
    finally:
        sys.setswitchinterval(previous)


class _RecordingConsumer:
    """Minimal consumer that counts final acks.

    `Message.on_final_ack` just sets `acked`; it is `ConsumerMessage` that
    routes to `Consumer.ack`.  Counting here records how many times the
    transition decided it was the last reference, which is the property at
    stake.
    """

    def __init__(self) -> None:
        self.final_acks = 0
        self._lock = threading.Lock()

    def ack(self, message: Message) -> bool:
        with self._lock:
            self.final_acks += 1
        return True


def _message(refcount: int, cls: Any = Message) -> Any:
    message = cls(
        topic="topic",
        partition=0,
        offset=0,
        timestamp=0.0,
        timestamp_type=0,
        headers={},
        key=b"k",
        value=b"v",
        checksum=None,
    )
    message.refcount = refcount
    return message


def _ack_from_threads(message: Message, consumer: Any, n: int) -> None:
    """Have `n` threads ack `message` once each, as simultaneously as possible."""
    barrier = threading.Barrier(n)

    def worker() -> None:
        barrier.wait()
        message.ack(consumer)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


@pytest.mark.usefixtures("fast_switching")
def test_concurrent_acks_do_not_lose_decrements() -> None:
    """`THREADS` acks of a message with `THREADS` references must fully ack it."""
    failures: List[str] = []

    for trial in range(TRIALS):
        consumer = _RecordingConsumer()
        message = _message(THREADS)

        _ack_from_threads(message, consumer, THREADS)

        if message.refcount != 0:
            failures.append(
                f"trial {trial}: refcount is {message.refcount}, expected 0 "
                f"-- {message.refcount} decrement(s) lost"
            )
        elif not message.acked:
            failures.append(f"trial {trial}: refcount reached 0 but acked is False")

    assert not failures, (
        f"{len(failures)} of {TRIALS} trials lost an acknowledgement:\n  "
        + "\n  ".join(failures[:5])
    )


@pytest.mark.usefixtures("fast_switching")
def test_final_ack_runs_exactly_once() -> None:
    """The last-reference branch must be taken once, not zero or twice.

    Distinct from the refcount check: a lost decrement can leave the count
    correct while two threads both observe zero, which would commit an offset
    twice.
    """
    counts: List[int] = []

    for _ in range(TRIALS):
        consumer = _RecordingConsumer()
        # ConsumerMessage, not Message: its `on_final_ack` is the one that
        # routes to `Consumer.ack`, which is where the offset bookkeeping
        # that must not run twice actually lives.
        message = _message(THREADS, cls=ConsumerMessage)

        _ack_from_threads(message, consumer, THREADS)

        counts.append(consumer.final_acks)

    bad = [c for c in counts if c != 1]
    assert not bad, (
        f"final ack ran {sorted(set(bad))} time(s) instead of exactly once, "
        f"in {len(bad)} of {TRIALS} trials"
    )


@requires_cython
@pytest.mark.usefixtures("fast_switching")
@pytest.mark.asyncio
async def test_cython_after_does_not_lose_acks(*, app: Any) -> None:
    """The compiled `after()` must be as atomic as the code it replaces.

    It inlines the transition rather than calling `Message.ack`, so it does
    not inherit that method's lock and would otherwise stay racy after the
    pure-Python path was fixed -- the accelerated path losing acks that the
    interpreted one keeps.
    """
    from faust.streams import _CStreamIterator

    assert _CStreamIterator is not None, "compiled stream iterator not loaded"

    failures: List[str] = []
    # Fewer trials than above: each one builds a stream, and the window here
    # is the same width, so this still fails reliably when the lock is absent.
    trials = TRIALS // 4

    for trial in range(trials):
        stream = app.stream(app.channel())
        iterator = _CStreamIterator(stream)
        message = _message(THREADS, cls=ConsumerMessage)
        event = Event(app, message.key, message.value, {}, message)

        barrier = threading.Barrier(THREADS)

        def worker(it: Any = iterator, ev: Any = event, b: Any = barrier) -> None:
            b.wait()
            it.after(ev, True, None)

        threads = [threading.Thread(target=worker) for _ in range(THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        if message.refcount != 0:
            failures.append(
                f"trial {trial}: refcount is {message.refcount}, expected 0"
            )
        elif not message.acked:
            failures.append(f"trial {trial}: refcount reached 0 but acked is False")

    assert not failures, (
        f"the compiled after() lost acknowledgements in {len(failures)} of "
        f"{trials} trials:\n  " + "\n  ".join(failures[:5])
    )


@pytest.mark.usefixtures("fast_switching")
def test_concurrent_increfs_are_not_lost() -> None:
    """`incref` is the same read-modify-write and needs the same guarantee."""
    for _ in range(TRIALS):
        message = _message(0)
        barrier = threading.Barrier(THREADS)

        def worker(m: Message = message, b: Any = barrier) -> None:
            b.wait()
            m.incref()

        threads = [threading.Thread(target=worker) for _ in range(THREADS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert message.refcount == THREADS, (
            f"refcount is {message.refcount}, expected {THREADS} -- "
            f"{THREADS - message.refcount} incref(s) lost"
        )
