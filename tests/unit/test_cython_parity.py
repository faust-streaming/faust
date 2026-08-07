"""Check the optional Cython accelerators against their pure-Python twins.

Faust ships several hot paths twice: a readable pure-Python implementation and
a Cython one used instead whenever the extension could be built (see
``NO_CYTHON``).  The two are expected to behave identically, but nothing has
been enforcing that -- and the duplication has already cost real bugs:

* #608, "Fix cython stream_event_in to match python impl";
* the ``on_topic_buffer_full`` defect recorded but deliberately left unfixed
  in ``faust/transport/conductor.py``, because fixing one twin alone would
  make them disagree;
* the ``_try_get_quick_value`` pair fixed alongside this file, where the
  extension's queue fast path was both unreachable and, had it run, wrong.

The last one survived because the compiled code is never imported by the test
suite unless the extensions were built *in place*: pytest runs from the
repository root, so ``import faust`` resolves to the source tree, and every
accelerated import sits behind ``try: ... except ImportError``.  A missing
``.so`` therefore means the whole suite silently tests pure Python -- including
the parity tests that exist, which then compare an implementation against
itself.

``test_cython_is_loaded_when_required`` closes that hole: set
``FAUST_REQUIRE_CYTHON=1`` (the CI legs that build the extensions do) and the
suite fails loudly rather than quietly proving nothing.
"""

import asyncio
import os

import pytest

from faust.windows import (
    HoppingWindow,
    SlidingWindow,
    _PyHoppingWindow,
    _PySlidingWindow,
)

#: True when faust imported the compiled window type rather than falling back.
CYTHON_LOADED = HoppingWindow is not _PyHoppingWindow

#: Set by the CI legs that build the extensions.  When set, the accelerators
#: are mandatory and their absence is a failure rather than a skip.
REQUIRE_CYTHON = bool(os.environ.get("FAUST_REQUIRE_CYTHON", False))

requires_cython = pytest.mark.skipif(
    not CYTHON_LOADED,
    reason="extensions not built in place (USE_CYTHON=1 python setup.py "
    "build_ext --inplace)",
)


def test_cython_is_loaded_when_required() -> None:
    """Fail when a build that must have the accelerators does not.

    Without this, `USE_CYTHON=true` legs pass just as happily on the
    pure-Python fallback, and every parity test below degrades into comparing
    an object with itself.
    """
    if not REQUIRE_CYTHON:
        pytest.skip("FAUST_REQUIRE_CYTHON not set")
    assert CYTHON_LOADED, (
        "FAUST_REQUIRE_CYTHON is set, but faust fell back to the pure-Python "
        "implementations: the extension modules were not importable from the "
        "source tree.  Build them with "
        "`USE_CYTHON=1 python setup.py build_ext --inplace`; `pip install .` "
        "is not enough, because pytest imports faust from the repository root."
    )


# --------------------------------------------------------------------- windows
#: (cython, python) pairs.  When the extensions are missing both entries are
#: the same object, which is exactly what the guard above exists to catch.
WINDOW_PAIRS = [
    pytest.param(HoppingWindow, _PyHoppingWindow, id="HoppingWindow"),
    pytest.param(SlidingWindow, _PySlidingWindow, id="SlidingWindow"),
]

#: Timestamps chosen to land on, just before and just after step boundaries,
#: where the two implementations' differing arithmetic is most likely to part.
TIMESTAMPS = [0.0, 0.5, 1.0, 4.9, 5.0, 5.1, 9.999, 10.0, 33.3, 100.0, 12345.678]


@requires_cython
@pytest.mark.parametrize("cy,py", WINDOW_PAIRS)
@pytest.mark.parametrize("timestamp", TIMESTAMPS)
def test_window_current_matches(cy, py, timestamp) -> None:
    if cy is HoppingWindow:
        a, b = cy(size=10, step=5, expires=3600), py(size=10, step=5, expires=3600)
    else:
        a, b = cy(before=10, after=0, expires=3600), py(
            before=10, after=0, expires=3600
        )
    assert a.current(timestamp) == pytest.approx(b.current(timestamp))


@requires_cython
@pytest.mark.parametrize("timestamp", TIMESTAMPS)
def test_hopping_window_ranges_matches(timestamp) -> None:
    a = HoppingWindow(size=10, step=5, expires=3600)
    b = _PyHoppingWindow(size=10, step=5, expires=3600)
    assert a.ranges(timestamp) == pytest.approx(b.ranges(timestamp))


@requires_cython
@pytest.mark.parametrize("timestamp", TIMESTAMPS)
def test_hopping_window_stale_matches(timestamp) -> None:
    a = HoppingWindow(size=10, step=5, expires=60)
    b = _PyHoppingWindow(size=10, step=5, expires=60)
    latest = timestamp + 3600
    assert a.stale(timestamp, latest) == b.stale(timestamp, latest)


# --------------------------------------------------------------------- streams
def _new_iterator(app):
    """A compiled StreamIterator over a fresh channel, plus a call counter.

    Driven directly rather than through ``async for``: ``Stream.__aiter__``
    starts the Stream service and needs a running worker, while
    ``StreamIterator.next()`` is exactly the code under test and needs
    neither.  Plain values (not Events) are used so the assertions stay on
    the queue path instead of the flow-control and acking machinery.

    The counter is on the channel's ``__anext__`` -- the awaiting path --
    which gives a clean binary signal: the fast path never touches it.  The
    two obvious alternatives do not work.  ``queue.get_nowait`` is called by
    ``Queue.get`` on the slow path too, and ``queue.empty`` is called from
    inside ``get_nowait`` as well, so both are called either way and only the
    exact counts differ.
    """
    from faust.streams import _CStreamIterator

    assert _CStreamIterator is not None, "compiled stream iterator not loaded"

    stream = app.stream(app.channel())
    # `app.stream(channel)` clones the channel, so the queue the iterator
    # reads is `stream.channel.queue` -- not the queue of the channel that
    # was passed in.
    channel = stream.channel
    queue = channel.queue

    real_anext = channel.__anext__
    anext_calls = []

    def counting_anext():
        anext_calls.append(1)
        return real_anext()

    channel.__anext__ = counting_anext
    # StreamIterator caches the channel's and queue's bound methods at
    # construction, so it has to be built after the patch is in place.
    return _CStreamIterator(stream), queue, anext_calls


@requires_cython
@pytest.mark.asyncio
async def test_cython_stream_uses_queue_fast_path(*, app) -> None:
    """The compiled iterator must take the non-blocking queue path.

    ``_try_get_quick_value`` skips the ``await`` when the channel queue
    already has something in it.  It used to test the truthiness of the bound
    ``queue.empty`` method rather than calling it, so the fast path was
    unreachable and every value went through ``await __anext__``.
    """
    it, queue, anext_calls = _new_iterator(app)

    for i in range(5):
        queue.put_nowait(i)

    seen = []
    for _ in range(5):
        value, _sensor_state = await asyncio.wait_for(it.next(), timeout=5)
        seen.append(value)

    assert seen == [0, 1, 2, 3, 4], (
        "the compiled iterator mis-unpacked the queue fast path: "
        "_try_get_quick_value must return (need_slow_get, value)"
    )
    assert anext_calls == [], (
        f"the iterator awaited Channel.__anext__ {len(anext_calls)} times for "
        f"5 already-queued values: it took the slow path instead of the queue "
        f"fast path, so _try_get_quick_value is testing the bound empty "
        f"method rather than calling it again"
    )


@requires_cython
@pytest.mark.asyncio
async def test_cython_stream_falls_back_to_slow_path_when_empty(*, app) -> None:
    """An empty queue must still take the awaiting path.

    The fix to ``_try_get_quick_value`` made the fast path reachable; this is
    the other half of the branch, guarding against a fix that always reports
    "not empty" and calls ``get_nowait()`` on an empty queue.

    The iterator is only checked to still be *pending* -- completing the slow
    path means driving ``Channel.__anext__`` on a channel with no running
    worker behind it, which is out of scope here.
    """
    it, queue, anext_calls = _new_iterator(app)
    assert queue.empty()

    pending = asyncio.ensure_future(it.next())
    try:
        # Several turns: next() awaits sleep(0) before it consults the queue.
        for _ in range(10):
            await asyncio.sleep(0)
        assert not pending.done(), (
            "iterator returned a value from an empty queue -- the empty "
            "branch of _try_get_quick_value is gone"
        )
        assert anext_calls, "iterator did not await Channel.__anext__ on an empty queue"
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
