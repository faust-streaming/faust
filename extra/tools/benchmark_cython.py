#!/usr/bin/env python3
"""Micro-benchmark the optional Cython accelerators against pure Python.

Faust ships a handful of hot code paths twice: a readable pure-Python
implementation, and a Cython one that is used instead whenever the extension
modules could be built (see ``NO_CYTHON``).  This script imports *both* and
times them side by side in the same interpreter, so the numbers are directly
comparable.

Usage:

.. sourcecode:: console

    $ python setup.py build_ext --inplace
    $ python extra/tools/benchmark_cython.py

Anything that needs a running worker (the ``Stream`` iterator and the topic
``Conductor``) is out of scope here -- those are covered by the end-to-end
benchmark in ``extra/tools/benchmark.py``.
"""

import sys
from time import perf_counter
from typing import Callable, List, Optional, Tuple

from faust.sensors.base import _PySensorDelegateBase, _SensorDelegateBase
from faust.transport.utils import (
    DefaultSchedulingStrategy,
    _py_records_iterator,
    _records_iterator,
)
from faust.types import TP
from faust.utils.functional import _py_first_consecutive_run, first_consecutive_run
from faust.windows import (
    HoppingWindow,
    SlidingWindow,
    _PyHoppingWindow,
    _PySlidingWindow,
)

MIN_TIME = 0.2  # seconds to spend on each timed run
Case = Tuple[str, Callable[[], None], Callable[[], None], int]


def _time(fn: Callable[[], None], iterations: int) -> float:
    start = perf_counter()
    for _ in range(iterations):
        fn()
    return perf_counter() - start


def _autorange(fn: Callable[[], None]) -> int:
    """Find an iteration count that runs for at least ``MIN_TIME``."""
    iterations = 1
    while True:
        elapsed = _time(fn, iterations)
        if elapsed >= MIN_TIME or iterations >= 1 << 30:
            return iterations
        iterations *= 4 if elapsed < MIN_TIME / 8 else 2


def _best(fn: Callable[[], None], iterations: int, rounds: int = 5) -> float:
    """Return the best per-operation time in nanoseconds."""
    return min(_time(fn, iterations) for _ in range(rounds)) / iterations * 1e9


def run(cases: List[Case]) -> bool:
    """Time each case and print a comparison table."""
    name_width = max(len(name) for name, _, _, _ in cases)
    print(
        f"{'benchmark'.ljust(name_width)}  {'python':>12}  "
        f"{'cython':>12}  {'speedup':>8}"
    )
    print("-" * (name_width + 38))

    all_faster = True
    for name, py_fn, cy_fn, scale in cases:
        py_fn()
        cy_fn()
        iterations = _autorange(cy_fn)
        py_ns = _best(py_fn, max(1, iterations // 4)) / scale
        cy_ns = _best(cy_fn, iterations) / scale
        speedup = py_ns / cy_ns
        all_faster &= speedup > 1.0
        print(
            f"{name.ljust(name_width)}  {py_ns:9.1f} ns  "
            f"{cy_ns:9.1f} ns  {speedup:7.2f}x"
        )
    return all_faster


def offset_cases() -> List[Case]:
    """faust.utils.functional.first_consecutive_run -- once per TP per commit."""
    cases = []
    for size in (100, 10_000, 100_000):
        acked = list(range(size))
        cases.append(
            (
                f"first_consecutive_run/{size} offsets",
                lambda acked=acked: _py_first_consecutive_run(acked),
                lambda acked=acked: first_consecutive_run(acked),
                1,
            )
        )
    return cases


def _drain(impl: Callable, records: dict) -> None:
    for _ in impl(DefaultSchedulingStrategy.map_from_records(records)):
        pass


def scheduler_cases() -> List[Case]:
    """faust.transport.utils records_iterator -- once per fetched record."""
    cases = []
    for topics, partitions, per_partition in ((1, 1, 500), (4, 8, 100), (8, 16, 50)):
        records = {
            TP(f"topic-{t}", p): list(range(per_partition))
            for t in range(topics)
            for p in range(partitions)
        }
        cases.append(
            (
                f"records_iterator/{topics}t x {partitions}p x {per_partition}",
                lambda records=records: _drain(_py_records_iterator, records),
                lambda records=records: _drain(_records_iterator, records),
                topics * partitions * per_partition,
            )
        )
    return cases


class _NoopSensor:
    """Stand-in sensor: measures delegation overhead, not sensor work."""

    beacon = None

    def on_message_in(self, tp, offset, message) -> None: ...

    def on_stream_event_in(self, tp, offset, stream, event) -> None:
        return None

    def on_stream_event_out(self, tp, offset, stream, event, state=None) -> None: ...

    def on_message_out(self, tp, offset, message) -> None: ...


class _FakeBeacon:
    def new(self, sensor: object) -> None:
        return None


class _FakeApp:
    beacon = _FakeBeacon()


_TP = TP("t", 0)
_OFFSET = 42


def _event_in(delegate: object) -> None:
    delegate.on_stream_event_in(_TP, _OFFSET, None, None)


def _all_hooks(delegate: object) -> None:
    """The exact sequence of delegate calls a single message triggers."""
    delegate.on_message_in(_TP, _OFFSET, None)
    state = delegate.on_stream_event_in(_TP, _OFFSET, None, None)
    delegate.on_stream_event_out(_TP, _OFFSET, None, None, state)
    delegate.on_message_out(_TP, _OFFSET, None)


def sensor_cases() -> List[Case]:
    """faust.sensors.base -- four hooks fire on every single message."""
    cases = []
    for n_sensors in (1, 3):
        py = _PySensorDelegateBase(_FakeApp())
        cy = _SensorDelegateBase(_FakeApp())
        for _ in range(n_sensors):
            py.add(_NoopSensor())
            cy.add(_NoopSensor())

        cases.extend(
            [
                (
                    f"SensorDelegate.on_stream_event_in/{n_sensors} sensor(s)",
                    lambda py=py: _event_in(py),
                    lambda cy=cy: _event_in(cy),
                    1,
                ),
                (
                    f"SensorDelegate all 4 hooks/{n_sensors} sensor(s)",
                    lambda py=py: _all_hooks(py),
                    lambda cy=cy: _all_hooks(cy),
                    1,
                ),
            ]
        )
    return cases


def window_cases() -> List[Case]:
    """faust.windows -- already shipped; included for completeness."""
    py_hopping = _PyHoppingWindow(10.0, 5.0, 60.0)
    cy_hopping = HoppingWindow(10.0, 5.0, 60.0)
    py_sliding = _PySlidingWindow(10.0, 5.0, 60.0)
    cy_sliding = SlidingWindow(10.0, 5.0, 60.0)
    timestamp = 1_600_000_000.0
    return [
        (
            "HoppingWindow.ranges",
            lambda: py_hopping.ranges(timestamp),
            lambda: cy_hopping.ranges(timestamp),
            1,
        ),
        (
            "HoppingWindow.current",
            lambda: py_hopping.current(timestamp),
            lambda: cy_hopping.current(timestamp),
            1,
        ),
        (
            "SlidingWindow.ranges",
            lambda: py_sliding.ranges(timestamp),
            lambda: cy_sliding.ranges(timestamp),
            1,
        ),
    ]


def main(argv: Optional[List[str]] = None) -> int:
    """Run every benchmark group."""
    if _records_iterator is _py_records_iterator:
        print(
            "The Cython extensions are not built (or NO_CYTHON is set), so "
            "there is nothing to compare against.\n"
            "Build them first with:  python setup.py build_ext --inplace",
            file=sys.stderr,
        )
        return 1

    groups = [
        ("offset commit path", offset_cases),
        ("consumer record scheduler", scheduler_cases),
        ("sensor delegation", sensor_cases),
        ("windows", window_cases),
    ]
    for title, make_cases in groups:
        print(f"\n== {title} ==")
        run(make_cases())
    print("\nLower is better; speedup is python/cython.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
