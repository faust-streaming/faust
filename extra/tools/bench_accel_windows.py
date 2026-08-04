#!/usr/bin/env python
"""Compare the available HoppingWindow implementations.

Faust ships a pure-Python ``HoppingWindow`` and a Cython one, and the Rust
evaluation in ``docs/proposals/rust-acceleration.md`` adds a third candidate.
This script times whichever of them are importable, checks they agree, and
prints ns/call plus speedups so the numbers in that document can be re-checked
on other hardware.

Usage::

    pip install -e .          # builds faust._cython.windows
    python extra/tools/bench_accel_windows.py

Any implementation that is not importable is simply skipped, so the script is
useful on a pure-Python install too.
"""

import timeit
from typing import Any, Callable, Dict, List, Tuple

from faust.windows import _PyHoppingWindow

SIZE = 60.0
STEP = 10.0
EXPIRES = 3600.0
TIMESTAMP = 1_700_000_000.123
ITERATIONS = 200_000
REPEAT = 5


def _implementations() -> List[Tuple[str, Any]]:
    impls: List[Tuple[str, Any]] = [("python", _PyHoppingWindow)]
    try:
        from faust._cython.windows import HoppingWindow as CythonHoppingWindow
    except ImportError:
        pass
    else:
        impls.append(("cython", CythonHoppingWindow))
    try:
        # Not built by default; see docs/proposals/rust-acceleration.md.
        from faust._rust._accel import HoppingWindow as RustHoppingWindow
    except ImportError:
        pass
    else:
        impls.append(("rust", RustHoppingWindow))
    return impls


CASES: Dict[str, Callable[[Any], Any]] = {
    "ranges(ts)": lambda w: w.ranges(TIMESTAMP),
    "current(ts)": lambda w: w.current(TIMESTAMP),
    "stale(ts, ts+1)": lambda w: w.stale(TIMESTAMP, TIMESTAMP + 1.0),
    "earliest(ts)": lambda w: w.earliest(TIMESTAMP),
}


def _time_ns_per_call(fn: Callable[[], Any]) -> float:
    best = min(timeit.repeat(fn, number=ITERATIONS, repeat=REPEAT))
    return best / ITERATIONS * 1e9


def main() -> None:
    impls = _implementations()
    names = [name for name, _ in impls]
    print(f"implementations: {', '.join(names)}")
    print(f"size={SIZE} step={STEP} expires={EXPIRES} " f"iterations={ITERATIONS}\n")

    windows = [(name, cls(SIZE, STEP, EXPIRES)) for name, cls in impls]

    for case, fn in CASES.items():
        results = {name: fn(window) for name, window in windows}
        distinct = {repr(value) for value in results.values()}
        if len(distinct) > 1:
            print(f"MISMATCH in {case}: {results}")

    header = f"{'case':<18}" + "".join(f"{name:>12}" for name in names)
    if "python" in names:
        header += "".join(
            f"{name + ' vs py':>14}" for name in names if name != "python"
        )
    print(header)

    for case, fn in CASES.items():
        timings = {
            name: _time_ns_per_call(lambda fn=fn, w=window: fn(w))
            for name, window in windows
        }
        row = f"{case:<18}" + "".join(f"{timings[n]:>10.0f}ns" for n in names)
        if "python" in names:
            base = timings["python"]
            row += "".join(
                f"{base / timings[n]:>13.2f}x" for n in names if n != "python"
            )
        print(row)

    # Cost of crossing the Python/native boundary with no work behind it: the
    # floor under any accelerator, and the reason fine-grained calls do not
    # benefit from a faster language.
    print()
    for name, window in windows:
        if name == "python":
            continue
        attr = _time_ns_per_call(lambda w=window: w.size)
        print(f"{name:<8} attribute read (call-boundary floor): {attr:.0f}ns")


if __name__ == "__main__":
    main()
