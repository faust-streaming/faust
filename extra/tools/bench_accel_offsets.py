#!/usr/bin/env python
"""Compare the available ``first_consecutive_run`` implementations.

``Consumer._new_offset`` scans the sorted list of acked offsets for a
partition on every commit, which is the most batch-shaped hot path Faust has
in its own code -- one call, then up to hundreds of thousands of iterations.
``docs/proposals/rust-acceleration.md`` names exactly that shape as the
trigger for revisiting Rust, so this script measures it.

Usage::

    pip install -e .                              # builds faust._cython.functional
    extra/tools/build_rust_accel.sh --no-abi3     # optional, adds the rust columns
    python extra/tools/bench_accel_offsets.py

Any implementation that is not importable is skipped, so this is useful on a
pure-Python install too.
"""

import timeit
from typing import Any, Callable, List, Tuple

from faust.utils.functional import _py_first_consecutive_run

SIZES = (1_000, 10_000, 100_000, 600_000)
MIN_TIME = 0.25
REPEAT = 5

Impl = Tuple[str, Callable[[List[int]], Any]]


def _implementations() -> List[Impl]:
    impls: List[Impl] = [("python", _py_first_consecutive_run)]
    try:
        from faust.utils._cython.functional import first_consecutive_run
    except ImportError:
        pass
    else:
        impls.append(("cython", first_consecutive_run))

    # Not built by default; see docs/proposals/rust-acceleration.md.
    try:
        from faust._rust import _accel
    except ImportError:
        return impls
    # Ordered worst to best, which is also least to most unsafe.
    for name, attr in (
        ("rust/pyo3", "first_consecutive_run"),
        ("rust/ffi", "first_consecutive_run_ffi"),
        ("rust/macro", "first_consecutive_run_macro"),
    ):
        fn = getattr(_accel, attr, None)
        if fn is not None:
            impls.append((name, fn))
    return impls


def _check(impls: List[Impl]) -> None:
    """Fail loudly rather than benchmark implementations that disagree."""
    cases = [
        [1, 2, 3, 4, 6, 7, 8],
        [1, 4, 6, 8, 10],
        [1],
        [],
        [0, 1, 2],
        [1, 1, 2],
        list(range(100)),
    ]
    reference_name, reference = impls[0]
    for case in cases:
        want = reference(list(case))
        for name, fn in impls[1:]:
            got = fn(list(case))
            if got != want:
                raise SystemExit(
                    f"{name} disagrees with {reference_name} on {case!r}: "
                    f"{got!r} != {want!r}"
                )
    print(f"{len(impls)} implementations agree on {len(cases)} cases\n")


def _ns_per_call(fn: Callable[[List[int]], Any], data: List[int]) -> float:
    fn(data)
    number = 1
    while timeit.timeit(lambda: fn(data), number=number) < MIN_TIME:
        number *= 4
    best = min(timeit.timeit(lambda: fn(data), number=number) for _ in range(REPEAT))
    return best / number * 1e9


def main() -> int:
    impls = _implementations()
    _check(impls)

    width = max(len(name) for name, _ in impls) + 2
    header = f"{'offsets':>9}" + "".join(f"{name:>{width + 4}}" for name, _ in impls)
    print(header)
    print("-" * len(header))

    baselines = {}
    for size in SIZES:
        data = list(range(size))
        row = f"{size:>9}"
        for name, fn in impls:
            micros = _ns_per_call(fn, data) / 1000.0
            baselines.setdefault(name, []).append(micros)
            row += f"{micros:>{width}.1f} us"
        print(row)

    if "cython" in baselines:
        print("\nrelative to cython (higher is faster):")
        for name, values in baselines.items():
            ratios = [
                cython / value for cython, value in zip(baselines["cython"], values)
            ]
            span = f"{min(ratios):.2f}x - {max(ratios):.2f}x"
            print(f"  {name:>12}: {span}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
