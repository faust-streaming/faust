#!/usr/bin/env python
"""CPU and memory comparison across Python, PyPy, Cython and Rust.

``bench_accel_windows.py`` and ``bench_accel_offsets.py`` compare
implementations inside one interpreter.  This one adds the two axes those
cannot cover:

* **PyPy**, which needs a separate interpreter (it runs the pure-Python code
  under its JIT -- exactly what Faust's CI does, since the PyPy leg sets
  ``USE_CYTHON: 'false'``).
* **Memory**, which needs a fresh process per measurement so that peak RSS
  means something.

Every number is produced by a subprocess running exactly one implementation
against one workload, so nothing another implementation allocated can leak
into a reading.

Usage::

    pip install -e .                              # builds the Cython extensions
    extra/tools/build_rust_accel.sh --no-abi3     # optional: adds the rust rows
    python extra/tools/bench_accel_matrix.py

Rows for implementations that cannot be imported are dropped with a note, so
this is useful on a pure-Python install and without PyPy installed.
"""

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import timeit
from typing import Any, Callable, Dict, List, Optional, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

#: (label, interpreter, how to get the callable).  "pypy" runs the same
#: pure-Python code as "python", on a different interpreter.
IMPLEMENTATIONS = ("python", "pypy", "cython", "rust")

OFFSET_SIZES = (10_000, 100_000, 600_000)
MIN_TIME = 0.2
REPEAT = 5


# --------------------------------------------------------------------------
# workloads, resolved inside the worker process
# --------------------------------------------------------------------------


def _load_offsets(impl: str) -> Callable[[List[int]], Any]:
    if impl in ("python", "pypy"):
        from faust.utils.functional import _py_first_consecutive_run

        return _py_first_consecutive_run
    if impl == "cython":
        from faust.utils._cython.functional import first_consecutive_run

        return first_consecutive_run
    if impl == "rust":
        from faust._rust import _accel

        # The best Rust can do: raw ffi + the CPython macros, no abi3.
        return getattr(
            _accel,
            "first_consecutive_run_macro",
            _accel.first_consecutive_run,
        )
    raise ValueError(impl)


def _load_windows(impl: str) -> Callable[[], Any]:
    if impl in ("python", "pypy"):
        from faust.windows import _PyHoppingWindow as Window
    elif impl == "cython":
        from faust._cython.windows import HoppingWindow as Window
    elif impl == "rust":
        from faust._rust._accel import HoppingWindow as Window
    else:
        raise ValueError(impl)
    return Window(60.0, 10.0, 3600.0)


# --------------------------------------------------------------------------
# worker: one implementation, one workload, one process
# --------------------------------------------------------------------------


def _peak_rss_kb() -> int:
    """Peak RSS of this process in KiB (monotonic, so deltas are safe)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


def _time_ns(fn: Callable[[], Any]) -> float:
    # PyPy needs the JIT warmed before the timed run or the first numbers are
    # interpreted rather than compiled.
    for _ in range(1000):
        fn()
    number = 1
    while timeit.timeit(fn, number=number) < MIN_TIME:
        number *= 4
    best = min(timeit.timeit(fn, number=number) for _ in range(REPEAT))
    return best / number * 1e9


def _worker(impl: str, workload: str, size: int, mode: str) -> Dict[str, Any]:
    if workload == "offsets":
        run = _load_offsets(impl)
        baseline = _peak_rss_kb()
        data = list(range(size))
        after_input = _peak_rss_kb()

        def call() -> Any:
            return run(data)

    elif workload == "windows":
        window = _load_windows(impl)
        baseline = _peak_rss_kb()
        after_input = baseline
        timestamp = 1_700_000_000.123

        def call() -> Any:
            return window.ranges(timestamp)

    else:
        raise ValueError(workload)

    out: Dict[str, Any] = {"impl": impl, "workload": workload, "size": size}
    if mode == "cpu":
        out["ns"] = _time_ns(call)
    else:
        # Keep the results alive across the reading, so the peak includes
        # what the call produced rather than just what it was given.
        held = [call() for _ in range(3)]
        out["baseline_kb"] = baseline
        out["input_kb"] = after_input - baseline
        out["peak_kb"] = _peak_rss_kb()
        out["result_kb"] = out["peak_kb"] - after_input
        out["held"] = len(held)
    return out


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------


def _interpreter_for(impl: str) -> Optional[str]:
    if impl == "pypy":
        return shutil.which("pypy3")
    return sys.executable


def _run_one(
    impl: str, workload: str, size: int, mode: str
) -> Tuple[Optional[Dict[str, Any]], str]:
    interpreter = _interpreter_for(impl)
    if interpreter is None:
        return None, "pypy3 not on PATH"
    # PyPy has no editable install of faust, so put the repo on its path.
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [REPO, env.get("PYTHONPATH", "")]))
    proc = subprocess.run(
        [
            interpreter,
            os.path.abspath(__file__),
            "--worker",
            "--impl",
            impl,
            "--workload",
            workload,
            "--size",
            str(size),
            "--mode",
            mode,
        ],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
    )
    for line in proc.stdout.splitlines():
        if line.startswith("{"):
            return json.loads(line), ""
    detail = (proc.stderr or proc.stdout).strip().splitlines()
    return None, detail[-1][:80] if detail else f"exit {proc.returncode}"


def _versions() -> Dict[str, str]:
    versions = {"python": f"CPython {sys.version.split()[0]}"}
    versions["cython"] = versions["python"]
    versions["rust"] = versions["python"]
    pypy = shutil.which("pypy3")
    if pypy:
        out = subprocess.run(
            [
                pypy,
                "-c",
                "import sys; print(sys.version.split()[0], sys.pypy_version_info[:3])",
            ],
            capture_output=True,
            text=True,
        ).stdout.split()
        if out:
            versions["pypy"] = f"PyPy (Python {out[0]})"
    return versions


def _artifact_sizes() -> List[Tuple[str, str, float]]:
    """On-disk size of each accelerator, which is a real deployment cost."""
    candidates = [
        ("cython", "faust/utils/_cython/functional"),
        ("cython", "faust/_cython/windows"),
        ("rust", "faust/_rust/_accel"),
    ]
    found = []
    for label, stem in candidates:
        for suffix in (".so", ".abi3.so", ".cpython-311-x86_64-linux-gnu.so"):
            path = os.path.join(REPO, stem + suffix)
            if os.path.exists(path):
                found.append((label, stem.split("/")[-1], os.path.getsize(path) / 1024))
                break
    return found


def _ratios(results: Dict[str, float]) -> str:
    """Speed relative to Cython, which is the thing that ships today."""
    if "cython" not in results:
        return "-"
    base = results["cython"]
    return " ".join(
        f"{impl}={base / results[impl]:.2f}x"
        for impl in IMPLEMENTATIONS
        if impl in results and impl != "cython"
    )


def _table(title: str, header: List[str], rows: List[List[str]]) -> None:
    widths = [
        max(len(str(header[i])), max((len(str(r[i])) for r in rows), default=0))
        for i in range(len(header))
    ]
    print(f"\n{title}")
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(header)))
    print("-" * (sum(widths) + 2 * (len(widths) - 1)))
    for row in rows:
        print("  ".join(str(c).ljust(widths[i]) for i, c in enumerate(row)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--impl")
    parser.add_argument("--workload")
    parser.add_argument("--size", type=int, default=0)
    parser.add_argument("--mode", default="cpu")
    args = parser.parse_args()

    if args.worker:
        print(json.dumps(_worker(args.impl, args.workload, args.size, args.mode)))
        return 0

    versions = _versions()
    print("Interpreters and builds:")
    for impl in IMPLEMENTATIONS:
        print(f"  {impl:>7}: {versions.get(impl, 'not available')}")

    skipped: Dict[str, str] = {}

    # ---- CPU: offsets ----
    rows = []
    for size in OFFSET_SIZES:
        row = [f"{size:,}"]
        results = {}
        for impl in IMPLEMENTATIONS:
            result, why = _run_one(impl, "offsets", size, "cpu")
            if result is None:
                skipped.setdefault(impl, why)
                row.append("-")
            else:
                results[impl] = result["ns"]
                row.append(f"{result['ns'] / 1000:,.1f} us")
        row.append(_ratios(results))
        rows.append(row)
    _table(
        "CPU -- first_consecutive_run (offset commit scan), lower is better",
        ["offsets", "python", "pypy", "cython", "rust", "vs cython"],
        rows,
    )

    # ---- CPU: windows ----
    rows = []
    row = ["HoppingWindow.ranges"]
    results = {}
    for impl in IMPLEMENTATIONS:
        result, why = _run_one(impl, "windows", 0, "cpu")
        if result is None:
            skipped.setdefault(impl, why)
            row.append("-")
        else:
            results[impl] = result["ns"]
            row.append(f"{result['ns']:,.0f} ns")
    row.append(_ratios(results))
    rows.append(row)
    _table(
        "CPU -- HoppingWindow.ranges (per call), lower is better",
        ["case", "python", "pypy", "cython", "rust", "vs cython"],
        rows,
    )

    # ---- Memory ----
    rows = []
    for size in (100_000, 600_000):
        for impl in IMPLEMENTATIONS:
            result, why = _run_one(impl, "offsets", size, "mem")
            if result is None:
                skipped.setdefault(impl, why)
                continue
            rows.append(
                [
                    f"{size:,}",
                    impl,
                    f"{result['baseline_kb'] / 1024:,.1f}",
                    f"{result['input_kb'] / 1024:,.1f}",
                    f"{result['peak_kb'] / 1024:,.1f}",
                ]
            )
    _table(
        "Memory -- fresh process per row, peak RSS in MiB",
        ["offsets", "impl", "after import", "input list", "peak"],
        rows,
    )

    artifacts = _artifact_sizes()
    if artifacts:
        _table(
            "Artifact size on disk (KiB)",
            ["kind", "module", "size"],
            [[k, m, f"{s:,.0f}"] for k, m, s in artifacts],
        )

    if skipped:
        print("\nSkipped:")
        for impl, why in skipped.items():
            print(f"  {impl}: {why}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
