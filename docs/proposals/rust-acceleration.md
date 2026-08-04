# Evaluation: a feature-flagged Rust accelerator build for Faust

Status: **evaluation only — no build changes are proposed for merge by this
document.**
Scope: whether Faust should grow an optional, feature-flagged Rust extension
alongside (or eventually instead of) its Cython extensions.

**Recommendation in one line:** do not add a Rust build axis to Faust today.
The prototype works and the build integration is sound, but on the code Faust
actually accelerates, Rust is between 1.9x faster and 15% *slower* than the
Cython we already ship, and it would roughly double the test matrix and add a
second native toolchain for that. Prefer keeping Cython, and revisit only if a
genuinely batch-shaped hot path appears (see
[What would change this answer](#what-would-change-this-answer)).

Everything below was measured or built, not estimated; see
[Reproducing](#reproducing).

---

## 1. What "a feature-flagged build" would have to mean here

Faust already has a working accelerator feature-flag pattern, and any Rust work
has to fit it rather than replace it:

| Layer | Mechanism today | Location |
| --- | --- | --- |
| Build flag | `USE_CYTHON` / `NO_CYTHON` env vars | `setup.py:12-21` |
| Graceful build failure | `ve_build_ext` raises `BuildFailed`, `do_setup()` retried with no `ext_modules` | `setup.py:103-122`, `setup.py:216-222` |
| Runtime flag | `NO_CYTHON` env var re-read at import, plus `try: import ... except ImportError` fallback | `faust/streams.py:57-65`, `faust/windows.py:78-92`, `faust/transport/conductor.py:37-45` |
| Packaging extra | `faust[cython]`, rolled into `faust[fast]` | `requirements/extras/cython.txt`, `fast.txt` |
| Visibility | worker banner prints `+ Cython (compiler)` | `faust/cli/worker.py:156-163` |
| CI | `use-cython: ['true', 'false']` axis across 5 Pythons | `.github/workflows/python-package.yml` |

A Rust build must reproduce **all six** rows, not just the first. A flag that
only gates compilation, without the import guard, the extra, the banner line
and the CI axis, produces a build that silently differs from what is tested.

Worth noting up front: Faust *already ships Rust*, just not its own. Two
optional dependencies are Rust extension modules — `orjson` (declares
`Programming Language :: Rust`) and `rocksdict` (links `pyo3-0.27.1`). Both
arrive as prebuilt wheels and cost the project nothing. That is the cheap way
to consume Rust, and it is already being used.

## 2. The prototype that was built and verified

A working `USE_RUST` build was assembled against a clone of this repo (not on
this branch). It is small — this is the whole of it:

**`setup.py`** (after the existing `cythonize` block):

```python
def _flag(name, default=""):
    v = os.environ.get(name, default)
    return bool(v) and str(v).lower() not in {"0", "false", "no", "off"}

USE_RUST = _flag("USE_RUST")
rust_extensions = []
if USE_RUST:
    try:
        from setuptools_rust import Binding, RustExtension
    except ImportError:
        print("---*--- USE_RUST set but setuptools-rust missing: SKIPPING ---*---")
    else:
        print("---*--- USING RUST ---*---")
        rust_extensions = [
            RustExtension(
                "faust._rust._accel",
                path="faust/_rust/Cargo.toml",
                binding=Binding.PyO3,
                py_limited_api=True,
                optional=True,   # a cargo failure must not fail the install
            )
        ]
```

…passed through as `rust_extensions=rust_extensions` in `do_setup()`, plus
`"setuptools-rust>=1.9"` in `[build-system] requires`, plus
`faust/_rust/{Cargo.toml,src/lib.rs}` holding a PyO3 0.29 port of
`faust._cython.windows.HoppingWindow` (`abi3-py310`, `crate-type = ["cdylib"]`).

Verified behaviours:

* `USE_CYTHON=1 USE_RUST=1 pip install .` builds **both** accelerators in one
  pass; `faust._rust._accel` imports and returns results identical to the
  Python and Cython implementations for `ranges`, `current`, `stale`,
  `earliest`.
* `py_limited_api=True` really does produce a stable-ABI object
  (`_accel.abi3.so`), so *one* Rust build covers CPython 3.10–3.14.
* With `cargo` removed from `PATH` and `USE_RUST=1` still set, `pip install .`
  **succeeds** and simply omits the module — `optional=True` gives Rust the
  same graceful degradation `ve_build_ext` gives Cython. Without
  `optional=True` this install aborts, which would be a regression for
  source installs.
* Default (`USE_RUST` unset) builds are byte-for-byte unaffected and need no
  Rust toolchain.

Three integration details that are easy to miss and that any real PR must
handle:

1. **`MANIFEST.in` excludes the Rust sources.** `recursive-include faust *.py
   *.typed *.pyx` does not match `*.rs`, `Cargo.toml` or `Cargo.lock`, so the
   sdist would ship without them and `USE_RUST=1` would be a no-op for anyone
   installing from source. Needs an explicit include.
2. **`[build-system] requires` cannot be conditional.** PEP 518 has no env
   switch, so `setuptools-rust` becomes an unconditional build dependency for
   *everyone*, including the 99% who never set `USE_RUST`. It is pure Python
   and small, but it is a new mandatory build-time download.
3. **`optional=True` fails silently.** A user who asks for `USE_RUST=1` and
   gets no acceleration receives no signal at all. This is why the worker
   banner row in the table above is not optional — a `_human_rust_info()`
   sibling of `_human_cython_info()` (`faust/cli/worker.py:156`) is the only
   way to tell the two builds apart at runtime.

## 3. Measurements

Linux x86_64, CPython 3.11, rustc 1.94.1, PyO3 0.29 (`--release`), Cython
extensions built with the project's standard `-O2`. `min` of 5 runs ×
200 000 iterations, ns/call. Reproduce with
`extra/tools/bench_accel_windows.py`.

### 3.1 `HoppingWindow`, the one accelerator with a like-for-like Rust port

| case | python | cython | rust | cython vs py | rust vs py | **rust vs cython** |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `ranges(ts)` | 2045 ns | 1012 ns | 592 ns | 2.02x | 3.46x | **1.71x faster** |
| `current(ts)` | 425 ns | 151 ns | 174 ns | 2.81x | 2.44x | **0.87x — slower** |
| `stale(ts, ts+1)` | 559 ns | 136 ns | 157 ns | 4.11x | 3.57x | **0.87x — slower** |
| `earliest(ts)` | 357 ns | 141 ns | 170 ns | 2.53x | 2.10x | **0.83x — slower** |

(An earlier run of the same benchmark put `ranges` at 1.91x and the three
scalar methods at 0.87–0.91x; run-to-run spread is a few percent, the sign is
stable.)

The pattern is the important part, and it is not about Rust being slow:

* **The win is proportional to work done per call.** `ranges` builds a list of
  ~7 tuples and Rust wins 1.7x. The three methods that do a couple of
  floating-point operations and return one tuple all *lose* to Cython.
* **Because the floor is the call boundary, not the language.** A bare
  attribute read costs **60 ns through Cython and 79 ns through PyO3** —
  PyO3's argument parsing, `Bound` handling and error plumbing are simply
  thicker than `cdef class` access. Any call whose body is cheaper than ~30 ns
  of that difference is a guaranteed loss, whatever the language.

### 3.2 Build cost

| build | wall time |
| --- | ---: |
| `pip install .` with `USE_CYTHON=1` | 12.5 s |
| `pip install .` with `USE_CYTHON=1 USE_RUST=1`, cold `CARGO_HOME` | 29.2 s |
| the Rust crate alone (`maturin build --release`, cold) | 21 s |

So ~+17 s per cold build for a crate containing one 80-line struct — that is
almost entirely compiling the PyO3 macro stack (`syn`, `quote`,
`proc-macro2`, `pyo3-macros`), and it is a fixed cost that does not grow much
as Faust's own Rust grows. It is paid per CI leg, not once.

## 4. Which Faust code could actually go to Rust

| Candidate | Shape | Verdict |
| --- | --- | --- |
| `faust/_cython/windows.pyx` (109 lines) | Pure float math, no Python objects held | **Portable, mostly not worth it.** The port exists and is correct; §3.1 shows it wins only on `ranges`. |
| `faust/_cython/streams.pyx` (198 lines) | `async def next()`, awaits `chan_slow_get`, `maybe_async`, sensor callbacks | **Poor fit.** The body is an await-driven sequence of calls back into Python; PyO3 needs `pyo3-async-runtimes` to express it and pays the 79 ns boundary on *every* callback it drives. The Cython version wins here precisely because `cdef class` attribute access is cheap, which is the one thing Rust is worse at. |
| `faust/transport/_cython/conductor.pyx` (134 lines) | Same — async dispatch, all work is calling Python | **Poor fit**, same reason. |
| JSON encode/decode (`faust/utils/json.py`) | Genuinely batch-shaped | **Already solved** by `faust[orjson]`, which is Rust. Writing our own would be strictly worse. |
| ISO-8601 parsing (`faust/utils/_iso8601_python.py`) | Batch-shaped, small | **Already solved** by `faust[ciso8601]`. |
| State store | Batch-shaped, big | **Already solved** by `faust[rocksdict]`, which is PyO3. |
| Codec chains (`faust/serializers/codecs.py`) | Thin wrappers over `json`/`pickle`/`base64` | No meaningful compute of our own to move. |
| Model field coercion (`faust/models/`) | Plausible on paper | Deeply coupled to `typing` introspection and user-supplied Python callables; would cross the boundary constantly. Not evaluated further. |

The summary of that table is the crux of the evaluation: **every part of Faust
whose shape suits Rust is already served by someone else's Rust**, and the
three things Faust accelerates itself are all latency-bound call-boundary code,
which is the one workload where Cython beats PyO3.

## 5. What it would cost

**CI matrix.** `test-pytest` runs 5 Pythons × `use-cython: [true, false]` = 10
legs, plus 5 confluent legs and PyPy. A `use-rust` axis makes it 20, each also
paying the cargo build from §3.2 — and a Rust build that is never exercised
with `USE_RUST=1` in CI is worse than no Rust at all, because it would ship
untested.

**Wheels.** `build_wheels` runs 4 runner images × cp310–cp314. Each needs Rust
inside the manylinux container (`before-all = "curl -sSf https://sh.rustup.rs |
sh -s -- -y"` plus a `PATH` entry in `environment`), and cargo re-runs per
Python version even though the `abi3` output is identical — roughly +6 minutes
across the release job. Tolerable; it is the test matrix, not the wheel job,
that hurts.

**The abi3 upside does not materialise.** `abi3-py310` means one Rust build
serves every supported CPython — but only a package whose *every* extension is
`abi3` can ship one wheel per platform. Faust's Cython extensions are
version-specific, so wheels stay per-version and the stable-ABI win is
theoretical until Rust *replaces* Cython. That is a much larger project than a
feature flag, and §3.1 says it would make three of four window methods slower.

**Platforms.** `[tool.cibuildwheel] skip` already drops musllinux and
free-threaded builds. Rust is neutral-to-positive here (PyO3 supports
free-threading; the Cython extension is the reason `cp31?t-*` is skipped), but
that is a benefit only for a replacement, not an addition. PyPy already runs
pure-Python (`USE_CYTHON: 'false'`), so a Rust module would simply be absent —
no new work.

**A third copy of every accelerated semantic.** The Python and Cython window
implementations have *already* drifted: `_PyHoppingWindow(60, 10).expires` is
`None` while `faust._cython.windows.HoppingWindow(60, 10).expires` is `0.0`.
No test asserts the two implementations agree — the CI matrix flips
`USE_CYTHON` globally and runs the same tests, so a divergence only surfaces if
a test happens to touch it. Adding a third implementation triples that exposure
against a test suite that does not currently check for it.

**Supply chain and maintenance.** ~14 transitive crates for a hello-world PyO3
module, a `Cargo.lock` to keep current, an MSRV to track (Rust ≥1.64 pins
manylinux2014 as the floor — fine today), and a contributor base that must now
include someone who reads Rust. For a project this size that last one is the
real cost.

## 6. What would change this answer

Concrete triggers, in rough order of likelihood:

1. **A batch-shaped hot path appears in Faust's own code** — something that
   crosses the boundary once and then does thousands of operations
   (windowed-table range scans over many keys, bulk changelog
   reconstruction on recovery, a partition-wide sort/merge). §3.1 shows the
   win scales with work-per-call; `ranges`, the most batch-like of four
   methods, is the only one that wins.
2. **Profiling shows the Cython extensions are actually material.** Nobody has
   published what fraction of Faust's per-message cost lives in
   `streams.pyx` + `conductor.pyx`. If it is small, the entire accelerator
   question — Rust or Cython — is the wrong thing to optimise. This is the
   cheapest next step and should precede any language decision.
3. **Free-threaded CPython becomes a target.** If dropping the `cp31?t-*` skip
   matters, a `Py_GIL_DISABLED`-safe accelerator is needed and PyO3 is a
   better starting point than auditing the `.pyx` files. That argues for
   *replacement*, and would need the §3.1 regressions solved first.
4. **Cython becomes a liability** — a Python release it does not support in
   time, or the `cp3*`/free-threading friction already visible in
   `pyproject.toml`'s skip comment.

If (1) or (3) lands, the §2 prototype is the implementation: it is about 40
lines of build wiring, it degrades correctly, and it has been shown to work.

## 7. Reproducing

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e .                                  # builds the Cython extensions
python extra/tools/bench_accel_windows.py         # python vs cython (vs rust, if built)
```

The benchmark skips any implementation it cannot import, so it is useful on a
pure-Python install too. To reproduce the `rust` column, apply the §2 wiring,
port `faust/_cython/windows.pyx`'s `HoppingWindow` to
`faust/_rust/src/lib.rs`, and build with `USE_RUST=1 pip install -e .` — the
benchmark picks up `faust._rust._accel` automatically.
