.. _developers-free-threading:

==========================================
 Free-threaded Python (PEP 703)
==========================================

.. contents::
    :local:
    :depth: 2

.. _free-threading-status:

Status
======

Faust builds, installs and passes its full unit and functional suite on the
free-threaded builds of CPython 3.13 (``3.13t``) and 3.14 (``3.14t``) with the
GIL genuinely disabled, and ``[tool.cibuildwheel]`` publishes ``cp313t`` and
``cp314t`` wheels.  The ``free-threaded`` CI job covers both interpreters and
gates merges.

What that does **not** mean is that Faust becomes multi-threaded.  Faust's
concurrency model is :mod:`asyncio`: agents, streams and the conductor all run
as tasks on a single event loop, and a loop runs one task at a time whether or
not the interpreter has a GIL.  Removing the GIL does not make any of that run
in parallel.  Treat free-threading support as *"Faust runs correctly on a
free-threaded interpreter"* -- which matters if the rest of your application
wants the no-GIL build -- and not as a throughput feature.

.. _free-threading-declaration:

Why the ``.pyx`` files declare ``freethreading_compatible``
===========================================================

A free-threaded interpreter re-enables the GIL, for the whole process, the
moment it imports an extension module that does not declare
``Py_mod_gil = Py_MOD_GIL_NOT_USED``.  It reports this with a
:exc:`RuntimeWarning` and nothing else -- the import succeeds, the program
runs, and free-threading is simply gone:

.. sourcecode:: text

    RuntimeWarning: The global interpreter lock (GIL) has been enabled to load
    module 'faust._cython.windows', which has not declared that it can run
    safely without the GIL.

All three extension modules therefore set the directive that makes Cython emit
that slot:

.. sourcecode:: cython

    # cython: language_level=3
    # cython: freethreading_compatible=True

Two things make this easy to lose silently, and both are pinned down
deliberately:

* The directive only exists in **Cython 3.1 and later**.  Older Cython
  *ignores* unknown directives rather than failing, so building with Cython
  3.0 produces extensions with no declaration and no diagnostic.  Hence the
  ``cython>=3.1`` floor for Python 3.13+ in ``[build-system].requires`` and the
  ``before-build`` pin in ``[tool.cibuildwheel]``.

* Nothing fails when the declaration is missing.  Hence
  :file:`tests/unit/test_free_threading.py`, which imports each extension in a
  subprocess and asserts the GIL is still off afterwards.  It skips entirely on
  a normal interpreter, so it costs nothing on the GIL builds.

The extensions qualify for the declaration because none of them keep mutable
state at C level: :file:`windows.pyx` holds ``cdef`` doubles that are written
once in ``__init__`` and only read afterwards, and :file:`streams.pyx` and
:file:`conductor.pyx` hold per-instance references to Python objects, with all
shared state living in ordinary Python containers that CPython locks
internally.

.. _free-threading-aiokafka:

Known limitation: aiokafka re-enables the GIL
=============================================

Faust's own extensions are clean, but ``aiokafka`` -- a core dependency --
ships Cython extensions that have not made the declaration.  Importing
:mod:`faust` is fine; the GIL comes back when the transport driver is
resolved, which is to say when a worker starts:

.. sourcecode:: pycon

    >>> import faust, sys
    >>> sys._is_gil_enabled()
    False
    >>> app = faust.App('probe', broker='kafka://localhost:9092')
    >>> sys._is_gil_enabled()
    False
    >>> app.transport          # loads aiokafka.record._crecords
    >>> sys._is_gil_enabled()
    True

So a real worker on the default ``aiokafka`` transport runs *with* a GIL today,
regardless of anything Faust does.  This is an upstream fix, not one Faust can
make.  ``PYTHON_GIL=0`` overrides the re-enabling if you want to run without a
GIL anyway -- which is what the CI job does -- but that is an assertion that
``aiokafka``'s extensions are thread-safe, and nobody has verified that.

.. _free-threading-races:

Latent races that free-threading would expose
=============================================

Because everything runs on one event loop, Faust relies in places on
read-modify-write sequences that are not atomic.  The event loop serializes
them today, so they are not bugs in normal use, but they *are* the code that
would break first if any of it were ever driven from more than one thread.

The clearest example is message reference counting, in
:meth:`faust.types.tuples.Message.ack`:

.. sourcecode:: python

    def ack(self, consumer, n: int = 1) -> bool:
        if not self.acked:                      # check ...
            if not self.decref(n):              # ... then act
                return self.on_final_ack(consumer)
        return False

    def decref(self, n: int = 1) -> int:
        refcount = self.refcount = max(self.refcount - n, 0)   # not atomic
        return refcount

With 16 threads acking the same message on a free-threaded interpreter, the
decrements are lost and the final ack -- the one that marks the offset
safe-to-commit -- either fires more than once or never fires at all.  The same
sequence is duplicated in :file:`faust/_cython/streams.pyx` (``after()``),
:file:`faust/streams.py` and :file:`faust/transport/consumer.py`.

This is reachable from public API: :meth:`faust.Event.ack` is documented for
callers to use, and nothing stops a user calling it from a thread.  It is
*not* reachable from Faust's own code paths, all of which ack from the event
loop.  Fixing it means either a lock on the ack path -- which is hot, and would
cost every single-threaded user -- or documenting that acking is event-loop-only.
That decision is deliberately left open; it is recorded here so it is not
rediscovered from scratch.

.. _free-threading-dev-env:

Working on a free-threaded interpreter
======================================

.. sourcecode:: console

    $ uv python install 3.14t
    $ uv venv --python 3.14t
    $ uv pip install -r requirements/freethreading.txt 'Cython>=3.1' setuptools setuptools_scm

Use :file:`requirements/freethreading.txt`, not :file:`requirements/test.txt`:
parts of the latter cannot be built on a free-threaded interpreter at all
(``twine`` pulls in ``cffi``, which refuses to build on 3.13t; ``hypothesis``
6.130+ ships a PyO3 extension that does not support 3.13t either).  That file
documents each omission.

Then build the extensions **in place**:

.. sourcecode:: console

    $ USE_CYTHON=1 python setup.py build_ext --inplace
    $ PYTHON_GIL=0 python -m pytest tests/unit tests/functional

The in-place build is not optional if you mean to test the compiled code.
:program:`pytest` runs from the repository root, so ``import faust`` resolves to
the source tree, and ``faust/streams.py`` imports its accelerated
implementation behind a ``try: ... except ImportError``:

.. sourcecode:: python

    if not NO_CYTHON:
        try:
            from ._cython.streams import StreamIterator as _CStreamIterator
        except ImportError:
            _CStreamIterator = None

With no ``.so`` next to the ``.pyx``, that import fails, the fallback engages
silently, and the run exercises the pure-Python path no matter what
``USE_CYTHON`` was set to during ``pip install``.  This applies to the
``USE_CYTHON: true`` legs of the main test matrix as well, which is why the
free-threaded job builds in place explicitly.
