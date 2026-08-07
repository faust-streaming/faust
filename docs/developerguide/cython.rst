.. _developers-cython:

==========================================
 The optional Cython accelerators
==========================================

.. contents::
    :local:
    :depth: 2

Faust ships several hot code paths twice: a readable pure-Python
implementation, and a Cython one used instead whenever the extension modules
could be built.  Nothing in Faust requires the extensions -- every accelerated
import falls back:

.. sourcecode:: python

    if not NO_CYTHON:
        try:
            from ._cython.streams import StreamIterator as _CStreamIterator
        except ImportError:
            _CStreamIterator = None

That fallback is what makes the accelerators optional, and it is also the
single biggest hazard in maintaining them.  This page is about the hazard.

.. _cython-testing:

Testing the compiled code
=========================

**The extensions have to be built in place, or the tests do not touch them.**

:program:`pytest` runs from the repository root, so ``import faust`` resolves
to the source tree -- not to whatever ``pip install .`` compiled into
``site-packages``.  With no ``.so`` next to the ``.pyx``, every accelerated
import raises :exc:`ImportError`, the fallback engages, and the whole suite
tests pure Python.  Silently: nothing warns, and the run is green either way.

.. sourcecode:: console

    $ USE_CYTHON=1 python setup.py build_ext --inplace
    $ FAUST_REQUIRE_CYTHON=1 python -m pytest tests/unit tests/functional

``FAUST_REQUIRE_CYTHON=1`` asserts that the accelerators really were loaded,
turning the silent fallback into a failure.  Set it whenever a run is supposed
to be testing the compiled code; the CI legs that build the extensions do.

Without it, a green run proves nothing about the Cython path, and any test
that compares the two implementations degrades into comparing one
implementation against itself.

.. _cython-drift:

Why parity tests exist
======================

Two implementations of the same behaviour drift, and this pair has drifted
repeatedly:

* **#608**, *"Fix cython stream_event_in to match python impl"* -- shipped, and
  fixed only after the fact.

* ``Conductor.on_topic_buffer_full`` passes a channel where a ``TP`` is
  expected, so ``Monitor``'s per-TP counts are wrong.  The comment in
  ``faust/transport/conductor.py`` records that the defect is **deliberately
  left unfixed**, because fixing one twin alone would make the two disagree.
  The duplication turned a small bug into one nobody wants to touch.

* ``StreamIterator._try_get_quick_value`` carried two bugs that concealed each
  other.  ``chan_queue_empty`` holds the bound ``queue.empty`` *method*:

  .. sourcecode:: python

      # streams.py                    # streams.pyx (before)
      if chan_queue_empty():          if self.chan_queue_empty:

  A bound method is always truthy, so the extension always reported "queue
  empty" and took the awaiting path.  That made the ``else`` branch
  unreachable -- which hid the fact that it returned the bare value from
  ``get_nowait()`` instead of the ``(need_slow_get, value)`` pair the caller
  unpacks.  Had the fast path ever run, it would have raised
  :exc:`TypeError`, or silently mis-unpacked a two-element value.

  So the extension quietly did *more* work than the pure-Python code it was
  meant to accelerate, for as long as it has existed.

None of these were caught by a test, because until recently no test ever
imported the compiled modules.

:file:`tests/unit/test_cython_parity.py` covers both halves: it asserts the
accelerators are loaded when they are required, and compares the two
implementations where they can be driven directly.

.. _cython-writing:

Writing an accelerator
======================

The conventions the existing modules follow:

* **Keep the pure-Python implementation.**  It is the reference, it is what
  PyPy and no-compiler installs use, and it is the other half of every parity
  test.  Name it ``_py_<name>`` or ``_Py<Name>`` and export both, so tests can
  reach the two independently.

* **Mirror behaviour rather than approximating it.**  Anything the
  pure-Python version guarantees -- iteration order, what happens when a
  mapping is mutated mid-pass, which exception comes out -- is a guarantee of
  the accelerated one too.

* **Add parity tests in the same change**, parametrised over both
  implementations.  A differential test over randomised inputs is worth more
  than a handful of examples.

* **Measure first, and record it** -- time the accelerator against its twin in
  the same interpreter, and put the numbers in the commit message.  An
  accelerator that does not clearly pay is a second implementation to keep in
  sync forever, in exchange for nothing.  (A shared harness for this,
  ``extra/tools/benchmark_cython.py``, is proposed in #751.)

Not every hot path is worth compiling.  The wins concentrate in code doing
real per-call arithmetic -- the window types are ~4-5x faster compiled.  Code
whose body is mostly ``await`` and calls back into Python gains much less,
because the time is in the awaiting, not the arithmetic.
