"""Guard the free-threading (PEP 703) properties of the compiled extensions.

A free-threaded interpreter re-enables the GIL for the whole process the
moment it imports an extension module that does not declare
``Py_mod_gil = Py_MOD_GIL_NOT_USED``.  It says so only through a
``RuntimeWarning``, which nothing fails on, so the loss is invisible: the
build succeeds, the tests pass, and free-threading is simply gone at runtime.

The three ``.pyx`` files set ``# cython: freethreading_compatible=True``,
which is what makes Cython emit that slot.  Cython <3.1 does not know the
directive and *ignores* it rather than failing, so a build picking up an
older Cython would drop the declaration silently -- hence the
``cython>=3.1`` floor for 3.13+ in ``pyproject.toml``, and hence this test.

Everything here is skipped unless the tests are running on a free-threaded
build with the GIL actually disabled, so the module is a no-op on a normal
CPython (and on the ``USE_CYTHON=false`` legs, where there is nothing to
check).
"""

import importlib.util
import sys

import pytest

#: The extension modules built from ``faust/**/_cython/*.pyx``.
CYTHON_MODULES = [
    "faust._cython.windows",
    "faust._cython.streams",
    "faust.transport._cython.conductor",
]


def _gil_disabled() -> bool:
    # `sys._is_gil_enabled` only exists on 3.13+; on a GIL build it is either
    # absent or always True.
    is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
    return is_gil_enabled is not None and not is_gil_enabled()


#: Applied to every test below: there is nothing to assert unless this is a
#: free-threaded interpreter that still has the GIL off by the time the suite
#: runs.  Note the GIL can be re-enabled by *any* import that happened earlier
#: (a dependency's extension, for instance), which is exactly the condition
#: this file exists to detect -- but it can only be attributed to faust when
#: faust's own modules are the ones being imported, so the checks below import
#: them in a subprocess.
requires_free_threading = pytest.mark.skipif(
    not _gil_disabled(),
    reason="not running on a free-threaded interpreter with the GIL disabled",
)


def _import_in_subprocess(modules: list) -> "tuple":
    """Import `modules` in a fresh interpreter, return (gil_enabled, stderr).

    A subprocess, because the GIL cannot be re-disabled once something has
    switched it back on: by the time the test module is imported the damage
    from any earlier import is already done and unattributable.
    """
    import os
    import subprocess

    code = (
        "import sys\n"
        + "".join(f"import {m}\n" for m in modules)
        + "sys.stdout.write('1' if sys._is_gil_enabled() else '0')\n"
    )

    # Drop PYTHON_GIL from the child's environment.  The free-threaded CI job
    # runs the suite under `PYTHON_GIL=0`, which forces the GIL to stay off
    # even for a module that never declared it was safe -- exactly the thing
    # being tested for.  Inheriting it would make these assertions pass
    # unconditionally, so the child has to see the interpreter's default
    # behaviour: re-enable the GIL, and say so on stderr.
    env = {k: v for k, v in os.environ.items() if k != "PYTHON_GIL"}

    proc = subprocess.run(
        [sys.executable, "-W", "always", "-c", code],
        capture_output=True,
        text=True,
        check=True,
        env=env,
    )
    return proc.stdout.strip() == "1", proc.stderr


@requires_free_threading
@pytest.mark.parametrize("module", CYTHON_MODULES)
def test_extension_does_not_re_enable_gil(module: str) -> None:
    """Importing a faust extension must leave the GIL disabled."""
    if importlib.util.find_spec(module) is None:
        pytest.skip(f"{module} is not built (USE_CYTHON=false)")

    gil_enabled, stderr = _import_in_subprocess([module])

    assert not gil_enabled, (
        f"importing {module} re-enabled the GIL, so the process lost "
        f"free-threading.  The module is missing the "
        f"'# cython: freethreading_compatible=True' directive, or it was "
        f"compiled by a Cython older than 3.1 (which ignores that directive). "
        f"Interpreter said:\n{stderr}"
    )


@requires_free_threading
def test_importing_faust_does_not_re_enable_gil() -> None:
    """`import faust` must leave the GIL disabled.

    Broader than the per-module check above: this also catches a *dependency*
    imported at faust import time that has not declared itself
    free-threading-safe.
    """
    gil_enabled, stderr = _import_in_subprocess(["faust"])

    assert not gil_enabled, (
        f"importing faust re-enabled the GIL.  The warning on stderr names "
        f"the module responsible:\n{stderr}"
    )
