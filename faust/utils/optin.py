"""Reading opt-in settings from faust's own internals.

``cython_optimizations`` is transitional: it exists so the repaired Cython
fast paths can be adopted deliberately rather than arriving in an upgrade, and
it is expected to be deprecated and removed once they are the default (see
``docs/developerguide/cython.rst``).

That plan has a trap in it, which is what this module exists to avoid.
:meth:`faust.types.settings.params.Param.__get__` emits a :exc:`UserWarning`
on **every read** of a setting once ``version_deprecated`` is set on it -- and
faust reads this one itself, once per :class:`~faust.Stream` and once per
assigned partition.  Deprecating the setting would therefore make faust warn
at itself, repeatedly, about a setting the user very likely never set and
cannot act on.

So internal reads go through :func:`cython_optimizations_enabled`, which takes
the stored value rather than the descriptor.  User-facing reads of
``app.conf.cython_optimizations`` are untouched and *should* warn once the
setting is deprecated -- that is the whole point of deprecating it.

Note this deliberately does not use :func:`warnings.catch_warnings` to
suppress the warning instead: that manipulates global state and is not
thread-safe, which matters on the free-threaded builds faust now supports.

When the setting is finally removed, delete this module and the two calls to
it.
"""

from typing import Any

__all__ = ["cython_optimizations_enabled"]


def cython_optimizations_enabled(conf: Any) -> bool:
    """Return whether the repaired Cython fast paths are enabled.

    Arguments:
        conf: The app's :class:`~faust.types.settings.Settings`.

    Reads the value the descriptor stores rather than going through the
    descriptor, so that deprecating the setting does not make every stream and
    every partition assignment emit a warning from inside faust.  The storage
    attribute is looked up through the settings registry rather than
    hard-coded, so renaming the setting cannot silently turn this into a
    read of a non-existent attribute.
    """
    param = type(conf).SETTINGS["cython_optimizations"]
    return bool(getattr(conf, param.storage_name))
