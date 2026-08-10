"""The internal read of ``cython_optimizations`` must survive deprecation.

The setting is transitional and expected to be deprecated and then removed.
That plan has a trap in it: ``Param.__get__`` emits a ``UserWarning`` on every
read once ``version_deprecated`` is set, and faust reads this setting itself --
once per stream, and once per assigned partition.  Deprecating it naively would
make faust warn at itself, repeatedly, about a setting the user probably never
set.

These tests pin the two halves of the contract:

* internal reads (via ``cython_optimizations_enabled``) stay silent, and
* user-facing reads (``app.conf.cython_optimizations``) still warn, because
  warning is the entire point of deprecating a setting.
"""

import warnings

import pytest

from faust.utils.optin import cython_optimizations_enabled


@pytest.fixture()
def param(app):
    """The ``cython_optimizations`` Param descriptor."""
    return type(app.conf).SETTINGS["cython_optimizations"]


@pytest.fixture()
def deprecated(param):
    """Mark the setting deprecated for the duration of a test."""
    saved = (param.version_deprecated, param.deprecation_reason)
    param.version_deprecated = "0.99.0"
    param.deprecation_reason = "the fast paths are now unconditional"
    try:
        yield param
    finally:
        param.version_deprecated, param.deprecation_reason = saved


def test_matches_the_public_read(app) -> None:
    assert cython_optimizations_enabled(app.conf) is app.conf.cython_optimizations


def test_default_is_false(app) -> None:
    assert cython_optimizations_enabled(app.conf) is False


@pytest.mark.conf(cython_optimizations=True)
def test_reads_true_when_enabled(app) -> None:
    assert cython_optimizations_enabled(app.conf) is True


def test_internal_read_is_silent_when_deprecated(app, deprecated) -> None:
    """The whole reason this helper exists."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        value = cython_optimizations_enabled(app.conf)

    assert value is False
    ours = [w for w in caught if "cython_optimizations" in str(w.message)]
    assert not ours, (
        f"reading the setting from inside faust warned: "
        f"{[str(w.message) for w in ours]}.  faust reads this per stream and "
        f"per partition, so a deprecation would flood logs with a warning "
        f"the user cannot act on."
    )


def test_public_read_still_warns_when_deprecated(app, deprecated) -> None:
    """The helper must not disarm the deprecation for users.

    Without this, a future deprecation could be silently ineffective -- which
    is worse than noisy, because nobody would ever be told to stop using it.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        app.conf.cython_optimizations

    ours = [w for w in caught if "cython_optimizations" in str(w.message)]
    assert ours, "deprecating the setting no longer warns users who read it"
    assert "deprecated" in str(ours[0].message)


def test_extensions_are_silent_when_deprecated(app, deprecated) -> None:
    """The end-to-end version: neither extension warns per construction.

    The helper is only useful if the extensions actually go through it.  This
    builds the objects that read the setting -- one per stream, one per
    assigned partition -- and asserts the deprecation stays quiet.
    """
    from faust.streams import _CStreamIterator
    from faust.transport.conductor import Conductor, ConductorHandler
    from faust.types import TP

    if _CStreamIterator is None or ConductorHandler is None:
        pytest.skip("extensions not built in place")

    conductor = Conductor(app)
    topic = app.topic("foo")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        for _ in range(3):
            _CStreamIterator(app.stream(app.channel()))
        for i in range(3):
            ConductorHandler(conductor, TP("foo", i), {topic})

    ours = [w for w in caught if "cython_optimizations" in str(w.message)]
    assert not ours, (
        f"{len(ours)} deprecation warnings from 3 stream iterators and 3 "
        f"conductor handlers.  A real worker builds one of each per stream and "
        f"per assigned partition, so this scales with the deployment."
    )
