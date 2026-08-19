"""Parity tests for the no-op :pypi:`opentracing` stand-in.

Three kinds of check, none sufficient alone: surface comparison catches a name
appearing or disappearing, value comparison catches a mirrored constant
drifting, and functional calls catch a name that exists but holds the wrong
value -- the bug that broke rebalancing, where ``Span.tracer`` existed and was
``None``.

These exercise the shim's objects directly, so they do not prove the
``except ImportError`` fallback wiring itself.
"""

import opentracing
import pytest
from opentracing.ext import tags as real_tags

from faust.utils import _opentracing as shim
from faust.utils.tracing import current_span, set_current_span, traced_from_parent_span

SPAN_SURFACE = frozenset(
    {
        "tracer",
        "context",
        "finish",
        "set_tag",
        "set_operation_name",
        "log_kv",
        "__enter__",
        "__exit__",
    }
)

SPAN_ABSENT = frozenset({"operation_name"})

# ``start_active_span`` is excluded: the shim omits it by design.
TRACER_SURFACE = frozenset({"start_span", "extract", "inject", "_noop_span"})

# ``tags`` is excluded: the real one lives at ``opentracing.ext.tags``.
MODULE_SURFACE = frozenset(
    {
        "tracer",
        "follows_from",
        "child_of",
        "start_child_span",
        "Format",
        "Span",
        "Tracer",
    }
)

MODULES = [pytest.param(opentracing, id="real"), pytest.param(shim, id="shim")]

SURFACES = [
    pytest.param(
        lambda m: m.Tracer().start_span(operation_name="p"), SPAN_SURFACE, id="span"
    ),
    pytest.param(lambda m: m.Tracer(), TRACER_SURFACE, id="tracer"),
    pytest.param(lambda m: m, MODULE_SURFACE, id="module"),
]

CONSTANTS = [
    pytest.param(
        real_tags,
        shim.tags,
        (
            "ERROR",
            "SAMPLING_PRIORITY",
            "SPAN_KIND",
            "COMPONENT",
            "MESSAGE_BUS_DESTINATION",
        ),
        id="tags",
    ),
    pytest.param(
        opentracing.Format,
        shim.Format,
        ("TEXT_MAP", "HTTP_HEADERS", "BINARY"),
        id="format",
    ),
]


def noop_span(module):
    return module.Tracer().start_span(operation_name="parity-probe")


@pytest.fixture(autouse=True)
def restore_current_span():
    yield
    set_current_span(None)


@pytest.mark.parametrize("get, names", SURFACES)
def test_surface_matches_real_library(get, names):
    real, fake = get(opentracing), get(shim)
    for name in names:
        assert hasattr(real, name) == hasattr(fake, name), name


@pytest.mark.parametrize("real, fake, names", CONSTANTS)
def test_constant_values_match_real_library(real, fake, names):
    for name in names:
        assert getattr(real, name) == getattr(fake, name), name


@pytest.mark.parametrize("module", MODULES)
def test_start_span_returns_the_tracers_own_usable_noop_span(module):
    tracer = module.Tracer()
    span = tracer.start_span(operation_name="x")
    assert span is tracer._noop_span
    assert span.tracer.start_span(operation_name="child") is not None


@pytest.mark.parametrize("module", MODULES)
def test_span_hides_operation_name(module):
    for name in SPAN_ABSENT:
        assert not hasattr(noop_span(module), name)


@pytest.mark.parametrize("module", MODULES)
def test_start_child_span_returns_a_span_with_a_tracer(module):
    assert module.start_child_span(noop_span(module), "child-op").tracer is not None


@pytest.mark.parametrize("module", MODULES)
def test_traced_from_parent_span_runs_with_a_noop_parent(module):
    """Both rebalance callbacks hand this decorator a no-op span."""

    @traced_from_parent_span(noop_span(module))
    def work(value):
        return value * 2

    assert work(21) == 42


@pytest.mark.parametrize("module", MODULES)
async def test_traced_from_parent_span_awaits_a_coroutine(module):
    """Production awaits, routing through ``corowrapped`` and ``_restore_span``."""
    parent = noop_span(module)

    @traced_from_parent_span(parent)
    async def work(value):
        return value * 2

    assert await work(21) == 42
    assert current_span() is parent


def test_shim_exit_finishes_the_span():
    finished = []

    class RecordingSpan(shim.Span):
        def finish(self, *args, **kwargs):
            finished.append(True)

    with RecordingSpan(tracer=shim.Tracer()):
        pass

    assert finished == [True]


def test_shim_span_requires_a_tracer():
    with pytest.raises(TypeError):
        shim.Span()


def test_shim_hides_start_active_span():
    assert not hasattr(shim.Tracer, "start_active_span")
