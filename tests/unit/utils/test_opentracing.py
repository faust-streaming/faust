"""The no-op stand-in used when :pypi:`opentracing` is not installed.

``requirements/test.txt`` pulls in the ``opentracing`` extra, so the real
library is always importable here and the ``except ImportError`` fallback in
``faust.utils.tracing`` never fires by accident.  These tests therefore reach
for the stand-in explicitly, and check it two ways: side by side with the real
library, so a divergence shows up as a failing parametrization rather than as
a claim about what the real library does; and substituted for it, so the code
paths that broke without the extra (#786) are actually walked.
"""

import opentracing
import pytest
from opentracing.ext import tags as real_tags

from faust.types import TP
from faust.utils import _opentracing as shim
from faust.utils.tracing import (
    current_span,
    noop_span,
    set_current_span,
    traced_from_parent_span,
)
from tests.helpers import AsyncMock

#: Both implementations, so every parity check runs against the real library
#: too: a test that only the stand-in passes proves nothing about parity.
MODULES = [pytest.param(opentracing, id="real"), pytest.param(shim, id="shim")]


def a_noop_span(module):
    """Build a no-op span the way ``faust.utils.tracing.noop_span`` does."""
    return module.Tracer()._noop_span


@pytest.fixture(autouse=True)
def _reset_current_span():
    # ``traced_from_parent_span`` leaves the parent span current.
    yield
    set_current_span(None)


@pytest.fixture()
def without_opentracing(monkeypatch):
    """Make Faust build no-op spans as it does without the extra installed."""
    monkeypatch.setattr("faust.utils.tracing.opentracing", shim)


class TestSpanParity:
    @pytest.mark.parametrize("module", MODULES)
    def test_noop_span_carries_the_tracer_that_made_it(self, module):
        # The bug behind #786: ``traced_from_parent_span`` starts its child
        # from ``parent.tracer``, so a span whose tracer is None cannot be a
        # parent -- and every rebalance passes a no-op span as one.
        span = a_noop_span(module)
        assert span.tracer is not None
        assert span.tracer.start_span(operation_name="child", child_of=span)

    @pytest.mark.parametrize("module", MODULES)
    def test_start_span_returns_the_tracers_noop_span(self, module):
        tracer = module.Tracer()
        assert tracer.start_span(operation_name="x") is tracer._noop_span

    @pytest.mark.parametrize("module", MODULES)
    def test_noop_span_has_no_operation_name(self, module):
        # ``AIOKafkaConsumerThread`` reads ``span.operation_name`` and catches
        # AttributeError to mean "not a real span"; defining the attribute
        # sends no-op spans down the real-span path, which then reaches for
        # ``_real_finish`` and fails.
        assert not hasattr(a_noop_span(module), "operation_name")

    @pytest.mark.parametrize("module", MODULES)
    def test_span_exit_finishes_the_span(self, module):
        # The driver's lazy spans work by rebinding ``finish``, which only
        # runs if leaving the span calls it.
        finished = []

        class RecordingSpan(module.Span):
            def finish(self, *args, **kwargs):
                finished.append(True)

        tracer = module.Tracer()
        with RecordingSpan(tracer, a_noop_span(module).context):
            pass

        assert finished == [True]

    @pytest.mark.parametrize("module", MODULES)
    def test_start_child_span_hangs_off_the_parents_tracer(self, module):
        parent = a_noop_span(module)
        child = module.start_child_span(parent, "child-op")
        assert child.tracer is not None
        assert child.tracer is parent.tracer

    @pytest.mark.parametrize(
        "real, fake, names",
        [
            pytest.param(
                opentracing.Format,
                shim.Format,
                ("TEXT_MAP", "HTTP_HEADERS", "BINARY"),
                id="Format",
            ),
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
        ],
    )
    def test_mirrored_constants_match(self, real, fake, names):
        assert {n: getattr(fake, n) for n in names} == {
            n: getattr(real, n) for n in names
        }


class TestTracedFromParentSpan:
    @pytest.mark.parametrize("module", MODULES)
    def test_traces_a_sync_function_from_a_noop_parent(self, module):
        @traced_from_parent_span(a_noop_span(module))
        def double(x):
            return x * 2

        assert double(21) == 42

    @pytest.mark.parametrize("module", MODULES)
    @pytest.mark.asyncio
    async def test_traces_a_coroutine_from_a_noop_parent(self, module):
        parent = a_noop_span(module)

        @traced_from_parent_span(parent)
        async def double(x):
            return x * 2

        assert await double(21) == 42
        assert current_span() is parent

    @pytest.mark.parametrize("module", MODULES)
    def test_propagates_the_wrapped_functions_error(self, module):
        @traced_from_parent_span(a_noop_span(module))
        def raiser():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            raiser()

    def test_runs_untraced_when_the_parent_has_no_tracer(self):
        # Not reachable through the stand-in any more, but a span is whatever
        # the configured tracer hands back: instrumentation must not be the
        # thing that breaks the call.
        class TracerlessSpan(shim.Span):
            tracer = None

        @traced_from_parent_span(TracerlessSpan(tracer=None))
        def double(x):
            return x * 2

        assert double(21) == 42

    def test_runs_untraced_with_no_parent_span_at_all(self):
        @traced_from_parent_span()
        def double(x):
            return x * 2

        assert double(21) == 42


class TestRebalanceWithoutOpentracing:
    """Regression tests for #786.

    Both rebalance callbacks trace their work from the span
    ``_start_span_from_rebalancing`` returns, which is a no-op span whenever no
    tracer is configured -- so without the extra installed *every* rebalance
    raised ``AttributeError`` and left the app mid-rebalance.
    """

    def test_noop_span_comes_from_the_stand_in(self, *, without_opentracing):
        assert isinstance(noop_span(), shim.Span)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, app, without_opentracing):
        consumer = app.consumer
        consumer._on_partitions_revoked = AsyncMock(name="_on_partitions_revoked")
        revoked = {TP("foo", 0)}

        await consumer.on_partitions_revoked(revoked)

        consumer._on_partitions_revoked.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, app, without_opentracing):
        consumer = app.consumer
        consumer._on_partitions_assigned = AsyncMock(name="_on_partitions_assigned")
        assigned = {TP("foo", 0)}

        await consumer.on_partitions_assigned(assigned)

        consumer._on_partitions_assigned.assert_called_once_with(assigned, 0)
