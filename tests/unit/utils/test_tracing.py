"""Tracing helpers, and the rebalance path that runs through them.

``tests/unit/utils/test_opentracing_shim.py`` covers the no-op stand-in's
parity with the real library.  This covers what sits above it: that
``traced_from_parent_span`` survives a parent span it cannot trace from, and
that both rebalance callbacks complete with the stand-in substituted for the
real library -- the configuration reported in #786, and the one the parity
tests explicitly do not reach.
"""

import pytest

from faust.types import TP
from faust.utils import _opentracing as shim
from faust.utils.tracing import noop_span, set_current_span, traced_from_parent_span
from tests.helpers import AsyncMock


@pytest.fixture(autouse=True)
def _reset_current_span():
    # ``traced_from_parent_span`` leaves the parent span current.
    yield
    set_current_span(None)


@pytest.fixture()
def without_opentracing(monkeypatch):
    """Make Faust build no-op spans as it does without the extra installed."""
    monkeypatch.setattr("faust.utils.tracing.opentracing", shim)


class TestTracedFromParentSpan:
    def test_runs_untraced_when_the_parent_has_no_tracer(self):
        # A span is whatever the configured tracer hands back, and a child can
        # only come from the parent's own tracer.  Tracing is instrumentation:
        # it must not be the thing that breaks the call.
        @traced_from_parent_span(shim.Span(tracer=None))
        def double(x):
            return x * 2

        assert double(21) == 42

    def test_runs_untraced_when_the_current_span_has_no_tracer(self):
        # Same, reached through the context variable rather than an argument.
        set_current_span(shim.Span(tracer=None))

        @traced_from_parent_span()
        def double(x):
            return x * 2

        assert double(21) == 42

    def test_runs_untraced_with_no_parent_span_at_all(self):
        @traced_from_parent_span()
        def double(x):
            return x * 2

        assert double(21) == 42

    @pytest.mark.parametrize(
        "parent",
        [
            pytest.param(shim.Span(tracer=None), id="untraced"),
            pytest.param(shim.Tracer()._noop_span, id="traced"),
        ],
    )
    def test_propagates_the_wrapped_functions_error(self, parent):
        @traced_from_parent_span(parent)
        def raiser():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            raiser()


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
