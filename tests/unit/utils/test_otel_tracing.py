"""Unit tests for the native OpenTelemetry tracing core (Phase 1a foundation)."""

import pytest

from faust.utils import otel_tracing

# The whole module is meaningless without the OpenTelemetry API, and its SDK is
# what lets us assert on emitted spans -- skip cleanly when absent.
pytest.importorskip("opentelemetry.sdk.trace")

from opentelemetry import trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind  # noqa: E402


@pytest.fixture
def exporter():
    """Register a global provider backed by an in-memory exporter."""
    exp = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exp))
    # ``set_tracer_provider`` only takes effect once per process; override the
    # internal proxy so every test gets a fresh exporter.
    trace._TRACER_PROVIDER = provider
    yield exp
    exp.clear()


@pytest.fixture
def tracer(exporter):
    return otel_tracing.resolve_tracer("faust-test")


def _finished(exporter):
    return {s.name: s for s in exporter.get_finished_spans()}


def test_otel_available():
    assert otel_tracing.otel_available() is True


def test_resolve_tracer_returns_tracer(tracer):
    assert tracer is not None
    assert hasattr(tracer, "start_span")


def test_current_span_roundtrip():
    assert otel_tracing.current_span() is None
    sentinel = object()
    otel_tracing.set_current_span(sentinel)
    assert otel_tracing.current_span() is sentinel
    otel_tracing.set_current_span(None)
    assert otel_tracing.current_span() is None


def test_noop_span_is_non_recording():
    span = otel_tracing.noop_span()
    assert span is trace.INVALID_SPAN
    assert span.is_recording() is False


def test_trace_id_from_seed_is_deterministic_and_nonzero():
    a = otel_tracing.trace_id_from_seed("reb-myapp-7")
    b = otel_tracing.trace_id_from_seed("reb-myapp-7")
    c = otel_tracing.trace_id_from_seed("reb-myapp-8")
    assert a == b
    assert a != c
    assert 0 < a < (1 << 128)


def test_deterministic_parent_context_yields_child_with_seed_trace_id(tracer, exporter):
    trace_id = otel_tracing.trace_id_from_seed("reb-myapp-7")
    ctx = otel_tracing.deterministic_parent_context(trace_id)
    span = tracer.start_span("rebalancing", context=ctx, kind=SpanKind.CONSUMER)
    span.end()

    reb = _finished(exporter)["rebalancing"]
    assert reb.context.trace_id == trace_id
    assert reb.kind is SpanKind.CONSUMER


def test_two_members_same_generation_share_trace(tracer, exporter):
    for _ in range(2):
        ctx = otel_tracing.deterministic_parent_context(
            otel_tracing.trace_id_from_seed("reb-myapp-42")
        )
        tracer.start_span("rebalancing", context=ctx).end()

    trace_ids = {s.context.trace_id for s in exporter.get_finished_spans()}
    assert len(trace_ids) == 1


def test_default_tags(tracer, exporter):
    span = tracer.start_span("op")
    otel_tracing.add_default_tags(span, "myapp", "myapp-id")
    span.end()

    attrs = _finished(exporter)["op"].attributes
    assert attrs["faust_app"] == "myapp"
    assert attrs["faust_id"] == "myapp-id"


def test_consume_attributes_coexist_legacy_and_semconv(tracer, exporter):
    span = tracer.start_span("consume-from-orders", kind=SpanKind.CONSUMER)
    otel_tracing.add_consume_attributes(
        span, topic="orders", partition=3, key=b"user-1", offset=99
    )
    span.end()

    got = _finished(exporter)
    # Legacy name preserved.
    assert "consume-from-orders" in got
    attrs = got["consume-from-orders"].attributes
    # Legacy tags (deprecated) present.
    assert attrs["kafka-topic"] == "orders"
    assert attrs["kafka-partition"] == 3
    assert attrs["kafka-key"] == "user-1"
    # Semantic conventions present on the same span.
    assert attrs["messaging.system"] == "kafka"
    assert attrs["messaging.destination.name"] == "orders"
    assert attrs["messaging.operation.type"] == "process"
    assert attrs["messaging.destination.partition.id"] == "3"
    assert attrs["messaging.kafka.offset"] == 99


def test_produce_attributes_coexist_legacy_and_semconv(tracer, exporter):
    span = tracer.start_span("produce-to-orders", kind=SpanKind.PRODUCER)
    otel_tracing.add_produce_attributes(span, topic="orders", partition=1, offset=5)
    span.end()

    attrs = _finished(exporter)["produce-to-orders"].attributes
    assert attrs["kafka-topic"] == "orders"
    assert attrs["kafka-offset"] == 5
    assert attrs["messaging.system"] == "kafka"
    assert attrs["messaging.operation.type"] == "send"


def test_stringify_key_handles_non_utf8(tracer, exporter):
    span = tracer.start_span("op")
    otel_tracing.add_consume_attributes(span, topic="t", key=b"\xff\xfe")
    span.end()
    # Non-decodable bytes fall back to hex rather than raising.
    assert _finished(exporter)["op"].attributes["kafka-key"] == "fffe"


def test_inject_extract_roundtrip_reparents(tracer, exporter):
    # Produce under a parent span, inject headers.
    parent = tracer.start_span("produce-to-t", kind=SpanKind.PRODUCER)
    token = trace.set_span_in_context(parent)
    from opentelemetry import context as otel_ctx

    ctx_token = otel_ctx.attach(token)
    carrier = otel_tracing.inject_trace_headers({})
    otel_ctx.detach(ctx_token)
    parent.end()

    assert "traceparent" in carrier

    # Consume: extract and start a child under the extracted context.
    extracted = otel_tracing.extract_trace_context(carrier)
    child = tracer.start_span("consume-from-t", context=extracted)
    child.end()

    got = _finished(exporter)
    assert (
        got["consume-from-t"].context.trace_id == got["produce-to-t"].context.trace_id
    )


def test_pending_span_materializes_late(tracer, exporter):
    pending = otel_tracing.PendingSpan(
        tracer,
        "rebalancing",
        kind=SpanKind.CONSUMER,
        attributes={"kafka_member_id": "m1"},
    )
    # Nothing emitted until materialized.
    assert exporter.get_finished_spans() == ()
    span = pending.materialize()
    assert span is not None
    span.end()

    reb = _finished(exporter)["rebalancing"]
    assert reb.attributes["kafka_member_id"] == "m1"
    assert reb.kind is SpanKind.CONSUMER


def test_pending_span_cancel_marks_and_ends(tracer, exporter):
    pending = otel_tracing.PendingSpan(tracer, "rebalancing")
    pending.materialize()
    pending.cancel()

    names = [s.name for s in exporter.get_finished_spans()]
    assert names == ["rebalancing (CANCELLED)"]


def test_semconv_span_name_builder():
    assert otel_tracing.semconv_span_name("process", "orders") == "orders process"
    assert otel_tracing.semconv_span_name("publish", "orders") == "orders publish"
