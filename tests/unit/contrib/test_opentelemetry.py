from unittest.mock import Mock, patch

import pytest

import faust
from faust.types import TP

pytest.importorskip("opentelemetry")

from opentelemetry import propagate, trace  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)

from faust.contrib.opentelemetry import (  # noqa: E402
    OpenTelemetrySensor,
    _build_getter,
    _kafka_headers_as_list,
    instrument_asgi_app,
    opentelemetry_available,
    sdk_is_configured,
    setup_opentelemetry,
)

TOPIC = "greetings"
TP1 = TP(TOPIC, 3)


@pytest.fixture()
def exporter():
    return InMemorySpanExporter()


@pytest.fixture()
def provider(exporter):
    """A private TracerProvider -- the global one can only be set once."""
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


@pytest.fixture()
def sensor(provider):
    return OpenTelemetrySensor(tracer=provider.get_tracer("test"))


@pytest.fixture()
def app():
    return faust.App("test-contrib-otel", store="memory://", cache="memory://")


@pytest.fixture()
def stream(app):
    return Mock(name="stream", app=app)


def _event(headers=None, key=None):
    event = Mock(name="event")
    event.message = Mock(headers=headers, key=key)
    return event


def _traceparent_headers(provider):
    """Produce Kafka headers carrying a real upstream span context."""
    carrier = {}
    tracer = provider.get_tracer("producer")
    with tracer.start_as_current_span("upstream send") as span:
        propagate.inject(carrier)
        trace_id = span.get_span_context().trace_id
    return [(k, v.encode()) for k, v in carrier.items()], trace_id


class Test_kafka_headers_as_list:
    def test_none_and_empty(self):
        assert _kafka_headers_as_list(None) == []
        assert _kafka_headers_as_list([]) == []

    def test_passes_through_a_list(self):
        headers = [("traceparent", b"x")]
        assert _kafka_headers_as_list(headers) == headers

    def test_normalizes_a_mapping(self):
        assert _kafka_headers_as_list({"traceparent": b"x"}) == [("traceparent", b"x")]


class Test_getter:
    def test_decodes_bytes(self):
        getter = _build_getter()
        assert getter.get([("traceparent", b"abc")], "traceparent") == ["abc"]

    def test_missing_key_is_none(self):
        getter = _build_getter()
        assert getter.get([("other", b"abc")], "traceparent") is None

    def test_ignores_null_values(self):
        getter = _build_getter()
        assert getter.get([("traceparent", None)], "traceparent") is None

    def test_first_match_wins(self):
        getter = _build_getter()
        headers = [("traceparent", b"first"), ("traceparent", b"second")]
        assert getter.get(headers, "traceparent") == ["first"]

    def test_keys(self):
        getter = _build_getter()
        assert getter.keys([("a", b"1"), ("b", b"2")]) == ["a", "b"]

    def test_works_with_a_mapping(self):
        getter = _build_getter()
        assert getter.get({"traceparent": b"abc"}, "traceparent") == ["abc"]


class Test_OpenTelemetrySensor:
    def test_creates_a_process_span(self, *, sensor, stream, exporter):
        state = sensor.on_stream_event_in(TP1, 42, stream, _event())
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        (span,) = exporter.get_finished_spans()
        assert span.name == f"{TOPIC} process"
        assert span.kind is trace.SpanKind.CONSUMER

    def test_semantic_attributes(self, *, sensor, stream, exporter, app):
        state = sensor.on_stream_event_in(TP1, 42, stream, _event(key=b"k1"))
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        (span,) = exporter.get_finished_spans()
        assert span.attributes["messaging.system"] == "kafka"
        assert span.attributes["messaging.operation.type"] == "process"
        assert span.attributes["messaging.destination.name"] == TOPIC
        assert span.attributes["messaging.destination.partition.id"] == "3"
        assert span.attributes["messaging.kafka.message.offset"] == 42
        assert span.attributes["messaging.consumer.group.name"] == app.conf.id
        assert span.attributes["messaging.kafka.message.key"] == "k1"

    def test_null_key_is_omitted(self, *, sensor, stream, exporter):
        """The spec says the key attribute MUST NOT be set when key is null."""
        state = sensor.on_stream_event_in(TP1, 42, stream, _event(key=None))
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        (span,) = exporter.get_finished_spans()
        assert "messaging.kafka.message.key" not in span.attributes

    def test_continues_the_trace_from_message_headers(
        self, *, sensor, stream, exporter, provider
    ):
        """This is the gap the aiokafka instrumentation cannot close."""
        headers, upstream_trace_id = _traceparent_headers(provider)

        state = sensor.on_stream_event_in(TP1, 42, stream, _event(headers=headers))
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        spans = {s.name: s for s in exporter.get_finished_spans()}
        process = spans[f"{TOPIC} process"]
        assert process.context.trace_id == upstream_trace_id
        assert process.parent is not None

    def test_span_is_current_while_processing(
        self, *, sensor, stream, exporter, provider
    ):
        """Work done by the agent must nest under the process span."""
        state = sensor.on_stream_event_in(TP1, 42, stream, _event())
        with provider.get_tracer("agent").start_as_current_span("db query"):
            pass
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        spans = {s.name: s for s in exporter.get_finished_spans()}
        assert (
            spans["db query"].parent.span_id
            == spans[f"{TOPIC} process"].context.span_id
        )

    def test_context_is_detached_afterwards(self, *, sensor, stream, provider):
        state = sensor.on_stream_event_in(TP1, 42, stream, _event())
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        assert trace.get_current_span() is trace.INVALID_SPAN

    def test_missing_state_is_ignored(self, *, sensor, stream, exporter):
        """``Stream.ack()`` calls the hook without sensor state."""
        sensor.on_stream_event_out(TP1, 42, stream, _event(), None)

        assert exporter.get_finished_spans() == ()

    def test_out_is_idempotent(self, *, sensor, stream, exporter):
        state = sensor.on_stream_event_in(TP1, 42, stream, _event())
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)
        sensor.on_stream_event_out(TP1, 42, stream, _event(), state)

        assert len(exporter.get_finished_spans()) == 1

    def test_garbage_headers_do_not_break_processing(self, *, sensor, stream, exporter):
        event = _event(headers=[("traceparent", b"not-a-traceparent")])

        state = sensor.on_stream_event_in(TP1, 42, stream, event)
        sensor.on_stream_event_out(TP1, 42, stream, event, state)

        # Still traced, just not continuing a (nonexistent) upstream trace.
        (span,) = exporter.get_finished_spans()
        assert span.name == f"{TOPIC} process"

    def test_tracer_defaults_to_the_global_provider(self):
        sensor = OpenTelemetrySensor()
        assert sensor.tracer is not None


class Test_sdk_is_configured:
    def test_true_for_a_real_provider(self, *, provider):
        with patch.object(trace, "get_tracer_provider", return_value=provider):
            assert sdk_is_configured() is True

    def test_false_for_the_proxy_provider(self):
        proxy = Mock()
        type(proxy).__name__ = "ProxyTracerProvider"
        with patch.object(trace, "get_tracer_provider", return_value=proxy):
            assert sdk_is_configured() is False

    def test_api_is_available(self):
        assert opentelemetry_available() is True


class Test_instrument_asgi_app:
    def test_none_app(self):
        assert instrument_asgi_app(None) is False

    def test_skips_an_already_instrumented_app(self):
        asgi_app = Mock(_is_instrumented_by_opentelemetry=True)

        assert instrument_asgi_app(asgi_app, force=True) is False

    def test_skips_when_no_sdk_is_configured(self):
        proxy = Mock()
        type(proxy).__name__ = "ProxyTracerProvider"
        asgi_app = Mock(_is_instrumented_by_opentelemetry=False)
        with patch.object(trace, "get_tracer_provider", return_value=proxy):
            assert instrument_asgi_app(asgi_app) is False

    def test_instruments_when_forced(self, *, provider):
        pytest.importorskip("opentelemetry.instrumentation.fastapi")
        fastapi = pytest.importorskip("fastapi")
        api = fastapi.FastAPI()

        assert instrument_asgi_app(api, tracer_provider=provider, force=True) is True
        assert api._is_instrumented_by_opentelemetry is True
        # Second call is a no-op, not a duplicate middleware stack.
        assert instrument_asgi_app(api, tracer_provider=provider, force=True) is False


class Test_setup_opentelemetry:
    def test_registers_the_sensor(self, *, app):
        sensor = setup_opentelemetry(app)

        assert isinstance(sensor, OpenTelemetrySensor)
        assert sensor in list(app.sensors)

    def test_returns_none_without_opentelemetry(self, *, app):
        with patch(
            "faust.contrib.opentelemetry.opentelemetry_available", return_value=False
        ):
            assert setup_opentelemetry(app) is None

    def test_warns_when_opentracing_sensor_is_registered(self, *, app):
        from faust.sensors.distributed_tracing import TracingSensor

        app.sensors.add(TracingSensor())

        with patch("faust.contrib.opentelemetry.logger") as logger:
            setup_opentelemetry(app)

        assert logger.warning.called
        assert "traceparent" in logger.warning.call_args[0][0]
