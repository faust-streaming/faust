"""OpenTelemetry tracing for Faust, and for a co-hosted ASGI application.

Why this exists
===============

:pypi:`opentelemetry-instrumentation-aiokafka` already wraps
``AIOKafkaProducer.send`` and ``AIOKafkaConsumer.getmany``, which is exactly
what Faust's aiokafka driver calls.  So with that package installed you already
get most of a distributed trace for free::

    FastAPI server span
      -> aiokafka "{topic} send"     (PRODUCER, injects ``traceparent``)
      -> [Kafka]
      -> aiokafka "{topic} receive"  (CONSUMER, extracts ``traceparent``)
      -> ???

The last hop is the one nobody outside Faust can supply.  Faust's consumer runs
in its own thread (:class:`~faust.transport.consumer.ConsumerThread`), and
:mod:`contextvars` never cross threads -- so the ``receive`` span is opened
*and closed* inside ``getmany``, on a thread the agent never runs on.  Without
help you get an orphaned ``receive`` span and an unparented agent, which reads
worse in a trace viewer than no instrumentation at all.

:class:`OpenTelemetrySensor` closes that gap.  It extracts the trace context
from the Kafka message headers and opens a ``{topic} process`` span that stays
current for exactly as long as the stream is processing the event, so anything
the agent does -- an HTTP call, a database query, another ``topic.send()`` --
nests underneath it.

Usage
=====

.. sourcecode:: python

    from faust.contrib.opentelemetry import setup_opentelemetry

    app = faust.App("myapp", broker="kafka://localhost:9092")
    setup_opentelemetry(app)

Configure an SDK the usual way (``opentelemetry-sdk`` plus an exporter, or the
``opentelemetry-instrument`` CLI).  Until you do, the OpenTelemetry API is a
no-op and this module costs effectively nothing.

Install with ``pip install faust-streaming[opentelemetry]``.

Notes
=====

* This module depends only on ``opentelemetry-api``.  A library must never
  configure the SDK, so nothing here calls ``set_tracer_provider()``.
* Trace context is *read* from message headers, never written.  On the produce
  side ``opentelemetry-instrumentation-aiokafka`` already injects, and its
  setter appends unconditionally -- a second injector would put two
  ``traceparent`` headers on the wire.
* Faust's older :pypi:`opentracing` support
  (:class:`faust.sensors.distributed_tracing.TracingSensor`) *does* inject into
  Kafka headers.  Do not run both; :func:`setup_opentelemetry` warns if it
  sees one already registered.
"""

import typing
from typing import Any, Dict, List, Mapping, Optional, Sequence

from mode import get_logger

from faust.sensors.base import Sensor
from faust.types import TP, AppT, EventT, StreamT

if typing.TYPE_CHECKING:
    from opentelemetry.trace import Tracer
else:
    Tracer = Any

__all__ = [
    "OpenTelemetrySensor",
    "instrument_asgi_app",
    "opentelemetry_available",
    "sdk_is_configured",
    "setup_opentelemetry",
]

logger = get_logger(__name__)

#: Instrumentation scope name reported for spans created here.
INSTRUMENTATION_NAME = "faust"

#: Provider class names that mean "the user never configured an SDK".
_NOOP_PROVIDERS = frozenset(
    {"ProxyTracerProvider", "NoOpTracerProvider", "DefaultTracerProvider"}
)


def opentelemetry_available() -> bool:
    """Return :const:`True` if the OpenTelemetry API is importable."""
    try:
        import opentelemetry.trace  # noqa: F401
    except Exception:  # pragma: no cover
        return False
    return True


def sdk_is_configured() -> bool:
    """Return :const:`True` if a real :class:`TracerProvider` is installed.

    The OpenTelemetry API ships a proxy/no-op provider until an application
    calls ``set_tracer_provider()``.  Treating that as "tracing is off" is what
    lets this integration be enabled by default without doing anything behind
    an operator's back.
    """
    try:
        from opentelemetry import trace
    except Exception:  # pragma: no cover
        return False
    return type(trace.get_tracer_provider()).__name__ not in _NOOP_PROVIDERS


def _kafka_headers_as_list(
    headers: Optional[Any],
) -> List[Any]:
    """Normalize Faust's ``HeadersArg`` to a list of ``(str, bytes)``."""
    if not headers:
        return []
    if isinstance(headers, Mapping):
        return list(headers.items())
    return list(headers)


def _build_getter() -> Any:
    from opentelemetry.propagators import textmap

    class KafkaHeadersGetter(textmap.Getter):
        """Read W3C trace context out of Kafka record headers.

        Kafka headers are a sequence of ``(str, bytes)`` pairs which may repeat
        a key; OpenTelemetry carriers are string-keyed.  The first match wins,
        matching the behaviour of the aiokafka instrumentation.
        """

        def get(self, carrier: Any, key: str) -> Optional[Sequence[str]]:
            for item_key, value in _kafka_headers_as_list(carrier):
                if item_key == key and value is not None:
                    if isinstance(value, bytes):
                        return [value.decode("utf-8", "replace")]
                    return [str(value)]
            return None

        def keys(self, carrier: Any) -> List[str]:
            return [key for key, _ in _kafka_headers_as_list(carrier)]

    return KafkaHeadersGetter()


class OpenTelemetrySensor(Sensor):
    """Open an OpenTelemetry span around each event a stream processes.

    The span is parented to the producer's span via the ``traceparent`` header
    on the Kafka message, and stays current for the duration of processing.
    """

    def __init__(self, *, tracer: Optional[Tracer] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._explicit_tracer = tracer
        self._getter = _build_getter()

    @property
    def tracer(self) -> Tracer:
        """Tracer used to create spans (resolved lazily)."""
        if self._explicit_tracer is not None:
            return self._explicit_tracer
        from opentelemetry import trace

        # Resolved on every call on purpose: applications commonly configure
        # the SDK after importing their Faust app.
        return trace.get_tracer(INSTRUMENTATION_NAME)

    def _span_attributes(
        self, stream: StreamT, event: EventT, tp: TP, offset: int
    ) -> Dict[str, Any]:
        message = event.message
        attributes: Dict[str, Any] = {
            "messaging.system": "kafka",
            "messaging.operation.name": "process",
            "messaging.operation.type": "process",
            "messaging.destination.name": tp.topic,
            "messaging.destination.partition.id": str(tp.partition),
            "messaging.kafka.message.offset": offset,
        }
        # Sensors are not given the app, but the stream knows it.
        app = getattr(stream, "app", None)
        consumer_group = getattr(getattr(app, "conf", None), "id", None)
        if consumer_group:
            attributes["messaging.consumer.group.name"] = str(consumer_group)
        key = getattr(message, "key", None)
        # The spec says this MUST NOT be set when the key is null.
        if key is not None:
            attributes["messaging.kafka.message.key"] = (
                key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
            )
        return attributes

    def on_stream_event_in(
        self, tp: TP, offset: int, stream: StreamT, event: EventT
    ) -> Optional[Dict]:
        """Start a ``process`` span and make it current."""
        try:
            from opentelemetry import context as otel_context, propagate, trace
        except Exception:  # pragma: no cover
            return None

        try:
            headers = getattr(event.message, "headers", None)
            parent = propagate.extract(headers, getter=self._getter)
            span = self.tracer.start_span(
                f"{tp.topic} process",
                context=parent,
                kind=trace.SpanKind.CONSUMER,
                attributes=self._span_attributes(stream, event, tp, offset),
            )
            token = otel_context.attach(trace.set_span_in_context(span))
        except Exception as exc:  # pragma: no cover
            # Telemetry must never break message processing.
            logger.debug("OpenTelemetry: could not start process span: %r", exc)
            return None
        return {"span": span, "token": token}

    def on_stream_event_out(
        self,
        tp: TP,
        offset: int,
        stream: StreamT,
        event: EventT,
        state: Dict = None,
    ) -> None:
        """Detach the context and end the span."""
        if not state:
            # ``Stream.ack()`` calls this without sensor state; the span is
            # then closed by the ``__aiter__`` path that opened it.
            return
        span = state.pop("span", None)
        token = state.pop("token", None)
        try:
            from opentelemetry import context as otel_context
        except Exception:  # pragma: no cover
            return
        try:
            if token is not None:
                otel_context.detach(token)
        except Exception as exc:  # pragma: no cover
            logger.debug("OpenTelemetry: could not detach context: %r", exc)
        if span is not None:
            try:
                span.end()
            except Exception as exc:  # pragma: no cover
                logger.debug("OpenTelemetry: could not end span: %r", exc)


def instrument_asgi_app(
    asgi_app: Any, *, tracer_provider: Any = None, force: bool = False
) -> bool:
    """Attach OpenTelemetry instrumentation to a FastAPI/Starlette app.

    Returns :const:`True` if instrumentation was attached.

    Does nothing -- and returns :const:`False` -- when the instrumentation
    package is missing, when no SDK has been configured (unless ``force``), or
    when the application is already instrumented (for example because it was
    started under ``opentelemetry-instrument``).
    """
    if asgi_app is None:
        return False
    if getattr(asgi_app, "_is_instrumented_by_opentelemetry", False):
        logger.debug("OpenTelemetry: ASGI app already instrumented, skipping")
        return False
    if not force and not sdk_is_configured():
        return False

    try:
        # A ``try/except ImportError`` rather than ``find_spec``: the
        # instrumentation packages pin their siblings exactly, so an
        # importable-but-broken install is a realistic failure mode.
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    except Exception as exc:
        logger.debug("OpenTelemetry: FastAPI instrumentation unavailable: %r", exc)
        return False

    try:
        FastAPIInstrumentor.instrument_app(
            asgi_app,
            tracer_provider=tracer_provider,
            exclude_spans=["receive", "send"],
        )
    except TypeError:
        # ``exclude_spans`` was added in a later release.
        try:
            FastAPIInstrumentor.instrument_app(
                asgi_app, tracer_provider=tracer_provider
            )
        except Exception as exc:
            logger.debug("OpenTelemetry: could not instrument ASGI app: %r", exc)
            return False
    except Exception as exc:
        logger.debug("OpenTelemetry: could not instrument ASGI app: %r", exc)
        return False
    logger.info("OpenTelemetry: instrumented ASGI application")
    return True


def setup_opentelemetry(
    app: AppT, *, tracer: Optional[Tracer] = None
) -> Optional[OpenTelemetrySensor]:
    """Register the OpenTelemetry sensor on ``app``.

    Returns the sensor, or :const:`None` if OpenTelemetry is not installed.
    """
    if not opentelemetry_available():
        logger.debug("OpenTelemetry: API not installed, tracing sensor not registered")
        return None

    if _has_opentracing_sensor(app):
        logger.warning(
            "OpenTelemetry: a TracingSensor (opentracing) is already "
            "registered.  Both inject/extract Kafka trace headers; running "
            "them together produces duplicate traceparent headers and "
            "confusing traces.  Register only one."
        )

    sensor = OpenTelemetrySensor(tracer=tracer)
    app.sensors.add(sensor)
    return sensor


def _has_opentracing_sensor(app: AppT) -> bool:
    try:
        from faust.sensors.distributed_tracing import TracingSensor
    except Exception:  # pragma: no cover
        return False
    return any(isinstance(s, TracingSensor) for s in app.sensors)
