"""No-op stand-in for :pypi:`opentracing`.

OpenTracing is an optional dependency (``faust[opentracing]``). When it is not
installed, the tracing modules fall back to this module so that Faust still
imports and runs -- tracing simply becomes a no-op. Distributed tracing only
does real work when an ``app.tracer`` is configured (or the ``TracingSensor``
is used), which requires the real ``opentracing`` package to be installed.

Only the surface Faust touches is implemented, but across it this must stay
substitutable: callers reach through what they are handed (``parent.tracer``),
and absences count too -- the consumer thread reads ``operation_name`` and
treats ``AttributeError`` as "not a real span".
"""

from typing import Any, Literal


class Span:
    """A span that does nothing."""

    def __init__(self, tracer: Any, *args: Any, **kwargs: Any) -> None:
        self.context = _SpanContext()
        self.tracer: Any = tracer

    def __enter__(self) -> "Span":
        return self

    def __exit__(self, *exc_info: Any) -> Literal[False]:
        self.finish()
        return False

    def finish(self, *args: Any, **kwargs: Any) -> None: ...

    def set_tag(self, *args: Any, **kwargs: Any) -> "Span":
        return self

    def set_operation_name(self, *args: Any, **kwargs: Any) -> "Span":
        return self

    def log_kv(self, *args: Any, **kwargs: Any) -> "Span":
        return self

    def set_baggage_item(self, *args: Any, **kwargs: Any) -> "Span":
        return self

    def get_baggage_item(self, *args: Any, **kwargs: Any) -> Any:
        return None


class _SpanContext:
    trace_id: Any = None
    span_id: Any = None


class Tracer:
    """A tracer that produces only no-op spans."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._noop_span = Span(tracer=self)

    def start_span(self, *args: Any, **kwargs: Any) -> Span:
        return self._noop_span

    def extract(self, *args: Any, **kwargs: Any) -> Any:
        return None

    def inject(self, *args: Any, **kwargs: Any) -> None: ...


#: Module-level global tracer, mirroring ``opentracing.tracer``.
tracer = Tracer()


def follows_from(*args: Any, **kwargs: Any) -> Any:
    return None


def child_of(*args: Any, **kwargs: Any) -> Any:
    return None


def start_child_span(
    parent_span: Span, operation_name: Any = None, *args: Any, **kwargs: Any
) -> Span:
    return parent_span.tracer.start_span(operation_name=operation_name)


class Format:
    """Mirror of ``opentracing.Format`` carrier formats."""

    TEXT_MAP = "text_map"
    HTTP_HEADERS = "http_headers"
    BINARY = "binary"


class tags:
    """Mirror of the ``opentracing.ext.tags`` constants Faust references."""

    ERROR = "error"
    SAMPLING_PRIORITY = "sampling.priority"
    SPAN_KIND = "span.kind"
    COMPONENT = "component"
    MESSAGE_BUS_DESTINATION = "message_bus.destination"
