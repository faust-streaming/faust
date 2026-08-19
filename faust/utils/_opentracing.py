"""No-op stand-in for :pypi:`opentracing`.

OpenTracing is an optional dependency (``faust[opentracing]``). When it is not
installed, the tracing modules fall back to this module so that Faust still
imports and runs -- tracing simply becomes a no-op. Distributed tracing only
does real work when an ``app.tracer`` is configured (or the ``TracingSensor``
is used), which requires the real ``opentracing`` package to be installed.

This intentionally implements only the small surface Faust touches, but over
that surface it must stay *substitutable* for the real library: Faust reaches
through the objects it is handed (``parent.tracer.start_span(...)``), so a
plausible-looking attribute holding the wrong value breaks callers that never
mention this module.  Absences count as surface too -- the aiokafka consumer
thread reads ``Span.operation_name`` and treats the resulting
:exc:`AttributeError` as "this is not a real span", which only works because
:class:`opentracing.Span` does not define that attribute either.

``Tracer.start_active_span`` is deliberately *not* provided: Faust never calls
it, and the real one returns a ``Scope`` wrapping the span rather than the span
itself, so a stand-in returning a :class:`Span` would mis-serve anyone who did.
"""

from typing import Any, Literal


class Span:
    """A span that does nothing.

    Mirrors :class:`opentracing.Span`, including its ``(tracer, context)``
    signature: every span knows the tracer that made it, so callers can start
    a child span from any span they are given.
    """

    def __init__(self, tracer: Any, context: Any = None) -> None:
        self.tracer: Any = tracer
        self.context: Any = _SpanContext() if context is None else context

    def __enter__(self) -> "Span":
        return self

    def __exit__(self, *exc_info: Any) -> Literal[False]:
        # ``finish`` rather than ``pass``: subclasses (and the lazy-span
        # rebinding in the aiokafka driver) override ``finish``, and the real
        # library calls it on exit.
        if exc_info and exc_info[0] is not None:
            self.set_tag(tags.ERROR, True)
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
    return parent_span.tracer.start_span(
        operation_name=operation_name,
        child_of=parent_span.context,
    )


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
