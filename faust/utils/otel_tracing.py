"""Native OpenTelemetry tracing core for Faust.

Phase 1a foundation for the native-OpenTelemetry rewrite of Faust's built-in
distributed-tracing layer.  See
``docs/adr/0001-native-opentelemetry-tracing.md`` for the full decision record.

This module provides the reusable primitives the call-site conversion (Phase 1b)
will build on:

* tracer resolution against the globally-registered ``TracerProvider``
* current-span propagation via ``contextvars`` (parity with the legacy layer)
* a deterministic parent context for rebalance spans -- the native
  re-expression of the old ``span.context.trace_id = murmur2(...)`` mutation,
  which is impossible in OpenTelemetry because span contexts are immutable
* a deferred ("lazy") span helper -- the native re-expression of the old
  ``span.finish`` monkeypatch
* semantic-convention *coexistence* helpers that set both the legacy tags and
  the OpenTelemetry ``messaging.*`` attributes on every span
* W3C header inject/extract for cross-service context propagation

It depends only on ``opentelemetry-api`` (the optional ``faust[opentelemetry]``
extra) and degrades to no-ops when OpenTelemetry is not installed, so importing
it never hard-fails Faust core.
"""

import hashlib
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional

try:
    from opentelemetry import propagate, trace
    from opentelemetry.trace import (
        NonRecordingSpan,
        SpanContext,
        SpanKind,
        TraceFlags,
        set_span_in_context,
    )

    HAS_OTEL = True
except ImportError:  # pragma: no cover
    HAS_OTEL = False

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Span, Tracer

__all__ = [
    "HAS_OTEL",
    "SPAN_NAMES_LEGACY",
    "SPAN_NAMES_SEMCONV",
    "PendingSpan",
    "otel_available",
    "resolve_tracer",
    "current_span",
    "set_current_span",
    "noop_span",
    "trace_id_from_seed",
    "deterministic_parent_context",
    "add_default_tags",
    "add_consume_attributes",
    "add_produce_attributes",
    "inject_trace_headers",
    "extract_trace_context",
]

#: Span-name mode: keep the historical Faust names (deprecated, default).
SPAN_NAMES_LEGACY = "legacy"
#: Span-name mode: use OpenTelemetry messaging semantic-convention names.
SPAN_NAMES_SEMCONV = "semconv"

# -- OpenTelemetry messaging semantic-convention attribute keys -*-
_MSG_SYSTEM = "messaging.system"
_MSG_DESTINATION = "messaging.destination.name"
_MSG_PARTITION = "messaging.destination.partition.id"
_MSG_OPERATION_TYPE = "messaging.operation.type"
_MSG_KAFKA_KEY = "messaging.kafka.message.key"
_MSG_KAFKA_OFFSET = "messaging.kafka.offset"

_current_span: "ContextVar[Optional[Any]]" = ContextVar(
    "faust_otel_current_span", default=None
)


def otel_available() -> bool:
    """Return :const:`True` if the OpenTelemetry API is importable."""
    return HAS_OTEL


def resolve_tracer(
    service_name: str = "faust", version: Optional[str] = None
) -> Optional["Tracer"]:
    """Return a tracer from the globally-registered ``TracerProvider``.

    Returns :const:`None` when OpenTelemetry is not installed.  When no
    provider has been configured by the application, the OpenTelemetry API
    returns a no-op tracer whose spans are cheap and non-exporting, so callers
    never need to special-case the unconfigured state.
    """
    if not HAS_OTEL:
        return None
    return trace.get_tracer(service_name, version)


def current_span() -> Optional["Span"]:
    """Return the current Faust span for this context, if any."""
    return _current_span.get()


def set_current_span(span: Optional["Span"]) -> None:
    """Set the current Faust span for the current context."""
    _current_span.set(span)


def noop_span() -> Optional["Span"]:
    """Return a non-recording span (or :const:`None` without OpenTelemetry)."""
    if not HAS_OTEL:
        return None
    return trace.INVALID_SPAN


def trace_id_from_seed(seed: str) -> int:
    """Derive a stable, non-zero 128-bit trace id from a seed string.

    Used for rebalance spans so that every member computing the same
    ``reb-{app_id}-{generation}`` seed lands on the same trace, without
    mutating an (immutable) OpenTelemetry span context.
    """
    digest = hashlib.blake2b(seed.encode(), digest_size=16).digest()
    trace_id = int.from_bytes(digest, "big")
    # A trace id of 0 is invalid in OpenTelemetry; blake2b collisions to zero
    # are astronomically unlikely, but guard anyway.
    return trace_id or 1


def deterministic_parent_context(trace_id: int) -> Optional["Context"]:
    """Build a context carrying a deterministic ``trace_id`` for a child span.

    Wraps a :class:`~opentelemetry.trace.NonRecordingSpan` whose
    :class:`~opentelemetry.trace.SpanContext` carries *trace_id*; starting a
    span with the returned context makes it a child that inherits *trace_id*.
    This is the native replacement for the old
    ``span.context.trace_id = murmur2(...)`` mutation.
    """
    if not HAS_OTEL:
        return None
    # span_id must be a non-zero 64-bit int; derive it from the trace id so it
    # is stable per (app, generation) too.
    span_id = (trace_id & 0xFFFFFFFFFFFFFFFF) or 1
    parent_context = SpanContext(
        trace_id=trace_id,
        span_id=span_id,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return set_span_in_context(NonRecordingSpan(parent_context))


def add_default_tags(span: Optional["Span"], app_name: str, app_id: str) -> None:
    """Set Faust's default identifying tags on *span*."""
    if span is None or not HAS_OTEL:
        return
    span.set_attribute("faust_app", app_name)
    span.set_attribute("faust_id", app_id)


def _set_messaging_common(
    span: "Span",
    *,
    topic: str,
    partition: Optional[int],
    key: Any,
    offset: Optional[int],
    operation: str,
) -> None:
    span.set_attribute(_MSG_SYSTEM, "kafka")
    span.set_attribute(_MSG_DESTINATION, topic)
    span.set_attribute(_MSG_OPERATION_TYPE, operation)
    if partition is not None:
        span.set_attribute(_MSG_PARTITION, str(partition))
    if offset is not None:
        span.set_attribute(_MSG_KAFKA_OFFSET, offset)
    if key is not None:
        span.set_attribute(_MSG_KAFKA_KEY, _stringify_key(key))


def _stringify_key(key: Any) -> str:
    if isinstance(key, bytes):
        try:
            return key.decode("utf-8")
        except UnicodeDecodeError:
            return key.hex()
    return str(key)


def add_consume_attributes(
    span: Optional["Span"],
    *,
    topic: str,
    partition: Optional[int] = None,
    key: Any = None,
    offset: Optional[int] = None,
) -> None:
    """Set consume-side attributes, coexisting legacy tags and semconv.

    Emits the historical ``kafka-*`` tags *and* the OpenTelemetry
    ``messaging.*`` attributes on the same span (see the ADR: names stay
    legacy, attributes carry both).
    """
    if span is None or not HAS_OTEL:
        return
    # Legacy tags (deprecated -- retained for existing dashboards).
    span.set_attribute("kafka-topic", topic)
    if partition is not None:
        span.set_attribute("kafka-partition", partition)
    if key is not None:
        span.set_attribute("kafka-key", _stringify_key(key))
    # Semantic conventions.
    _set_messaging_common(
        span,
        topic=topic,
        partition=partition,
        key=key,
        offset=offset,
        operation="process",
    )


def add_produce_attributes(
    span: Optional["Span"],
    *,
    topic: str,
    partition: Optional[int] = None,
    key: Any = None,
    offset: Optional[int] = None,
) -> None:
    """Set produce-side attributes, coexisting legacy tags and semconv."""
    if span is None or not HAS_OTEL:
        return
    span.set_attribute("kafka-topic", topic)
    if partition is not None:
        span.set_attribute("kafka-partition", partition)
    if offset is not None:
        span.set_attribute("kafka-offset", offset)
    _set_messaging_common(
        span,
        topic=topic,
        partition=partition,
        key=key,
        offset=offset,
        operation="send",
    )


def inject_trace_headers(carrier: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Inject the current context into *carrier* as W3C trace headers."""
    if carrier is None:
        carrier = {}
    if not HAS_OTEL:
        return carrier
    propagate.inject(carrier)
    return carrier


def extract_trace_context(carrier: Mapping[str, Any]) -> Optional["Context"]:
    """Extract a parent context from *carrier* (W3C trace headers)."""
    if not HAS_OTEL:
        return None
    return propagate.extract(carrier)


class PendingSpan:
    """A span whose creation is deferred until enough state is known.

    Native replacement for the old lazy-finish monkeypatch: instead of starting
    a span and rewriting its ``.finish`` to run later, buffer the *intent* to
    trace and materialize the real span once the deferred state (e.g. the Kafka
    generation id) is available -- or drop it entirely.
    """

    def __init__(
        self,
        tracer: Optional["Tracer"],
        name: str,
        *,
        kind: Optional[Any] = None,
        attributes: Optional[Dict[str, Any]] = None,
        context: Optional["Context"] = None,
    ) -> None:
        self.tracer = tracer
        self.name = name
        self.kind = kind
        self.attributes = attributes
        self.context = context
        self._span: Optional["Span"] = None

    def materialize(self) -> Optional["Span"]:
        """Start and return the real span, or :const:`None` without OTel."""
        if self.tracer is None or not HAS_OTEL:
            return None
        kind = self.kind if self.kind is not None else SpanKind.INTERNAL
        self._span = self.tracer.start_span(
            self.name,
            context=self.context,
            kind=kind,
            attributes=self.attributes,
        )
        return self._span

    def cancel(self, note: str = "CANCELLED") -> None:
        """Drop the pending span; if already materialized, mark and end it."""
        if self._span is not None and HAS_OTEL:
            self._span.update_name(f"{self.name} ({note})")
            self._span.end()
            self._span = None


#: Mapping of legacy operation -> semconv span-name builder, for the opt-in
#: ``SPAN_NAMES_SEMCONV`` mode.  Consumed in Phase 1b when call sites are
#: rewired; declared here so the naming contract lives with the core.
def semconv_span_name(operation: str, destination: str) -> str:
    """Return the semantic-convention span name ``{destination} {operation}``."""
    return f"{destination} {operation}"
