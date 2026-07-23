"""Monitor reporting metrics via OpenTelemetry.

This sensor mirrors the metric set that :class:`~faust.sensors.statsd.StatsdMonitor`
reports, but records it through the `OpenTelemetry`_ metrics API instead of a
statsd socket.  Faust depends only on ``opentelemetry-api``: every instrument is
a cheap no-op until the *application* configures a global ``MeterProvider`` with
the exporter of its choice (OTLP, Prometheus, console, ...), so importing this
module never hard-fails Faust core.

Unlike statsd -- which encodes dimensions such as topic and partition into the
metric *name* -- the OpenTelemetry conventions favour a single instrument
dimensioned by *attributes*.  This sensor follows that convention: e.g. a single
``faust.messages.received`` counter carries ``topic``/``partition`` attributes
rather than one metric per topic.

.. _OpenTelemetry: https://opentelemetry.io/
"""

import typing
from typing import Any, Dict, Optional, cast

from mode.utils.objects import cached_property

import faust
from faust import web
from faust.exceptions import ImproperlyConfigured
from faust.types import (
    TP,
    AppT,
    CollectionT,
    EventT,
    Message,
    RecordMetadata,
    StreamT,
)
from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT

from .monitor import Monitor, TPOffsetMapping

try:
    from opentelemetry import metrics as otel_metrics
except ImportError:  # pragma: no cover
    otel_metrics = None

if typing.TYPE_CHECKING:  # pragma: no cover
    from opentelemetry.metrics import (
        Counter,
        Gauge,
        Histogram,
        Meter,
        UpDownCounter,
    )
else:

    class Meter: ...  # noqa

    class Counter: ...  # noqa

    class UpDownCounter: ...  # noqa

    class Histogram: ...  # noqa

    class Gauge: ...  # noqa


__all__ = ["OTelMetrics", "OpenTelemetryMonitor"]

#: Attributes = OpenTelemetry's term for the key/value dimensions on a metric
#: point (statsd bakes these into the metric name instead).
Attributes = Dict[str, Any]


class OTelMetrics:
    """Container for the OpenTelemetry instruments used by the monitor.

    Instruments are created once, up front, from a single ``Meter``; recording
    happens by supplying attributes at call time.  Latency histograms use the
    ``ms`` unit because Faust observes every latency in milliseconds (see
    :meth:`~faust.sensors.monitor.Monitor.ms_since`).
    """

    #: Monotonic counters (``.add``).
    messages_received: Counter
    events_total: Counter
    messages_sent: Counter
    messages_send_errors: Counter
    table_operations: Counter
    assignments: Counter
    http_requests: Counter
    custom_counts: Counter

    #: Up/down counters for values that rise and fall (``.add`` +/-).
    messages_active: UpDownCounter
    events_active: UpDownCounter
    rebalances_active: UpDownCounter
    rebalances_recovering: UpDownCounter

    #: Latency histograms in milliseconds (``.record``).
    events_runtime: Histogram
    commit_latency: Histogram
    send_latency: Histogram
    send_error_latency: Histogram
    assignment_latency: Histogram
    rebalance_return_latency: Histogram
    rebalance_end_latency: Histogram
    http_latency: Histogram

    #: Synchronous gauges for last-known values (``.set``).
    offset_read: Gauge
    offset_committed: Gauge
    offset_end: Gauge
    producer_buffer: Gauge

    def __init__(self, meter: Meter) -> None:
        self.meter = meter

        self.messages_received = meter.create_counter(
            "faust.messages.received",
            unit="1",
            description="Messages received from Kafka.",
        )
        self.events_total = meter.create_counter(
            "faust.events.total",
            unit="1",
            description="Stream events processed by agents.",
        )
        self.messages_sent = meter.create_counter(
            "faust.messages.sent",
            unit="1",
            description="Messages successfully sent to Kafka.",
        )
        self.messages_send_errors = meter.create_counter(
            "faust.messages.send_errors",
            unit="1",
            description="Messages that failed to send.",
        )
        self.table_operations = meter.create_counter(
            "faust.table.operations",
            unit="1",
            description="Table key operations (get/set/del).",
        )
        self.assignments = meter.create_counter(
            "faust.assignments",
            unit="1",
            description="Partition assignor runs (completed/error).",
        )
        self.http_requests = meter.create_counter(
            "faust.http.requests",
            unit="1",
            description="Web requests served, by status code.",
        )
        self.custom_counts = meter.create_counter(
            "faust.custom.count",
            unit="1",
            description="Application counters recorded via Monitor.count().",
        )

        self.messages_active = meter.create_up_down_counter(
            "faust.messages.active",
            unit="1",
            description="Messages currently being processed.",
        )
        self.events_active = meter.create_up_down_counter(
            "faust.events.active",
            unit="1",
            description="Stream events currently being processed.",
        )
        self.rebalances_active = meter.create_up_down_counter(
            "faust.rebalances.active",
            unit="1",
            description="Cluster rebalances currently in progress.",
        )
        self.rebalances_recovering = meter.create_up_down_counter(
            "faust.rebalances.recovering",
            unit="1",
            description="Rebalances currently in the recovery phase.",
        )

        self.events_runtime = meter.create_histogram(
            "faust.events.runtime",
            unit="ms",
            description="Time spent processing a stream event.",
        )
        self.commit_latency = meter.create_histogram(
            "faust.commit.latency",
            unit="ms",
            description="Consumer offset-commit latency.",
        )
        self.send_latency = meter.create_histogram(
            "faust.send.latency",
            unit="ms",
            description="Producer send latency (success).",
        )
        self.send_error_latency = meter.create_histogram(
            "faust.send.error_latency",
            unit="ms",
            description="Producer send latency (failure).",
        )
        self.assignment_latency = meter.create_histogram(
            "faust.assignment.latency",
            unit="ms",
            description="Partition-assignment latency.",
        )
        self.rebalance_return_latency = meter.create_histogram(
            "faust.rebalance.return_latency",
            unit="ms",
            description="Time until the consumer returned its assignment.",
        )
        self.rebalance_end_latency = meter.create_histogram(
            "faust.rebalance.end_latency",
            unit="ms",
            description="Total rebalance time, including recovery.",
        )
        self.http_latency = meter.create_histogram(
            "faust.http.latency",
            unit="ms",
            description="Web request handling latency.",
        )

        self.offset_read = meter.create_gauge(
            "faust.offset.read",
            unit="1",
            description="Last read offset per topic/partition.",
        )
        self.offset_committed = meter.create_gauge(
            "faust.offset.committed",
            unit="1",
            description="Last committed offset per topic/partition.",
        )
        self.offset_end = meter.create_gauge(
            "faust.offset.end",
            unit="1",
            description="Last known end offset per topic/partition.",
        )
        self.producer_buffer = meter.create_gauge(
            "faust.producer.buffer",
            unit="1",
            description="Size of the threaded producer send buffer.",
        )


class OpenTelemetryMonitor(Monitor):
    """OpenTelemetry Faust Sensor.

    Records the same metrics as :class:`~faust.sensors.statsd.StatsdMonitor`
    through the OpenTelemetry metrics API, dimensioned by attributes.

    The application is responsible for configuring a global ``MeterProvider``
    (and an exporter) before starting the app; until then the instruments are
    cheap no-ops.  A ``meter`` may be supplied explicitly, otherwise one is
    resolved from the globally-registered provider.
    """

    def __init__(
        self,
        *,
        meter: Optional[Meter] = None,
        service_name: str = "faust",
        metrics: Optional[OTelMetrics] = None,
        **kwargs: Any,
    ) -> None:
        if otel_metrics is None:
            raise ImproperlyConfigured(
                f"{type(self).__name__} requires "
                '`pip install "faust[opentelemetry]"`.'
            )
        self.service_name = service_name
        self._meter = meter
        self._metrics = metrics
        super().__init__(**kwargs)

    @cached_property
    def meter(self) -> Meter:
        """Return the OpenTelemetry meter for this monitor."""
        if self._meter is not None:
            return self._meter
        return otel_metrics.get_meter(self.service_name, faust.__version__)

    @cached_property
    def metrics(self) -> OTelMetrics:
        """Return the container of OpenTelemetry instruments."""
        if self._metrics is not None:
            return self._metrics
        return OTelMetrics(self.meter)

    # -- Messages -----------------------------------------------------------

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        super().on_message_in(tp, offset, message)
        attrs = self._tp_attrs(tp)
        self.metrics.messages_received.add(1, attrs)
        self.metrics.messages_active.add(1, attrs)
        self.metrics.offset_read.set(offset, attrs)

    def on_message_out(self, tp: TP, offset: int, message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        super().on_message_out(tp, offset, message)
        self.metrics.messages_active.add(-1, self._tp_attrs(tp))

    # -- Stream events ------------------------------------------------------

    def on_stream_event_in(
        self, tp: TP, offset: int, stream: StreamT, event: EventT
    ) -> Optional[Dict]:
        """Call when stream starts processing an event."""
        state = super().on_stream_event_in(tp, offset, stream, event)
        attrs = self._tp_attrs(tp, stream)
        self.metrics.events_total.add(1, attrs)
        self.metrics.events_active.add(1, attrs)
        return state

    def on_stream_event_out(
        self,
        tp: TP,
        offset: int,
        stream: StreamT,
        event: EventT,
        state: Dict = None,
    ) -> None:
        """Call when stream is done processing an event."""
        super().on_stream_event_out(tp, offset, stream, event, state)
        attrs = self._tp_attrs(tp, stream)
        self.metrics.events_active.add(-1, attrs)
        if state is not None:
            self.metrics.events_runtime.record(
                self.secs_to_ms(self.events_runtime[-1]), attrs
            )

    # -- Tables -------------------------------------------------------------

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Call when value in table is retrieved."""
        super().on_table_get(table, key)
        self.metrics.table_operations.add(1, self._table_attrs(table, "get"))

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Call when new value for key in table is set."""
        super().on_table_set(table, key, value)
        self.metrics.table_operations.add(1, self._table_attrs(table, "set"))

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Call when key in a table is deleted."""
        super().on_table_del(table, key)
        self.metrics.table_operations.add(1, self._table_attrs(table, "del"))

    # -- Producer -----------------------------------------------------------

    def on_send_completed(
        self, producer: ProducerT, state: Any, metadata: RecordMetadata
    ) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        attrs = {"topic": metadata.topic} if metadata.topic is not None else {}
        self.metrics.messages_sent.add(1, attrs)
        self.metrics.send_latency.record(self.ms_since(cast(float, state)))

    def on_send_error(
        self, producer: ProducerT, exc: BaseException, state: Any
    ) -> None:
        """Call when producer was unable to publish message."""
        super().on_send_error(producer, exc, state)
        self.metrics.messages_send_errors.add(1)
        self.metrics.send_error_latency.record(self.ms_since(cast(float, state)))

    # -- Assignments --------------------------------------------------------

    def on_assignment_error(
        self, assignor: PartitionAssignorT, state: Dict, exc: BaseException
    ) -> None:
        """Partition assignor did not complete assignor due to error."""
        super().on_assignment_error(assignor, state, exc)
        attrs = {"result": "error"}
        self.metrics.assignments.add(1, attrs)
        self.metrics.assignment_latency.record(
            self.ms_since(state["time_start"]), attrs
        )

    def on_assignment_completed(
        self, assignor: PartitionAssignorT, state: Dict
    ) -> None:
        """Partition assignor completed assignment."""
        super().on_assignment_completed(assignor, state)
        attrs = {"result": "completed"}
        self.metrics.assignments.add(1, attrs)
        self.metrics.assignment_latency.record(
            self.ms_since(state["time_start"]), attrs
        )

    # -- Rebalances ---------------------------------------------------------

    def on_rebalance_start(self, app: AppT) -> Dict:
        """Cluster rebalance in progress."""
        state = super().on_rebalance_start(app)
        self.metrics.rebalances_active.add(1)
        return state

    def on_rebalance_return(self, app: AppT, state: Dict) -> None:
        """Consumer replied assignment is done to broker."""
        super().on_rebalance_return(app, state)
        self.metrics.rebalances_active.add(-1)
        self.metrics.rebalances_recovering.add(1)
        self.metrics.rebalance_return_latency.record(
            self.ms_since(state["time_return"])
        )

    def on_rebalance_end(self, app: AppT, state: Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        super().on_rebalance_end(app, state)
        self.metrics.rebalances_recovering.add(-1)
        self.metrics.rebalance_end_latency.record(self.ms_since(state["time_end"]))

    # -- Offsets / commit ---------------------------------------------------

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        """Call when consumer commit offset operation completed."""
        super().on_commit_completed(consumer, state)
        self.metrics.commit_latency.record(self.ms_since(cast(float, state)))

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            self.metrics.offset_committed.set(offset, self._tp_attrs(tp))

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
        super().track_tp_end_offset(tp, offset)
        self.metrics.offset_end.set(offset, self._tp_attrs(tp))

    # -- Web ----------------------------------------------------------------

    def on_web_request_end(
        self,
        app: AppT,
        request: web.Request,
        response: Optional[web.Response],
        state: Dict,
        *,
        view: web.View = None,
    ) -> None:
        """Web server finished working on request."""
        super().on_web_request_end(app, request, response, state, view=view)
        status_code = int(state["status_code"])
        self.metrics.http_requests.add(1, {"status_code": status_code})
        self.metrics.http_latency.record(self.ms_since(state["time_end"]))

    def on_threaded_producer_buffer_processed(self, app: AppT, size: int) -> None:
        """Call when the threaded producer flushed its buffer."""
        super().on_threaded_producer_buffer_processed(app, size)
        self.metrics.producer_buffer.set(size)

    # -- Custom counters ----------------------------------------------------

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self.metrics.custom_counts.add(count, {"name": metric_name})

    # -- Attribute builders -------------------------------------------------

    def _tp_attrs(self, tp: TP, stream: Optional[StreamT] = None) -> Attributes:
        attrs: Attributes = {"topic": tp.topic, "partition": tp.partition}
        if stream is not None:
            attrs["stream"] = self._stream_label(stream)
        return attrs

    def _table_attrs(self, table: CollectionT, operation: str) -> Attributes:
        return {"table": table.name, "operation": operation}

    def _stream_label(self, stream: StreamT) -> str:
        return (
            self._normalize(
                stream.shortlabel.lstrip("Stream:"),
            )
            .strip("_")
            .lower()
        )
