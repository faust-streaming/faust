"""Monitor using Prometheus."""
import typing
from typing import Any, NamedTuple, cast

from aiohttp.web import Response

from faust import web, web as _web
from faust.exceptions import ImproperlyConfigured
from faust.types import (
    TP,
    AppT,
    CollectionT,
    EventT,
    Message,
    PendingMessage,
    RecordMetadata,
    StreamT,
)
from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT

from .monitor import Monitor, TPOffsetMapping

try:
    import prometheus_client
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        REGISTRY,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
except ImportError:  # pragma: no cover
    prometheus_client = None


__all__ = ["setup_prometheus_sensors"]


def setup_prometheus_sensors(
    app: AppT,
    pattern: str = "/metrics",
    registry: CollectorRegistry = REGISTRY,
    name_prefix: str = None,
) -> None:
    """
    A utility function which sets up prometheus and attaches the config to the app.

    @param app: the faust app instance
    @param pattern: the url pattern for prometheus
    @param registry: the prometheus registry
    @param name_prefix: the name prefix. Defaults to the app name
    @return: None
    """
    if prometheus_client is None:
        raise ImproperlyConfigured(
            "prometheus_client requires `pip install prometheus_client`."
        )
    if name_prefix is None:
        app_conf_name = app.conf.name
        app.logger.info(
            "Name prefix is not supplied. Using the name %s from App config.",
            app_conf_name,
        )
        if "-" in app_conf_name:
            name_prefix = app_conf_name.replace("-", "_")
            app.logger.warning(
                "App config name %s does not conform to"
                " Prometheus naming conventions."
                " Using %s as a name_prefix.",
                app_conf_name,
                name_prefix,
            )

    faust_metrics = FaustMetrics.create(registry, name_prefix)
    app.monitor = PrometheusMonitor(metrics=faust_metrics)

    @app.page(pattern)
    async def metrics_handler(self: _web.View, request: _web.Request) -> _web.Response:
        headers = {"Content-Type": CONTENT_TYPE_LATEST}

        return cast(
            _web.Response,
            Response(body=generate_latest(REGISTRY), headers=headers, status=200),
        )


class FaustMetrics(NamedTuple):
    messages_received: Counter
    active_messages: Gauge
    messages_received_per_topics: Counter
    messages_received_per_topics_partition: Gauge
    events_runtime_latency: Histogram

    # On Event Stream in
    total_events: Counter
    total_active_events: Gauge
    total_events_per_stream: Counter

    # On table changes get/set/del keys
    table_operations: Counter

    # On message send
    topic_messages_sent: Counter
    total_sent_messages: Counter
    producer_send_latency: Histogram
    total_error_messages_sent: Counter
    producer_error_send_latency: Histogram

    # Assignment
    assignment_operations: Counter
    assign_latency: Histogram

    # Rebalances
    total_rebalances: Gauge
    total_rebalances_recovering: Gauge
    rebalance_done_consumer_latency: Histogram
    rebalance_done_latency: Histogram

    # Count Metrics by name
    count_metrics_by_name: Gauge

    # Web
    http_status_codes: Counter
    http_latency: Histogram

    # Topic/Partition Offsets
    topic_partition_end_offset: Gauge
    topic_partition_offset_commited: Gauge
    consumer_commit_latency: Histogram

    @classmethod
    def create(cls, registry: CollectorRegistry, app_name: str) -> "FaustMetrics":
        messages_received = Counter(
            f"{app_name}_messages_received",
            "Total messages received",
            registry=registry,
        )
        active_messages = Gauge(
            f"{app_name}_active_messages", "Total active messages", registry=registry
        )
        messages_received_per_topics = Counter(
            f"{app_name}_messages_received_per_topic",
            "Messages received per topic",
            ["topic"],
            registry=registry,
        )
        messages_received_per_topics_partition = Gauge(
            f"{app_name}_messages_received_per_topics_partition",
            "Messages received per topic/partition",
            ["topic", "partition"],
            registry=registry,
        )
        events_runtime_latency = Histogram(
            f"{app_name}_events_runtime_ms", "Events runtime in ms", registry=registry
        )
        total_events = Counter(
            f"{app_name}_total_events", "Total events received", registry=registry
        )
        total_active_events = Gauge(
            f"{app_name}_total_active_events", "Total active events", registry=registry
        )
        total_events_per_stream = Counter(
            f"{app_name}_total_events_per_stream",
            "Events received per Stream",
            ["stream"],
            registry=registry,
        )
        table_operations = Counter(
            f"{app_name}_table_operations",
            "Total table operations",
            ["table", "operation"],
            registry=registry,
        )
        topic_messages_sent = Counter(
            f"{app_name}_topic_messages_sent",
            "Total messages sent per topic",
            ["topic"],
            registry=registry,
        )
        total_sent_messages = Counter(
            f"{app_name}_total_sent_messages", "Total messages sent", registry=registry
        )
        producer_send_latency = Histogram(
            f"{app_name}_producer_send_latency",
            "Producer send latency in ms",
            registry=registry,
        )
        total_error_messages_sent = Counter(
            f"{app_name}_total_error_messages_sent",
            "Total error messages sent",
            registry=registry,
        )
        producer_error_send_latency = Histogram(
            f"{app_name}_producer_error_send_latency",
            "Producer error send latency in ms",
            registry=registry,
        )
        assignment_operations = Counter(
            f"{app_name}_assignment_operations",
            "Total assigment operations (completed/error)",
            ["operation"],
            registry=registry,
        )
        assign_latency = Histogram(
            f"{app_name}_assign_latency", "Assignment latency in ms", registry=registry
        )
        total_rebalances = Gauge(
            f"{app_name}_total_rebalances", "Total rebalances", registry=registry
        )
        total_rebalances_recovering = Gauge(
            f"{app_name}_total_rebalances_recovering",
            "Total rebalances recovering",
            registry=registry,
        )
        rebalance_done_consumer_latency = Histogram(
            f"{app_name}_rebalance_done_consumer_latency",
            "Consumer replying that rebalance is done to broker in ms",
            registry=registry,
        )
        rebalance_done_latency = Histogram(
            f"{app_name}_rebalance_done_latency",
            "Rebalance finished latency in ms",
            registry=registry,
        )
        count_metrics_by_name = Gauge(
            f"{app_name}_metrics_by_name",
            "Total metrics by name",
            ["metric"],
            registry=registry,
        )
        http_status_codes = Counter(
            f"{app_name}_http_status_codes",
            "Total http_status code",
            ["status_code"],
            registry=registry,
        )
        http_latency = Histogram(
            f"{app_name}_http_latency", "Http response latency in ms", registry=registry
        )
        topic_partition_end_offset = Gauge(
            f"{app_name}_topic_partition_end_offset",
            "Offset ends per topic/partition",
            ["topic", "partition"],
            registry=registry,
        )
        topic_partition_offset_commited = Gauge(
            f"{app_name}_topic_partition_offset_commited",
            "Offset commited per topic/partition",
            ["topic", "partition"],
            registry=registry,
        )
        consumer_commit_latency = Histogram(
            f"{app_name}_consumer_commit_latency",
            "Consumer commit latency in ms",
            registry=registry,
        )
        return cls(
            messages_received=messages_received,
            active_messages=active_messages,
            messages_received_per_topics=messages_received_per_topics,
            messages_received_per_topics_partition=(
                messages_received_per_topics_partition
            ),
            events_runtime_latency=events_runtime_latency,
            total_events=total_events,
            total_active_events=total_active_events,
            total_events_per_stream=total_events_per_stream,
            table_operations=table_operations,
            topic_messages_sent=topic_messages_sent,
            total_sent_messages=total_sent_messages,
            producer_send_latency=producer_send_latency,
            total_error_messages_sent=total_error_messages_sent,
            producer_error_send_latency=producer_error_send_latency,
            assignment_operations=assignment_operations,
            assign_latency=assign_latency,
            total_rebalances=total_rebalances,
            total_rebalances_recovering=total_rebalances_recovering,
            rebalance_done_consumer_latency=rebalance_done_consumer_latency,
            rebalance_done_latency=rebalance_done_latency,
            count_metrics_by_name=count_metrics_by_name,
            http_status_codes=http_status_codes,
            http_latency=http_latency,
            topic_partition_end_offset=topic_partition_end_offset,
            topic_partition_offset_commited=topic_partition_offset_commited,
            consumer_commit_latency=consumer_commit_latency,
        )

    def clear_topic_related_metrics(self) -> None:
        self._clear_topic_partition_related_metrics()
        self._clear_topic_related_metrics()

    def _clear_topic_partition_related_metrics(self) -> None:
        metrics = [
            self.messages_received_per_topics_partition,
            self.topic_partition_end_offset,
            self.topic_partition_offset_commited,
        ]
        for metric in metrics:
            topics_partitions = frozenset(
                (sample.labels["topic"], sample.labels["partition"])
                for sample in metric.collect()[0].samples
            )
            for topic, partition in topics_partitions:
                metric.remove(topic, partition)

    def _clear_topic_related_metrics(self) -> None:
        metrics = [
            self.messages_received_per_topics,
            self.topic_messages_sent,
        ]
        for metric in metrics:
            topics = frozenset(
                sample.labels["topic"] for sample in metric.collect()[0].samples
            )
            for topic in topics:
                metric.remove(topic)


class PrometheusMonitor(Monitor):
    """
    Prometheus Faust Sensor.

    This sensor, records statistics using prometheus_client and expose
    them using the aiohttp server running under /metrics by default

    Usage:
        import faust
        from faust.sensors.prometheus import setup_prometheus_sensors

        app = faust.App('example', broker='kafka://')
        setup_prometheus_sensors(app, pattern='/metrics', 'example_app_name')
    """

    ERROR = "error"
    COMPLETED = "completed"
    KEYS_RETRIEVED = "keys_retrieved"
    KEYS_UPDATED = "keys_updated"
    KEYS_DELETED = "keys_deleted"

    def __init__(self, metrics: FaustMetrics, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._metrics = metrics

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        super().on_message_in(tp, offset, message)

        self._metrics.messages_received.inc()
        self._metrics.active_messages.inc()
        self._metrics.messages_received_per_topics.labels(topic=tp.topic).inc()
        self._metrics.messages_received_per_topics_partition.labels(
            topic=tp.topic, partition=tp.partition
        ).set(offset)

    def on_stream_event_in(
        self, tp: TP, offset: int, stream: StreamT, event: EventT
    ) -> typing.Optional[typing.Dict]:
        """Call when stream starts processing an event."""
        state = super().on_stream_event_in(tp, offset, stream, event)
        self._metrics.total_events.inc()
        self._metrics.total_active_events.inc()
        self._metrics.total_events_per_stream.labels(
            stream=f"stream.{self._stream_label(stream)}.events"
        ).inc()

        return state

    def _stream_label(self, stream: StreamT) -> str:
        return (
            self._normalize(
                stream.shortlabel.lstrip("Stream:"),
            )
            .strip("_")
            .lower()
        )

    def on_stream_event_out(
        self,
        tp: TP,
        offset: int,
        stream: StreamT,
        event: EventT,
        state: typing.Dict = None,
    ) -> None:
        """Call when stream is done processing an event."""
        super().on_stream_event_out(tp, offset, stream, event, state)
        self._metrics.total_active_events.dec()
        if state is not None:
            self._metrics.events_runtime_latency.observe(
                self.secs_to_ms(self.events_runtime[-1])
            )

    def on_message_out(self, tp: TP, offset: int, message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        super().on_message_out(tp, offset, message)
        self._metrics.active_messages.dec()

    def on_table_get(self, table: CollectionT, key: typing.Any) -> None:
        """Call when value in table is retrieved."""
        super().on_table_get(table, key)
        self._metrics.table_operations.labels(
            table=f"table.{table.name}", operation=self.KEYS_RETRIEVED
        ).inc()

    def on_table_set(
        self, table: CollectionT, key: typing.Any, value: typing.Any
    ) -> None:
        """Call when new value for key in table is set."""
        super().on_table_set(table, key, value)
        self._metrics.table_operations.labels(
            table=f"table.{table.name}", operation=self.KEYS_UPDATED
        ).inc()

    def on_table_del(self, table: CollectionT, key: typing.Any) -> None:
        """Call when key in a table is deleted."""
        super().on_table_del(table, key)
        self._metrics.table_operations.labels(
            table=f"table.{table.name}", operation=self.KEYS_DELETED
        ).inc()

    def on_commit_completed(self, consumer: ConsumerT, state: typing.Any) -> None:
        """Call when consumer commit offset operation completed."""
        super().on_commit_completed(consumer, state)
        self._metrics.consumer_commit_latency.observe(
            self.ms_since(typing.cast(float, state))
        )

    def on_send_initiated(
        self,
        producer: ProducerT,
        topic: str,
        message: PendingMessage,
        keysize: int,
        valsize: int,
    ) -> typing.Any:
        """Call when message added to producer buffer."""
        self._metrics.topic_messages_sent.labels(topic=f"topic.{topic}").inc()

        return super().on_send_initiated(producer, topic, message, keysize, valsize)

    def on_send_completed(
        self, producer: ProducerT, state: typing.Any, metadata: RecordMetadata
    ) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        self._metrics.total_sent_messages.inc()
        self._metrics.producer_send_latency.observe(
            self.ms_since(typing.cast(float, state))
        )

    def on_send_error(
        self, producer: ProducerT, exc: BaseException, state: typing.Any
    ) -> None:
        """Call when producer was unable to publish message."""
        super().on_send_error(producer, exc, state)
        self._metrics.total_error_messages_sent.inc()
        self._metrics.producer_error_send_latency.observe(
            self.ms_since(typing.cast(float, state))
        )

    def on_assignment_error(
        self, assignor: PartitionAssignorT, state: typing.Dict, exc: BaseException
    ) -> None:
        """Partition assignor did not complete assignor due to error."""
        super().on_assignment_error(assignor, state, exc)
        self._metrics.assignment_operations.labels(operation=self.ERROR).inc()
        self._metrics.assign_latency.observe(self.ms_since(state["time_start"]))

    def on_assignment_completed(
        self, assignor: PartitionAssignorT, state: typing.Dict
    ) -> None:
        """Partition assignor completed assignment."""
        super().on_assignment_completed(assignor, state)
        self._metrics.assignment_operations.labels(operation=self.COMPLETED).inc()
        self._metrics.assign_latency.observe(self.ms_since(state["time_start"]))

    def on_rebalance_start(self, app: AppT) -> typing.Dict:
        """Cluster rebalance in progress."""
        state = super().on_rebalance_start(app)
        self._clear_partition_related_metrics()
        self._metrics.total_rebalances.inc()

        return state

    def on_rebalance_return(self, app: AppT, state: typing.Dict) -> None:
        """Consumer replied assignment is done to broker."""
        super().on_rebalance_return(app, state)
        self._metrics.total_rebalances.dec()
        self._metrics.total_rebalances_recovering.inc()
        self._metrics.rebalance_done_consumer_latency.observe(
            self.ms_since(state["time_return"])
        )

    def on_rebalance_end(self, app: AppT, state: typing.Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        super().on_rebalance_end(app, state)
        self._clear_partition_related_metrics()
        self._metrics.total_rebalances_recovering.dec()
        self._metrics.rebalance_done_latency.observe(self.ms_since(state["time_end"]))

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self._metrics.count_metrics_by_name.labels(metric=metric_name).inc(count)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        super().on_tp_commit(tp_offsets)
        for tp, offset in tp_offsets.items():
            self._metrics.topic_partition_offset_commited.labels(
                topic=tp.topic, partition=tp.partition
            ).set(offset)

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
        super().track_tp_end_offset(tp, offset)
        self._metrics.topic_partition_end_offset.labels(
            topic=tp.topic, partition=tp.partition
        ).set(offset)

    def on_web_request_end(
        self,
        app: AppT,
        request: web.Request,
        response: typing.Optional[web.Response],
        state: typing.Dict,
        *,
        view: web.View = None,
    ) -> None:
        """Web server finished working on request."""
        super().on_web_request_end(app, request, response, state, view=view)
        status_code = int(state["status_code"])
        self._metrics.http_status_codes.labels(status_code=status_code).inc()
        self._metrics.http_latency.observe(self.ms_since(state["time_end"]))

    def _clear_partition_related_metrics(self) -> None:
        self._metrics.clear_topic_related_metrics()
        self.tp_committed_offsets.clear()
        self.tp_read_offsets.clear()
        self.tp_end_offsets.clear()
