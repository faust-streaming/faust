from typing import Dict, Union
from unittest.mock import Mock

import pytest
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from faust import web
from faust.sensors.prometheus import FaustMetrics, PrometheusMonitor
from faust.types import TP, AppT, EventT, StreamT, TableT

Metric = Union[Counter, Gauge, Histogram]

TP1 = TP("foo", 3)
TP2 = TP("bar", 4)
TP3 = TP("baz", 5)


def _time() -> float:
    return 101.1


class TestPrometheusMonitor:
    @pytest.fixture
    def registry(self) -> CollectorRegistry:
        return CollectorRegistry()

    @pytest.fixture
    def metrics(self, registry: CollectorRegistry) -> FaustMetrics:
        return FaustMetrics.create(registry, "test")

    @pytest.fixture
    def app(self) -> AppT:
        return Mock(autospec=AppT)

    @pytest.fixture
    def monitor(self, metrics: FaustMetrics) -> PrometheusMonitor:
        return PrometheusMonitor(metrics=metrics, time=_time)

    @pytest.fixture()
    def stream(self) -> StreamT:
        stream = Mock(autospec=StreamT, name="stream")
        stream.shortlabel = "Stream: Topic: foo"
        return stream

    @pytest.fixture()
    def event(self) -> EventT:
        return Mock(autospec=EventT, name="event")

    @pytest.fixture()
    def table(self) -> TableT:
        table = Mock(autospec=TableT, name="table")
        return table

    @pytest.fixture()
    def response(self) -> web.Response:
        return Mock(name="response", autospec=web.Response)

    @pytest.fixture()
    def view(self) -> web.View:
        return Mock(name="view", autospec=web.View)

    def test_on_message_in(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        message = Mock(name="message")

        monitor.on_message_in(TP1, 400, message)

        self.assert_has_sample_value(
            metrics.messages_received, "test_messages_received_total", {}, 1
        )
        self.assert_has_sample_value(
            metrics.active_messages, "test_active_messages", {}, 1
        )
        self.assert_has_sample_value(
            metrics.messages_received_per_topics,
            "test_messages_received_per_topic_total",
            {"topic": "foo"},
            1,
        )
        self.assert_has_sample_value(
            metrics.messages_received_per_topics_partition,
            "test_messages_received_per_topics_partition",
            {"topic": "foo", "partition": "3"},
            400,
        )

    def test_on_message_out(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        n_messages = 20
        message = Mock(name="message", time_in=100.2)
        metrics.active_messages.inc(n_messages)

        monitor.on_message_out(TP1, 400, message)

        self.assert_has_sample_value(
            metrics.active_messages, "test_active_messages", {}, n_messages - 1
        )

    def test_on_stream_event_in(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        stream: StreamT,
        event: EventT,
    ) -> None:
        monitor.on_stream_event_in(TP1, 401, stream, event)

        self.assert_has_sample_value(
            metrics.total_events, "test_total_events_total", {}, 1
        )
        self.assert_has_sample_value(
            metrics.total_active_events, "test_total_active_events", {}, 1
        )
        self.assert_has_sample_value(
            metrics.total_events_per_stream,
            "test_total_events_per_stream_total",
            {"stream": "stream.topic_foo.events"},
            1,
        )

    def test_on_stream_event_out(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        stream: StreamT,
        event: EventT,
    ) -> None:
        n_events = 25
        state = {"time_in": 101.1, "time_out": None, "time_total": None}
        metrics.total_active_events.inc(n_events)

        monitor.on_stream_event_out(TP1, 401, stream, event, state)

        self.assert_has_sample_value(
            metrics.total_active_events,
            "test_total_active_events",
            {},
            n_events - 1,
        )

    def test_on_stream_event_out_does_not_measure_latency_without_state(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        stream: StreamT,
        event: EventT,
    ) -> None:
        monitor.on_stream_event_out(TP1, 401, stream, event, state=None)

        self.assert_doesnt_have_sample_values(
            metrics.events_runtime_latency,
            "test_events_runtime_latency_total",
            {},
        )

    def test_on_table_get(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, table: TableT
    ) -> None:
        monitor.on_table_get(table, "key")

        self.assert_has_sample_value(
            metrics.table_operations,
            "test_table_operations_total",
            {"table": f"table.{table.name}", "operation": "keys_retrieved"},
            1,
        )

    def test_on_table_set(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, table: TableT
    ) -> None:
        monitor.on_table_set(table, "key", "value")

        self.assert_has_sample_value(
            metrics.table_operations,
            "test_table_operations_total",
            {"table": f"table.{table.name}", "operation": "keys_updated"},
            1,
        )

    def test_on_table_del(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, table: TableT
    ) -> None:
        monitor.on_table_del(table, "key")

        self.assert_has_sample_value(
            metrics.table_operations,
            "test_table_operations_total",
            {"table": f"table.{table.name}", "operation": "keys_deleted"},
            1,
        )

    def test_on_commit_completed(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        consumer = Mock(name="consumer")

        state = monitor.on_commit_initiated(consumer)
        monitor.on_commit_completed(consumer, state)

        self.assert_has_sample_value(
            metrics.consumer_commit_latency,
            "test_consumer_commit_latency_sum",
            {},
            monitor.ms_since(float(state)),
        )

    def test_on_send_initiated_completed(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        producer = Mock(name="producer")

        state = monitor.on_send_initiated(producer, "topic1", "message", 321, 123)
        monitor.on_send_completed(producer, state, Mock(name="metadata"))

        self.assert_has_sample_value(
            metrics.total_sent_messages, "test_total_sent_messages_total", {}, 1
        )
        self.assert_has_sample_value(
            metrics.topic_messages_sent,
            "test_topic_messages_sent_total",
            {"topic": "topic.topic1"},
            1,
        )
        self.assert_has_sample_value(
            metrics.producer_send_latency,
            "test_producer_send_latency_sum",
            {},
            monitor.ms_since(float(state)),
        )

    def test_on_send_error(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        timestamp = 101.01
        producer = Mock(name="producer")

        monitor.on_send_error(producer, KeyError("foo"), timestamp)

        self.assert_has_sample_value(
            metrics.total_error_messages_sent,
            "test_total_error_messages_sent_total",
            {},
            1,
        )
        self.assert_has_sample_value(
            metrics.producer_error_send_latency,
            "test_producer_error_send_latency_sum",
            {},
            monitor.ms_since(timestamp),
        )

    def test_on_assignment_start_completed(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        assignor = Mock(name="assignor")

        state = monitor.on_assignment_start(assignor)
        monitor.on_assignment_completed(assignor, state)

        self.assert_has_sample_value(
            metrics.assignment_operations,
            "test_assignment_operations_total",
            {"operation": monitor.COMPLETED},
            1,
        )
        self.assert_has_sample_value(
            metrics.assign_latency,
            "test_assign_latency_sum",
            {},
            monitor.ms_since(state["time_start"]),
        )

    def test_on_assignment_start_failed(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        assignor = Mock(name="assignor")

        state = monitor.on_assignment_start(assignor)
        monitor.on_assignment_error(assignor, state, KeyError("foo"))

        self.assert_has_sample_value(
            metrics.assignment_operations,
            "test_assignment_operations_total",
            {"operation": monitor.ERROR},
            1,
        )
        self.assert_has_sample_value(
            metrics.assign_latency,
            "test_assign_latency_sum",
            {},
            monitor.ms_since(state["time_start"]),
        )

    def test_on_rebalance(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, app: AppT
    ) -> None:
        monitor.on_rebalance_start(app)

        self.assert_has_sample_value(
            metrics.total_rebalances, "test_total_rebalances", {}, 1
        )

    def test_on_rebalance_return(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, app: AppT
    ) -> None:
        state = {"time_start": 99.1}
        n_rebalances = 50
        metrics.total_rebalances.set(n_rebalances)

        monitor.on_rebalance_return(app, state)

        self.assert_has_sample_value(
            metrics.total_rebalances,
            "test_total_rebalances",
            {},
            n_rebalances - 1,
        )
        self.assert_has_sample_value(
            metrics.total_rebalances_recovering,
            "test_total_rebalances_recovering",
            {},
            1,
        )
        self.assert_has_sample_value(
            metrics.rebalance_done_consumer_latency,
            "test_rebalance_done_consumer_latency_sum",
            {},
            monitor.ms_since(state["time_return"]),
        )

    def test_on_rebalance_end(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics, app: AppT
    ) -> None:
        state = {"time_start": 99.2}
        n_rebalances = 12
        metrics.total_rebalances_recovering.set(n_rebalances)

        monitor.on_rebalance_end(app, state)

        self.assert_has_sample_value(
            metrics.total_rebalances_recovering,
            "test_total_rebalances_recovering",
            {},
            n_rebalances - 1,
        )
        self.assert_has_sample_value(
            metrics.rebalance_done_latency,
            "test_rebalance_done_latency_sum",
            {},
            monitor.ms_since(state["time_end"]),
        )

    def test_on_web_request(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        app: AppT,
        request: web.Request,
        response: web.Response,
        view: web.View,
    ) -> None:
        response.status = 404
        self.assert_on_web_request(
            monitor, metrics, app, request, response, view, expected_status=404
        )

    def test_on_web_request_none_response(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        app: AppT,
        request: web.Request,
        view: web.View,
    ) -> None:
        self.assert_on_web_request(
            monitor, metrics, app, request, None, view, expected_status=500
        )

    def assert_on_web_request(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        app: AppT,
        request: web.Request,
        response: web.Response,
        view: web.View,
        expected_status: int,
    ) -> None:
        state = monitor.on_web_request_start(app, request, view=view)
        monitor.on_web_request_end(app, request, response, state, view=view)

        self.assert_has_sample_value(
            metrics.http_status_codes,
            "test_http_status_codes_total",
            {"status_code": str(expected_status)},
            1,
        )
        self.assert_has_sample_value(
            metrics.http_latency,
            "test_http_latency_sum",
            {},
            monitor.ms_since(state["time_end"]),
        )

    def test_count(self, monitor: PrometheusMonitor, metrics: FaustMetrics) -> None:
        monitor.count("metric_name", count=3)

        self.assert_has_sample_value(
            metrics.count_metrics_by_name,
            "test_metrics_by_name",
            {"metric": "metric_name"},
            3,
        )

    def test_on_tp_commit(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        offsets = {TP("foo", 0): 1001, TP("foo", 1): 2002, TP("bar", 3): 3003}

        monitor.on_tp_commit(offsets)

        self.assert_has_sample_value(
            metrics.topic_partition_offset_commited,
            "test_topic_partition_offset_commited",
            {"topic": "foo", "partition": "0"},
            1001,
        )
        self.assert_has_sample_value(
            metrics.topic_partition_offset_commited,
            "test_topic_partition_offset_commited",
            {"topic": "foo", "partition": "1"},
            2002,
        )
        self.assert_has_sample_value(
            metrics.topic_partition_offset_commited,
            "test_topic_partition_offset_commited",
            {"topic": "bar", "partition": "3"},
            3003,
        )

    def test_track_tp_end_offsets(
        self, monitor: PrometheusMonitor, metrics: FaustMetrics
    ) -> None:
        monitor.track_tp_end_offset(TP("foo", 0), 4004)

        self.assert_has_sample_value(
            metrics.topic_partition_end_offset,
            "test_topic_partition_end_offset",
            {"topic": "foo", "partition": "0"},
            4004,
        )

    def test_old_labels_are_removed_from_registry_after_rebalance(
        self,
        monitor: PrometheusMonitor,
        metrics: FaustMetrics,
        registry: CollectorRegistry,
        stream: StreamT,
        event: EventT,
        app: AppT,
    ) -> None:
        self._handle_event(
            monitor=monitor,
            topic_partition=TP1,
            stream=stream,
            event=event,
            offset=10,
        )

        monitor.on_rebalance_start(app)
        monitor.on_rebalance_end(app, state={"time_start": monitor.time()})
        self._handle_event(
            monitor=monitor,
            topic_partition=TP2,
            stream=stream,
            event=event,
            offset=11,
        )

        collected_topics = frozenset(
            sample.labels["topic"]
            for metric in registry.collect()
            if metric.name == "test_messages_received_per_topic"
            for sample in metric.samples
        )
        assert collected_topics == frozenset([TP2.topic])
        collected_partitions = frozenset(
            (sample.labels["topic"], sample.labels["partition"])
            for metric in registry.collect()
            if metric.name == "test_messages_received_per_topics_partition"
            for sample in metric.samples
        )
        assert collected_partitions == frozenset([(TP2.topic, str(TP2.partition))])

    def assert_has_sample_value(
        self, metric: Metric, name: str, labels: Dict[str, str], value: int
    ) -> None:
        samples = metric.collect()[0].samples
        assert any(
            sample.name == name and sample.labels == labels and sample.value == value
            for sample in samples
        )

    def assert_doesnt_have_sample_values(
        self, metric: Metric, name: str, labels: Dict[str, str]
    ) -> None:
        samples = [
            sample for sample in metric.collect()[0].samples if sample.name == name
        ]
        assert samples == []

    def _handle_event(
        self,
        monitor: PrometheusMonitor,
        topic_partition: TP,
        stream: StreamT,
        event: EventT,
        offset: int,
    ) -> None:
        monitor.track_tp_end_offset(topic_partition, offset + 5)
        monitor.on_message_in(topic_partition, offset, event.message)
        monitor.on_stream_event_in(topic_partition, offset, stream, event)
        monitor.on_tp_commit({topic_partition: offset})
