from typing import Any, Dict, List, Optional
from unittest.mock import Mock

import pytest

from faust.exceptions import ImproperlyConfigured
from faust.sensors.otel import OpenTelemetryMonitor, OTelMetrics
from faust.types import TP

opentelemetry = pytest.importorskip("opentelemetry")

from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402

TP1 = TP("foo", 3)
TP2 = TP("bar", 4)


def _time() -> float:
    return 101.1


class Collected:
    """Flattened view of the metrics a reader collected."""

    def __init__(self, reader: InMemoryMetricReader) -> None:
        self.points: Dict[str, List[Any]] = {}
        data = reader.get_metrics_data()
        if data is None:
            return
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                self.scope = sm.scope
                for metric in sm.metrics:
                    self.points.setdefault(metric.name, [])
                    for point in metric.data.data_points:
                        self.points[metric.name].append(point)

    def names(self) -> List[str]:
        return sorted(self.points)

    def point(self, name: str, attributes: Optional[Dict] = None) -> Any:
        candidates = self.points.get(name, [])
        if attributes is None:
            assert len(candidates) == 1, f"{name}: {candidates}"
            return candidates[0]
        for point in candidates:
            if dict(point.attributes) == attributes:
                return point
        raise AssertionError(
            f"no point for {name} with attributes {attributes}: "
            f"{[dict(p.attributes) for p in candidates]}"
        )

    def value(self, name: str, attributes: Optional[Dict] = None) -> Any:
        return self.point(name, attributes).value

    def sum(self, name: str, attributes: Optional[Dict] = None) -> Any:
        return self.point(name, attributes).sum

    def count(self, name: str, attributes: Optional[Dict] = None) -> int:
        return self.point(name, attributes).count


class TestOpenTelemetryMonitor:
    @pytest.fixture()
    def reader(self) -> InMemoryMetricReader:
        return InMemoryMetricReader()

    @pytest.fixture()
    def meter(self, reader: InMemoryMetricReader):
        # An isolated provider per test -- never touch the global provider.
        provider = MeterProvider(metric_readers=[reader])
        return provider.get_meter("faust-test")

    @pytest.fixture()
    def mon(self, meter) -> OpenTelemetryMonitor:
        return OpenTelemetryMonitor(meter=meter, time=_time)

    @pytest.fixture()
    def stream(self):
        stream = Mock(name="stream")
        stream.shortlabel = "Stream: Topic: foo"
        return stream

    @pytest.fixture()
    def event(self):
        return Mock(name="event")

    @pytest.fixture()
    def table(self):
        table = Mock(name="table")
        table.name = "table1"
        return table

    def test_raises_if_opentelemetry_not_installed(self, *, monkeypatch):
        monkeypatch.setattr("faust.sensors.otel.otel_metrics", None)
        with pytest.raises(ImproperlyConfigured):
            OpenTelemetryMonitor()

    def test_resolves_global_meter_by_default(self, *, monkeypatch):
        get_meter = Mock(name="get_meter")
        monkeypatch.setattr("faust.sensors.otel.otel_metrics.get_meter", get_meter)
        mon = OpenTelemetryMonitor(service_name="svc")
        assert mon.meter is get_meter.return_value
        get_meter.assert_called_once()
        assert get_meter.call_args.args[0] == "svc"

    def test_metrics_container_built_lazily(self, *, mon):
        assert isinstance(mon.metrics, OTelMetrics)

    def test_on_message_in(self, *, mon, reader):
        mon.on_message_in(TP1, 400, Mock(name="message"))

        c = Collected(reader)
        attrs = {"topic": "foo", "partition": 3}
        assert c.value("faust.messages.received", attrs) == 1
        assert c.value("faust.messages.active", attrs) == 1
        assert c.value("faust.offset.read", attrs) == 400

    def test_on_message_out_decrements_active(self, *, mon, reader):
        mon.on_message_in(TP1, 400, Mock(name="message", time_in=100.0))
        mon.on_message_out(TP1, 400, Mock(name="message", time_in=100.0))

        c = Collected(reader)
        attrs = {"topic": "foo", "partition": 3}
        assert c.value("faust.messages.active", attrs) == 0

    def test_on_stream_event_in_out(self, *, mon, reader, stream, event):
        state = mon.on_stream_event_in(TP1, 401, stream, event)
        attrs = {"topic": "foo", "partition": 3, "stream": "topic_foo"}

        c = Collected(reader)
        assert c.value("faust.events.total", attrs) == 1
        assert c.value("faust.events.active", attrs) == 1

        mon.on_stream_event_out(TP1, 401, stream, event, state)
        c = Collected(reader)
        assert c.value("faust.events.active", attrs) == 0
        assert c.count("faust.events.runtime", attrs) == 1

    def test_on_stream_event_out_no_runtime_without_state(
        self, *, mon, reader, stream, event
    ):
        mon.on_stream_event_out(TP1, 401, stream, event, state=None)
        c = Collected(reader)
        assert "faust.events.runtime" not in c.names()

    def test_table_operations(self, *, mon, reader, table):
        mon.on_table_get(table, "k")
        mon.on_table_set(table, "k", "v")
        mon.on_table_del(table, "k")

        c = Collected(reader)
        base = {"table": "table1"}
        assert c.value("faust.table.operations", {**base, "operation": "get"}) == 1
        assert c.value("faust.table.operations", {**base, "operation": "set"}) == 1
        assert c.value("faust.table.operations", {**base, "operation": "del"}) == 1

    def test_on_send_completed(self, *, mon, reader):
        metadata = Mock(name="metadata")
        metadata.topic = "foo"
        mon.on_send_completed(Mock(name="producer"), 100.1, metadata)

        c = Collected(reader)
        assert c.value("faust.messages.sent", {"topic": "foo"}) == 1
        assert c.count("faust.send.latency") == 1
        # ms_since(100.1) with time()==101.1 -> 1000ms
        assert c.sum("faust.send.latency") == pytest.approx(1000.0)

    def test_on_send_error(self, *, mon, reader):
        mon.on_send_error(Mock(name="producer"), RuntimeError("boom"), 100.1)

        c = Collected(reader)
        assert c.value("faust.messages.send_errors") == 1
        assert c.count("faust.send.error_latency") == 1

    def test_assignments(self, *, mon, reader):
        assignor = Mock(name="assignor")
        mon.on_assignment_completed(assignor, {"time_start": 100.1})
        mon.on_assignment_error(assignor, {"time_start": 100.1}, RuntimeError())

        c = Collected(reader)
        assert c.value("faust.assignments", {"result": "completed"}) == 1
        assert c.value("faust.assignments", {"result": "error"}) == 1
        assert c.count("faust.assignment.latency", {"result": "completed"}) == 1
        assert c.count("faust.assignment.latency", {"result": "error"}) == 1

    def test_rebalance_lifecycle(self, *, mon, reader):
        app = Mock(name="app")
        state = mon.on_rebalance_start(app)
        c = Collected(reader)
        assert c.value("faust.rebalances.active") == 1

        mon.on_rebalance_return(app, state)
        c = Collected(reader)
        assert c.value("faust.rebalances.active") == 0
        assert c.value("faust.rebalances.recovering") == 1
        assert c.count("faust.rebalance.return_latency") == 1

        mon.on_rebalance_end(app, state)
        c = Collected(reader)
        assert c.value("faust.rebalances.recovering") == 0
        assert c.count("faust.rebalance.end_latency") == 1

    def test_on_commit_completed(self, *, mon, reader):
        mon.on_commit_completed(Mock(name="consumer"), 100.1)
        c = Collected(reader)
        assert c.count("faust.commit.latency") == 1
        assert c.sum("faust.commit.latency") == pytest.approx(1000.0)

    def test_offsets(self, *, mon, reader):
        mon.on_tp_commit({TP1: 10, TP2: 20})
        mon.track_tp_end_offset(TP1, 99)

        c = Collected(reader)
        assert c.value("faust.offset.committed", {"topic": "foo", "partition": 3}) == 10
        assert c.value("faust.offset.committed", {"topic": "bar", "partition": 4}) == 20
        assert c.value("faust.offset.end", {"topic": "foo", "partition": 3}) == 99

    def test_on_web_request_end(self, *, mon, reader):
        response = Mock(name="response")
        response.status = 200
        state = {"time_start": 100.1}
        mon.on_web_request_end(
            Mock(name="app"),
            Mock(name="request"),
            response,
            state,
            view=Mock(name="view"),
        )
        c = Collected(reader)
        assert c.value("faust.http.requests", {"status_code": 200}) == 1
        assert c.count("faust.http.latency") == 1

    def test_threaded_producer_buffer(self, *, mon, reader):
        mon.on_threaded_producer_buffer_processed(Mock(name="app"), 17)
        c = Collected(reader)
        assert c.value("faust.producer.buffer") == 17

    def test_count(self, *, mon, reader):
        mon.count("my_metric", 5)
        mon.count("my_metric", 3)
        c = Collected(reader)
        assert c.value("faust.custom.count", {"name": "my_metric"}) == 8
