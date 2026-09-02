from faust.sensors.metrics import performance_metrics
from faust.types import TP


def test_performance_metrics_is_framework_neutral(*, app):
    monitor = app.monitor
    monitor.messages_received_total = 10
    monitor.events_s = 3
    monitor.commit_latency.extend([0.01, 0.03, 0.02])
    monitor.tp_read_offsets[TP("orders", 0)] = 90
    monitor.tp_end_offsets[TP("orders", 0)] = 100

    payload = performance_metrics(app)

    assert payload["throughput"]["messages_received_total"] == 10
    assert payload["throughput"]["events_s"] == 3
    assert payload["latency"]["commit_latency"] == {
        "count": 3,
        "p50": 0.02,
        "p95": 0.03,
        "max": 0.03,
    }
    assert payload["consumer"]["lag_total"] == 10
    assert payload["consumer"]["lag_by_partition"] == {"orders": {0: 10}}


def test_performance_metrics_skips_unknown_read_offsets(*, app):
    app.monitor.tp_end_offsets[TP("orders", 0)] = 100

    payload = performance_metrics(app)

    assert payload["consumer"]["lag_total"] == 0
    assert payload["consumer"]["lag_by_partition"] == {}
