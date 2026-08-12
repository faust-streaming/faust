"""Framework-neutral worker performance metrics."""

from typing import Any, Iterable, Mapping, MutableMapping, Optional

from faust.types import AppT

__all__ = ["performance_metrics"]


def _percentile(values: Iterable[float], percentile: float) -> Optional[float]:
    """Return the nearest-rank percentile, or None when there is no data."""
    ordered = sorted(values)
    if not ordered:
        return None
    index = int(round(percentile * (len(ordered) - 1)))
    return ordered[index]


def _summarize(values: Iterable[float]) -> Mapping[str, Optional[float]]:
    ordered = list(values)
    return {
        "count": len(ordered),
        "p50": _percentile(ordered, 0.50),
        "p95": _percentile(ordered, 0.95),
        "max": max(ordered) if ordered else None,
    }


def _lag(read: Mapping, end: Mapping) -> Any:
    """Return consumer lag per partition and across all partitions."""
    lag: MutableMapping[str, MutableMapping[int, int]] = {}
    total = 0
    for topic, end_partitions in end.items():
        read_partitions = read.get(topic) or {}
        for partition, end_offset in end_partitions.items():
            read_offset = read_partitions.get(partition)
            if read_offset is None or end_offset is None:
                continue
            behind = max(0, end_offset - read_offset)
            lag.setdefault(topic, {})[partition] = behind
            total += behind
    return lag, total


def performance_metrics(app: AppT) -> Mapping[str, Any]:
    """Build the worker metrics payload for any web framework.

    The result contains only JSON-compatible values, so frameworks can return
    it directly from their own JSON response helpers.
    """
    monitor = app.monitor
    read = monitor._tp_read_offsets_dict()
    committed = monitor._tp_committed_offsets_dict()
    end = monitor._tp_end_offsets_dict()
    lag, lag_total = _lag(read, end)
    return {
        "app": {
            "id": app.conf.id,
            "web_port": app.conf.web_port,
        },
        "throughput": {
            "messages_active": monitor.messages_active,
            "messages_received_total": monitor.messages_received_total,
            "messages_s": monitor.messages_s,
            "messages_sent": monitor.messages_sent,
            "events_active": monitor.events_active,
            "events_total": monitor.events_total,
            "events_s": monitor.events_s,
        },
        "latency": {
            "events_runtime_avg": monitor.events_runtime_avg,
            "commit_latency": _summarize(monitor.commit_latency),
            "send_latency": _summarize(monitor.send_latency),
            "assignment_latency": _summarize(monitor.assignment_latency),
            "rebalance_return_avg": monitor.rebalance_return_avg,
            "rebalance_end_avg": monitor.rebalance_end_avg,
            "http_response_latency_avg": monitor.http_response_latency_avg,
        },
        "consumer": {
            "lag_total": lag_total,
            "lag_by_partition": lag,
            "read_offsets": read,
            "committed_offsets": committed,
            "end_offsets": end,
            "rebalances": monitor.rebalances,
        },
        "tables": {name: state.asdict() for name, state in monitor.tables.items()},
        "errors": {
            "send_errors": monitor.send_errors,
            "assignments_completed": monitor.assignments_completed,
            "assignments_failed": monitor.assignments_failed,
        },
        "topic_buffer_full": monitor._topic_buffer_full_dict(),
    }
