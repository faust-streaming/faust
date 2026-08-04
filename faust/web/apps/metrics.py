"""HTTP endpoint exposing performance metrics.

Enabled with the :setting:`web_metrics_enabled` setting, which is off by
default::

    app = faust.App("myapp", web_metrics_enabled=True)

Unlike the statistics endpoints in :mod:`faust.web.apps.stats` this is
independent of :setting:`debug`, so it can be left on in production.

The payload is grouped by concern rather than being a flat dump of
:meth:`Monitor.asdict() <faust.sensors.monitor.Monitor.asdict>`, and adds two
things that monitor does not compute:

* **consumer lag** -- derived from the log end offsets and the offsets this
  worker has actually read, which is the number you usually want to alert on.
* **latency percentiles** -- monitor keeps raw deques of up to a few thousand
  samples; those are summarized here instead of serialized.

:mod:`faust.sensors.prometheus` serves the same underlying data in Prometheus
format on its own path; the two are independent.
"""

from typing import Any, Iterable, Mapping, MutableMapping, Optional

from faust import web

__all__ = ["Metrics", "blueprint"]

blueprint = web.Blueprint("metrics")


def _percentile(values: Iterable[float], percentile: float) -> Optional[float]:
    """Nearest-rank percentile, or :const:`None` when there is no data.

    Deliberately not :mod:`statistics.quantiles`: that raises on fewer than
    two samples, and a metrics endpoint should never fail because a worker has
    only just started.
    """
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


@blueprint.route("/", name="index")
class Metrics(web.View):
    """
    ---
    description: Worker performance metrics.
    tags:
    - Faust
    produces:
    - application/json
    """

    async def get(self, request: web.Request) -> web.Response:
        """Return JSON response with performance metrics."""
        return self.json(self.metrics())

    def metrics(self) -> Mapping[str, Any]:
        """Build the metrics payload."""
        app = self.app
        monitor = app.monitor
        return {
            "app": {
                "id": app.conf.id,
                "web_port": app.conf.web_port,
            },
            "throughput": self._throughput(monitor),
            "latency": self._latency(monitor),
            "consumer": self._consumer(monitor),
            "tables": {name: state.asdict() for name, state in monitor.tables.items()},
            "errors": {
                "send_errors": monitor.send_errors,
                "assignments_completed": monitor.assignments_completed,
                "assignments_failed": monitor.assignments_failed,
            },
            "topic_buffer_full": monitor._topic_buffer_full_dict(),
        }

    def _throughput(self, monitor: Any) -> Mapping[str, Any]:
        return {
            "messages_active": monitor.messages_active,
            "messages_received_total": monitor.messages_received_total,
            "messages_s": monitor.messages_s,
            "messages_sent": monitor.messages_sent,
            "events_active": monitor.events_active,
            "events_total": monitor.events_total,
            "events_s": monitor.events_s,
        }

    def _latency(self, monitor: Any) -> Mapping[str, Any]:
        return {
            "events_runtime_avg": monitor.events_runtime_avg,
            "commit_latency": _summarize(monitor.commit_latency),
            "send_latency": _summarize(monitor.send_latency),
            "assignment_latency": _summarize(monitor.assignment_latency),
            "rebalance_return_avg": monitor.rebalance_return_avg,
            "rebalance_end_avg": monitor.rebalance_end_avg,
            "http_response_latency_avg": monitor.http_response_latency_avg,
        }

    def _consumer(self, monitor: Any) -> Mapping[str, Any]:
        read = monitor._tp_read_offsets_dict()
        committed = monitor._tp_committed_offsets_dict()
        end = monitor._tp_end_offsets_dict()
        lag, lag_total = self._lag(read, end)
        return {
            "lag_total": lag_total,
            "lag_by_partition": lag,
            "read_offsets": read,
            "committed_offsets": committed,
            "end_offsets": end,
            "rebalances": monitor.rebalances,
        }

    @classmethod
    def _lag(cls, read: Mapping, end: Mapping) -> Any:
        """Consumer lag per partition, and the total across all partitions.

        A partition is skipped when either offset is unknown -- reporting it
        as zero lag would be a lie, and reporting it as the full end offset
        would spike alerts every time a worker starts.
        """
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
