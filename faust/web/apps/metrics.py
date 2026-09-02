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

from typing import Any, Mapping

from faust import web
from faust.sensors.metrics import performance_metrics

__all__ = ["Metrics", "blueprint"]

blueprint = web.Blueprint("metrics")


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
        return performance_metrics(self.app)
