import pytest

from faust.types import TP

pytestmark = pytest.mark.app(web_metrics_enabled=True)


@pytest.fixture()
def traffic(app):
    """Populate the monitor as if the worker had been running."""
    monitor = app.monitor
    monitor.messages_received_total = 1_048_576
    monitor.messages_s = 812
    monitor.messages_sent = 1_048_570
    monitor.events_total = 1_048_576
    monitor.events_s = 812
    monitor.events_runtime_avg = 0.0031
    monitor.commit_latency.extend([0.010, 0.014, 0.031, 0.012])
    monitor.send_latency.extend([0.002, 0.004, 0.0021])
    monitor.tp_read_offsets[TP("withdrawals", 0)] = 91_240
    monitor.tp_end_offsets[TP("withdrawals", 0)] = 92_042
    monitor.tp_committed_offsets[TP("withdrawals", 0)] = 91_180
    return monitor


async def test_metrics(web_client, traffic):
    async with await web_client as client:
        resp = await client.get("/performance/")
        assert resp.status == 200
        payload = await resp.json()

    assert payload["app"]["id"]
    assert payload["throughput"]["messages_received_total"] == 1_048_576
    assert payload["throughput"]["events_s"] == 812
    assert payload["latency"]["events_runtime_avg"] == 0.0031


async def test_latency_is_summarized_not_dumped(web_client, traffic):
    """Monitor keeps raw deques; the endpoint must not serialize them."""
    async with await web_client as client:
        payload = await (await client.get("/performance/")).json()

    commit = payload["latency"]["commit_latency"]
    assert commit["count"] == 4
    assert commit["p50"] == 0.014
    assert commit["max"] == 0.031
    assert not isinstance(commit, list)


async def test_latency_with_no_samples(web_client):
    """A worker that just started must not 500."""
    async with await web_client as client:
        resp = await client.get("/performance/")
        assert resp.status == 200
        payload = await resp.json()

    assert payload["latency"]["commit_latency"] == {
        "count": 0,
        "p50": None,
        "p95": None,
        "max": None,
    }


async def test_consumer_lag(web_client, traffic):
    async with await web_client as client:
        payload = await (await client.get("/performance/")).json()

    consumer = payload["consumer"]
    assert consumer["lag_total"] == 802
    assert consumer["lag_by_partition"] == {"withdrawals": {"0": 802}}
    assert consumer["read_offsets"] == {"withdrawals": {"0": 91_240}}
    assert consumer["committed_offsets"] == {"withdrawals": {"0": 91_180}}


async def test_lag_skips_partitions_with_unknown_read_offset(web_client, traffic):
    """Reporting a fresh partition as fully lagged would spike alerts."""
    traffic.tp_end_offsets[TP("withdrawals", 1)] = 500

    async with await web_client as client:
        payload = await (await client.get("/performance/")).json()

    lag = payload["consumer"]["lag_by_partition"]["withdrawals"]
    assert "1" not in lag
    assert payload["consumer"]["lag_total"] == 802


async def test_tables_are_included(web_client, app):
    app.Table("counts")
    app.monitor.on_table_get(app.tables["counts"], "k")

    async with await web_client as client:
        payload = await (await client.get("/performance/")).json()

    assert payload["tables"]["counts"]["keys_retrieved"] == 1


@pytest.mark.app(web_metrics_enabled=False)
async def test_disabled_by_default(web_client):
    async with await web_client as client:
        resp = await client.get("/performance/")
        assert resp.status == 404
