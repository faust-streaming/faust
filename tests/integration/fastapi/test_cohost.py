"""End-to-end co-hosting check against a real FastAPI application.

Skipped unless the ``faust-streaming[fastapi]`` extra (and :pypi:`httpx`) are
installed, so it costs nothing in the default CI matrix.  No socket is bound:
``httpx.ASGITransport`` calls the ASGI application in-process, which is enough
to exercise the lifespan and prove that an endpoint can produce to a topic on
the same event loop.
"""

from unittest.mock import Mock

import pytest

import faust
from faust.contrib.asgi import faust_lifespan

fastapi = pytest.importorskip("fastapi")
httpx = pytest.importorskip("httpx")


@pytest.fixture()
def cohosted():
    """Declare app, topic, agent and API the way a module would."""
    app = faust.App(
        "test-fastapi-integration",
        store="memory://",
        cache="memory://",
        web_enabled=False,
    )
    greetings = app.topic("greetings", value_type=str)

    @app.agent(greetings)
    async def printer(stream):  # pragma: no cover - never started
        async for greeting in stream:
            yield greeting

    api = fastapi.FastAPI(lifespan=faust_lifespan(app, discover=False))

    @api.post("/greet")
    async def greet(text: str):
        await greetings.send(value=text)
        return {"ok": True}

    @api.get("/loop")
    async def loop_identity():
        import asyncio

        return {"same_loop": app.loop is asyncio.get_running_loop()}

    return app, api, greetings


async def test_endpoint_produces_on_the_same_loop(cohosted):
    app, api, greetings = cohosted
    # Stub the send path: this test is about loop identity and wiring, not
    # about talking to a broker.
    sent = []

    async def fake_send(**kwargs):
        sent.append(kwargs)
        return Mock(name="record_metadata")

    greetings.send = fake_send

    transport = httpx.ASGITransport(app=api)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        response = await client.post("/greet", params={"text": "hello"})
        assert response.status_code == 200
        assert response.json() == {"ok": True}

        loop_response = await client.get("/loop")
        assert loop_response.json() == {"same_loop": True}

    assert sent == [{"value": "hello"}]
    # The lifespan must have stopped the app again on exit.
    assert not app.started
