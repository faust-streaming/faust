"""Regression tests for co-hosting Faust with an ASGI server (FastAPI).

Faust apps are declared at module scope, but under an ASGI server such as
uvicorn the app is *started* from a loop created later by :func:`asyncio.run`.
If declaring an agent/table binds the App to whatever loop happens to exist at
import time, every service built during startup inherits that dead loop and the
worker fails with either::

    AssertionError: Please create objects with the same loop as running with
    RuntimeError: Task ... got Future ... attached to a different loop

See issues #322, #435 and #448.

These tests reproduce that split without needing Kafka: the ``declared_app``
fixture is *synchronous*, so it runs outside the running loop exactly like an
import does, while the ``async def`` tests run inside the per-test loop.
"""

import asyncio

import pytest

import faust


@pytest.fixture()
def declared_app():
    """Build an app the way a module does at import time -- no loop running.

    Returns a ``(app, topic, agent, table)`` tuple.  Declaring the agent is
    what matters most: ``@app.agent`` reaches ``app.topics`` and therefore
    builds the conductor and the transport, which is the path that used to
    pin the app.
    """
    app = faust.App(
        "test-fastapi-cohost",
        store="memory://",
        cache="memory://",
    )
    topic = app.topic("greetings", value_type=str)

    @app.agent(topic)
    async def printer(stream):  # pragma: no cover - never started
        async for greeting in stream:
            yield greeting

    table = app.Table("cohost-tbl", default=int)
    return app, topic, printer, table


def test_declaration_does_not_bind_loop(declared_app):
    """Declaring agents/tables must not resolve an event loop.

    This is the regression lock.  Every other failure mode in #448 follows
    from the app being bound here, at import time, to a loop that will never
    be run.
    """
    app, _topic, agent, table = declared_app

    assert app._loop is None
    assert agent._loop is None
    assert table._loop is None
    assert app.tables._loop is None
    assert app.agents._loop is None


def test_declaration_does_not_bind_transport_loop(declared_app):
    """The transport is reachable from import-time code, so it must be lazy."""
    app, *_ = declared_app

    # Touching ``app.topics`` builds the conductor -> transport.  Neither may
    # resolve a loop, and neither may pin the app.
    assert app.topics is not None
    assert app.transport._loop is None
    assert app._loop is None


async def test_binds_to_running_loop(declared_app):
    """First access from inside a running loop must resolve to *that* loop."""
    app, _topic, agent, table = declared_app
    running = asyncio.get_running_loop()

    assert app.loop is running
    assert agent.loop is running
    assert table.loop is running


async def test_transport_uses_running_loop(declared_app):
    """The transport -- and what it builds -- must land on the running loop.

    Constructing a ``Transport`` performs no I/O and opens no socket, so this
    can assert the exact invariant that #448 violated without a broker.
    """
    app, *_ = declared_app
    running = asyncio.get_running_loop()

    assert app._new_transport().loop is running
    assert app._new_producer_transport().loop is running


async def test_transport_loop_is_settable(declared_app):
    """``Transport.loop`` stayed writable when it became lazy."""
    app, *_ = declared_app
    transport = app._new_transport()
    sentinel = asyncio.get_running_loop()

    transport.loop = sentinel
    assert transport.loop is sentinel


async def test_explicit_loop_argument_still_wins(declared_app):
    """Passing ``loop=`` to a Transport must still override the lazy lookup."""
    app, *_ = declared_app
    running = asyncio.get_running_loop()
    transport = type(app.transport)(app.conf.broker_consumer, app, loop=running)

    assert transport.loop is running
