.. _guide-testing:

=================================================
 Testing
=================================================

.. contents::
    :local:
    :depth: 2

.. module:: faust
    :noindex:

.. currentmodule:: faust

Basics
======

To test an agent when unit testing or functional testing, use the special
``Agent.test()`` mode to send items to the stream while processing it locally:

.. sourcecode:: python

    app = faust.App('test-example')

    class Order(faust.Record, serializer='json'):
        account_id: str
        product_id: str
        amount: int
        price: float

    orders_topic = app.topic('orders', value_type=Order)
    orders_for_account = app.Table('order-count-by-account', default=int)

    @app.agent(orders_topic)
    async def order(orders):
        async for order in orders.group_by(Order.account_id):
            orders_for_account[order.account_id] += 1
            yield order

Our agent reads a stream of orders and keeps a count of them by account id
in a distributed table also partitioned by the account id.

To test this agent we use ``order.test_context()``:

.. sourcecode:: python

    async def test_order():
        # start and stop the agent in this block
        async with order.test_context() as agent:
            order = Order(account_id='1', product_id='2', amount=1, price=300)
            # sent order to the test agents local channel, and wait
            # the agent to process it.
            await agent.put(order)
            # at this point the agent already updated the table
            assert orders_for_account[order.account_id] == 1
            await agent.put(order)
            assert orders_for_account[order.account_id] == 2

    async def run_tests():
        app.conf.store = 'memory://'   # tables must be in-memory
        await test_order()

    if __name__ == '__main__':
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(run_tests())


For the rest of this guide we'll be using :pypi:`pytest` and
:pypi:`pytest-asyncio` for our examples. If you're using a different
testing framework you may have to adapt them a bit to work.

Testing with :pypi:`pytest`
===========================

Testing that an agent sends to topic/calls another agent.
---------------------------------------------------------

When unit testing you should mock any dependencies of the agent being tested,

- If your agent calls another function: mock that function to verify it was
  called.

- If your agent sends a message to a topic: mock that topic to verify
  a message was sent.

- If your agent calls another agent: mock the other agent to verify it
  was called.

Here's an example agent that calls another agent:

.. sourcecode:: python

    import faust

    app = faust.App('example-test-agent-call')

    @app.agent()
    async def foo(stream):
        async for value in stream:
            await bar.send(value)
            yield value

    @app.agent()
    async def bar(stream):
        async for value in stream:
            yield value + 'YOLO'

To test these two agents you have to test them in isolation of each other:
first test ``foo`` with ``bar`` mocked, then in a different test do ``bar``:

.. sourcecode:: python

    import pytest
    from unittest.mock import Mock, patch

    from example import app, foo, bar

    @pytest.fixture(scope="function")
    def test_app(event_loop):
        """passing in event_loop helps avoid 'attached to a different loop' error"""
        app.loop = event_loop
        app.finalize()
        app.conf.store = 'memory://'
        app.flow_control.resume()
        return app

    @pytest.mark.asyncio()
    async def test_foo(test_app):
        with patch(__name__ + '.bar') as mocked_bar:
                mocked_bar.send = mock_coro()
            async with foo.test_context() as agent:
                await agent.put('hey')
                mocked_bar.send.assert_called_with('hey')

    def mock_coro(return_value=None, **kwargs):
        """Create mock coroutine function."""
        async def wrapped(*args, **kwargs):
            return return_value
        return Mock(wraps=wrapped, **kwargs)

    @pytest.mark.asyncio()
    async def test_bar(test_app):
        async with bar.test_context() as agent:
            event = await agent.put('hey')
            assert agent.results[event.message.offset] == 'heyYOLO'


You can put the `test_app` fixture into a [`conftest.py` file](https://docs.pytest.org/en/6.2.x/fixture.html#scope-sharing-fixtures-across-classes-modules-packages-or-session). If the fixture is not in the same file as the app's definition (which should be the case) you must import the app the fixture definition:

.. sourcecode:: python
    from example import app

    @pytest.fixture(scope="function")
    def test_app(event_loop):
        """passing in event_loop helps avoid 'attached to a different loop' error"""

        from example import app

        app.loop = event_loop
        app.finalize()
        app.conf.store = 'memory://'
        app.flow_control.resume()
        return app

.. note::

    The :pypi:`pytest-asyncio` extension must be installed to run these tests.
    If you don't have it use :program:`pip` to install it:

    .. sourcecode:: console

        $ pip install -U pytest-asyncio


Setting up a test suite
=======================

A faust app is normally a module-level singleton whose agents are attached to
it at import time, so a suite needs a little setup before those agents can be
started in isolation.

The four files below are a complete, working suite: copy them into an empty
directory, ``pip install faust-streaming pytest pytest-asyncio``, run
:program:`pytest`, and four tests pass.  The rest of this section explains
what each piece is doing.

The application under test -- :file:`myapp.py`
----------------------------------------------

The agent here is a *pure consumer*: it updates a table, calls a service and
forwards to another topic, and never yields.  See
:ref:`testing-sinkless-agents` for why that shape needs no sink.

.. sourcecode:: python

    import faust


    class Order(faust.Record, serializer='json'):
        account_id: str
        amount: int


    app = faust.App('orders-app', broker='kafka://localhost:9092')

    orders_topic = app.topic('orders', value_type=Order)
    shipped_topic = app.topic('shipped', value_type=Order)

    orders_for_account = app.Table('order-count-by-account', default=int)


    async def notify_warehouse(order: Order) -> None:
        """Pretend this calls an external HTTP service."""
        ...


    @app.agent(orders_topic)
    async def track_order(orders):
        # NOTE: no ``yield`` -- this agent is a pure consumer.
        async for order in orders:
            orders_for_account[order.account_id] += 1
            await notify_warehouse(order)
            await shipped_topic.send(value=order)

Enabling async tests -- :file:`pytest.ini`
------------------------------------------

These examples use :pypi:`pytest-asyncio` in ``auto`` mode, so async tests and
fixtures need no decorator:

.. sourcecode:: ini

    [pytest]
    asyncio_mode = auto

Fixtures -- :file:`conftest.py`
-------------------------------

.. sourcecode:: python

    import asyncio
    from unittest.mock import AsyncMock

    import pytest

    import myapp
    from myapp import app as _app


    @pytest.fixture()
    async def app():
        # Bind the app to the event loop pytest-asyncio created for this test.
        _app.loop = asyncio.get_running_loop()
        _app.finalize()
        _app.conf.store = 'memory://'
        _app.flow_control.resume()
        return _app


    @pytest.fixture()
    def shipped(monkeypatch):
        """Keep the agent from reaching a real broker."""
        send = AsyncMock()
        monkeypatch.setattr(myapp.shipped_topic, 'send', send)
        return send


    @pytest.fixture()
    def warehouse(monkeypatch):
        """Keep the agent from calling the real service."""
        notify = AsyncMock()
        monkeypatch.setattr(myapp, 'notify_warehouse', notify)
        return notify

The tests -- :file:`test_myapp.py`
----------------------------------

.. sourcecode:: python

    from myapp import Order, orders_for_account, track_order


    async def test_counts_orders_per_account(app, shipped, warehouse):
        async with track_order.test_context() as agent:
            await agent.put(Order(account_id='A', amount=1))
            assert orders_for_account['A'] == 1

            await agent.put(Order(account_id='A', amount=2))
            assert orders_for_account['A'] == 2


    async def test_notifies_warehouse(app, shipped, warehouse):
        async with track_order.test_context() as agent:
            order = Order(account_id='B', amount=3)
            await agent.put(order)
            warehouse.assert_awaited_once_with(order)


    async def test_forwards_to_shipped_topic(app, shipped, warehouse):
        async with track_order.test_context() as agent:
            order = Order(account_id='C', amount=4)
            await agent.put(order)
            shipped.assert_awaited_once_with(value=order)


    async def test_results_records_input_values(app, shipped, warehouse):
        async with track_order.test_context() as agent:
            order = Order(account_id='D', amount=5)
            await agent.put(order)
            # For a sink-less agent ``results`` holds what went IN.
            assert agent.results[0] == order

What the ``app`` fixture is doing
----------------------------------

Each line earns its place:

``_app.loop``
    Re-binds the app to the event loop :pypi:`pytest-asyncio` created for this
    test.  A module-level app otherwise stays bound to the loop of whichever
    test ran first, and every later test fails with ``RuntimeError: Event loop
    is closed``.

``_app.finalize()``
    Completes app configuration.  Normally the worker does this for you.

``_app.conf.store = 'memory://'``
    Tables must be in-memory in tests; the default (RocksDB) wants a real data
    directory.  Faust warns that the setting arrives after your topics and
    agents were declared -- expected here and harmless, since no table has
    started yet.  Set ``store`` on the :class:`~faust.App` itself if you would
    rather not see the warning.

``_app.flow_control.resume()``
    Stream queues start out suspended.  Omit this and the agent never receives
    anything, so ``put()`` hangs forever.

.. note::

    The ``test_app`` fixture shown earlier does the same job, but reaches the
    loop by requesting :pypi:`pytest-asyncio`'s ``event_loop`` fixture.  Recent
    releases deprecate that (*"Asynchronous fixtures and test functions should
    use asyncio.get_running_loop() instead"*), so prefer the async fixture
    above in new test suites.

Mock anything that leaves the process
--------------------------------------

``test_context()`` feeds the agent through a local channel, but it does *not*
stub out the rest of your app.  If the agent sends to a topic or calls an
external service it will genuinely try to, and the test fails with::

    aiokafka.errors.KafkaConnectionError: Unable to bootstrap from [('localhost', 9092, ...)]

That is what the ``shipped`` and ``warehouse`` fixtures are for: give every
outbound dependency its own fixture, so each test both stays offline and gets
a mock it can assert on.

.. _testing-sinkless-agents:

Testing agents that don't yield
===============================

Every earlier example ends in ``yield`` and reads its output back through
``agent.results``.  Plenty of real agents never yield: they update a table,
call a service, or forward to another topic and stop there.  Such an agent
cannot use sinks at all -- attaching one raises
``ImproperlyConfigured('Agent must yield to use sinks')`` -- yet
``test_context()`` tests them perfectly well.  ``track_order`` above is
exactly that shape.

Because nothing is yielded, you assert on what the agent *did* rather than on
what it returned: the table it wrote (``test_counts_orders_per_account``), the
service it called (``test_notifies_warehouse``), and the message it forwarded
(``test_forwards_to_shipped_topic``).

What ``agent.results`` holds
----------------------------

``results`` is populated for both kinds of agent, but it does not mean the
same thing in each case:

===================== =============================================
Agent                 ``agent.results[offset]`` contains
===================== =============================================
yields                the value the agent **yielded** (its output)
does not yield        the value that was **sent in** (its input)
===================== =============================================

Which row applies is decided from the running agent, not from how its function
is written.  An agent implemented as a callable object with an
async-generator ``__call__``, or as a plain function returning an async
generator, yields just as much as an ``async def`` that yields directly, and
its ``results`` hold its output.

There is no output to capture for a sink-less agent, so faust records the
incoming value instead.  That still makes ``results`` useful for confirming
which values reached the agent -- as ``test_results_records_input_values``
does above -- but do not read it expecting a return value.

.. admonition:: When ``put()`` returns

    ``await agent.put(value)`` waits for the value to be picked up by the
    agent, and for an agent body that does not ``await`` anything mid-loop the
    side effects are already visible when ``put()`` returns -- which is why the
    assertions above can follow it directly.

    If the body *does* await something (an HTTP call, ``asyncio.sleep``, a real
    ``send``), ``put()`` can return before the body has finished with that
    value, and asserting immediately will be flaky.  Wait for the effect rather
    than assuming it -- for example by leaving the ``async with`` block, since
    stopping the agent drains what is in flight:

    .. sourcecode:: python

        async def test_slow_agent(app):
            processed = []
            async with slow_agent.test_context() as agent:
                await agent.put('a')
            # the context manager stopped the agent: work is finished
            assert processed == ['a']

Testing and windowed tables
===========================

If your table is windowed and you want to verify that the value for a key is
correctly set, use ``table[k].current(event)`` to get the value placed within
the window of the current event:

.. sourcecode:: python

    import faust
    import pytest

    @pytest.mark.asyncio()
    async def test_process_order():
        app.conf.store = 'memory://'
        async with process_order.test_context() as agent:
            order = Order(account_id='1', product_id='2', amount=1, price=300)
            event = await agent.put(order)

            # windowed table: we select window relative to the current event
            assert orders_for_account['1'].current(event) == 1

            # in the window 3 hours ago there were no orders:
            assert orders_for_account['1'].delta(3600 * 3, event)


    class Order(faust.Record, serializer='json'):
        account_id: str
        product_id: str
        amount: int
        price: float

    app = faust.App('test-example')
    orders_topic = app.topic('orders', value_type=Order)

    # order count within the last hour (window is a 1-hour TumblingWindow).
    orders_for_account = app.Table(
        'order-count-by-account', default=int,
    ).tumbling(3600).relative_to_stream()

    @app.agent(orders_topic)
    async def process_order(orders):
        async for order in orders.group_by(Order.account_id):
            orders_for_account[order.account_id] += 1
            yield order
