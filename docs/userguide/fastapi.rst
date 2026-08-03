.. _guide-fastapi:

=================================================
 FastAPI and other ASGI applications
=================================================

.. module:: faust.contrib.fastapi

.. contents::
    :local:
    :depth: 2

.. _fastapi-basics:

Basics
======

Faust and an ASGI server can share a single process, so an HTTP endpoint can
produce to a Kafka topic directly:

.. sourcecode:: python

    import faust
    from fastapi import FastAPI
    from faust.contrib.fastapi import faust_lifespan

    faust_app = faust.App("hello", broker="kafka://localhost:9092")
    greetings = faust_app.topic("greetings", value_type=str)

    api = FastAPI(lifespan=faust_lifespan(faust_app))

    @faust_app.agent(greetings)
    async def print_greetings(stream):
        async for greeting in stream:
            print(greeting)

    @api.post("/greet")
    async def greet(text: str):
        await greetings.send(value=text)
        return {"ok": True}

Run it with:

.. sourcecode:: console

    $ uvicorn myapp:api

Install the optional dependencies with:

.. sourcecode:: console

    $ pip install "faust-streaming[fastapi]"

Nothing in :mod:`faust.contrib.fastapi` imports :pypi:`fastapi` or
:pypi:`starlette`, so it works equally well with Starlette, Quart or Litestar.

.. _fastapi-one-loop:

One process, one event loop
===========================

This is the whole reason the module exists.

A Faust app resolves its event loop the first time something asks for it, and
everything it builds afterwards -- producers, consumers, timers, table
managers -- belongs to that loop.  An ASGI server such as :pypi:`uvicorn`
creates its *own* loop with :func:`asyncio.run`.  If the app has already
resolved a different loop by then, every one of those objects is attached to a
loop that will never run, and you get::

    AssertionError: Please create objects with the same loop as running with
    RuntimeError: Task ... got Future ... attached to a different loop

:func:`faust_lifespan` and :func:`faust_app_running` bind the app to the loop
that is actually running before starting it, so this cannot happen.

.. admonition:: Do not touch ``app.loop`` at import time

    Declare the app, its topics, agents and tables at module scope as usual --
    that is supported and is what the examples do.  What you must *not* do is
    ask the app for its event loop before the server starts.  In practice that
    means avoiding, at module scope:

    * ``app.loop``
    * ``app.web``, ``app.transport``, ``app.producer``
    * passing your own loop to ``faust.App(loop=...)``

    If the app is already bound to another loop,
    :func:`bind_to_running_loop` raises :exc:`LoopMismatch` with a message
    naming the likely cause, rather than letting it fail later and less
    legibly.

.. _fastapi-composing:

Composing with your own lifespan
================================

When you have setup of your own, use :func:`faust_app_running` -- it is the
context manager :func:`faust_lifespan` is built from:

.. sourcecode:: python

    from contextlib import asynccontextmanager
    from faust.contrib.fastapi import faust_app_running

    @asynccontextmanager
    async def lifespan(api: FastAPI):
        async with faust_app_running(faust_app):
            ml_models["answer"] = load_model()
            yield
            ml_models.clear()

    api = FastAPI(lifespan=lifespan)

The app is started with ``maybe_start()``, so this composes safely with an app
that is already running, and will not stop one it did not start.

.. _fastapi-worker:

Running under ``faust worker``
==============================

The other direction: keep ``faust worker`` as your entry point and let it serve
your ASGI application on its own loop.

.. sourcecode:: python

    from faust.contrib.fastapi import serve_asgi

    api = FastAPI()
    serve_asgi(faust_app, api, port=8000)

.. sourcecode:: console

    $ faust -A myapp worker -l info

:func:`serve_asgi` registers the server as an app service, so it starts only
once the app is up -- after table recovery has finished, which is when it is
actually safe to serve traffic.  Extra keyword arguments are passed through to
:class:`uvicorn.Config`.

.. admonition:: ``faust -A`` looks for ``app``

    The ``-A`` option imports the module and looks for an attribute named
    ``app``.  Bind that name to the **Faust** app, not to the ``FastAPI``
    object::

        app = faust_app = faust.App("myapp", ...)
        api = FastAPI()

    Naming the FastAPI object ``app`` -- the FastAPI convention -- is what
    makes ``faust -A`` fail to find your app.

.. _fastapi-web-server:

Faust's own web server
======================

Faust ships its own :pypi:`aiohttp` server for ``@app.page``, table routing and
``/metrics``.  It is independent of anything here and keeps listening on
``web_port`` (6066 by default).  Two servers in one process is fine; if you do
not want Faust's, turn it off:

.. sourcecode:: python

    app = faust.App("myapp", web_enabled=False)

There is no ASGI *driver* for Faust's own views -- ``@app.page`` and
``@app.table_route`` are still served by aiohttp.  This page is about
co-hosting your application, not about replacing that.

.. _fastapi-opentelemetry:

OpenTelemetry
=============

.. sourcecode:: console

    $ pip install "faust-streaming[opentelemetry]"

With the instrumentation packages installed, most of a distributed trace works
already: :pypi:`opentelemetry-instrumentation-aiokafka` wraps the same
``AIOKafkaProducer.send`` and ``AIOKafkaConsumer.getmany`` calls that Faust's
driver uses, so an HTTP request that produces to Kafka carries its trace
context onto the wire in a ``traceparent`` header.

Two things Faust adds:

**Your FastAPI application is instrumented automatically.**
:func:`faust_lifespan` and :func:`serve_asgi` attach
``FastAPIInstrumentor`` when OpenTelemetry is installed *and* the application
has configured a real ``TracerProvider``.  Until an SDK is configured the
OpenTelemetry API is a no-op, so nothing is enabled behind your back.  Pass
``opentelemetry=False`` to opt out, or ``True`` to force it.  Applications
already instrumented (for example under ``opentelemetry-instrument``) are left
alone rather than double-wrapped.

**The consumer-to-agent hop is bridged.**  Faust's consumer runs in its own
thread, and :mod:`contextvars` do not cross threads -- so the ``receive`` span
created inside ``getmany`` is closed before your agent ever runs, leaving the
agent's work unparented.  Register the sensor to close that gap:

.. sourcecode:: python

    from faust.contrib.opentelemetry import setup_opentelemetry

    setup_opentelemetry(app)

It extracts the trace context from each message's headers and opens a
``{topic} process`` span that stays current for as long as the stream is
processing the event, so everything the agent does nests underneath it.  The
resulting trace reads:

.. sourcecode:: text

    FastAPI server span
      └─ {topic} send        (PRODUCER, from opentelemetry-instrumentation-aiokafka)
          └─ {topic} process (CONSUMER, from faust.contrib.opentelemetry)
              └─ ...whatever your agent does

.. admonition:: Do not run this alongside the opentracing sensor

    :class:`faust.sensors.distributed_tracing.TracingSensor` also injects trace
    headers into Kafka messages.  Running both puts two ``traceparent`` headers
    on the wire.  :func:`setup_opentelemetry` warns if it sees one registered.
    Note also that the OpenTracing bridge was deprecated upstream in March
    2026; new work should target OpenTelemetry directly.

.. _fastapi-examples:

Examples
========

Two complete examples ship with Faust:

``examples/fastapi_example.py``
    A single file -- ``hello_world.py`` plus a FastAPI application.

``examples/fastapi_project/``
    The same thing as a package, with agents, tables, timers and routers in
    their own modules.  ``main.py`` is served by uvicorn; ``worker_main.py``
    shows the same API served from inside ``faust worker``.

.. _fastapi-caveats:

Caveats
=======

* ``producer_threaded=True`` spawns a producer with its own thread and loop.
  It is untested in a co-hosted process and is not supported here yet.
* ``uvicorn --reload`` and ``--workers`` fork or re-exec the process.  Faust
  is not designed to be forked; use a single worker.
* Faust's own web views stay on aiohttp, as described above.
