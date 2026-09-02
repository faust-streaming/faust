.. _guide-asgi:

=================================================
 Pluggable web frameworks and ASGI
=================================================

.. module:: faust.contrib.asgi

.. contents::
    :local:
    :depth: 2

.. _asgi-basics:

Basics
======

Faust and an ASGI server can share a single process, so an HTTP endpoint can
produce to a Kafka topic directly:

.. sourcecode:: python

    import faust
    from fastapi import FastAPI
    from faust.contrib.asgi import faust_lifespan

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

Nothing in :mod:`faust.contrib.asgi` imports :pypi:`fastapi` or
:pypi:`starlette`, so it works equally well with FastAPI, Starlette, Quart,
Litestar, Django ASGI, or any other ASGI callable. Use
``faust-streaming[asgi]`` when your selected framework is already installed
and you only need the ASGI server integration.

.. _asgi-django:

Django and applications without a lifespan hook
================================================

Django exposes a standard ASGI callable through
:func:`django.core.asgi.get_asgi_application`, but it does not accept the
constructor-level lifespan handler used in the FastAPI example. Wrap that
callable in :class:`FaustLifespanMiddleware` in the project's ``asgi.py``:

.. sourcecode:: python

    import os

    from django.core.asgi import get_asgi_application
    from faust.contrib.asgi import FaustLifespanMiddleware

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
    django_application = get_asgi_application()

    from myproject.faust import app as faust_app

    application = FaustLifespanMiddleware(django_application, faust_app)

Run the combined application with any ASGI server:

.. sourcecode:: console

    $ uvicorn myproject.asgi:application

The middleware implements the standard ASGI lifespan protocol. It starts
Faust before replying with ``lifespan.startup.complete`` and stops Faust
before replying with ``lifespan.shutdown.complete``. HTTP and WebSocket
scopes pass to Django unchanged, and Faust never imports Django.

The wrapper owns the lifespan scope instead of forwarding it to the inner
application. Use :func:`faust_lifespan` or :func:`faust_app_running` when the
selected framework already has startup and shutdown work that must be
composed with Faust.

.. _asgi-one-loop:

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

.. _asgi-composing:

Composing with your own lifespan
================================

When you have setup of your own, use :func:`faust_app_running` -- it is the
context manager :func:`faust_lifespan` is built from:

.. sourcecode:: python

    from contextlib import asynccontextmanager
    from faust.contrib.asgi import faust_app_running

    @asynccontextmanager
    async def lifespan(api: FastAPI):
        async with faust_app_running(faust_app):
            ml_models["answer"] = load_model()
            yield
            ml_models.clear()

    api = FastAPI(lifespan=lifespan)

The app is started with ``maybe_start()``, so this composes safely with an app
that is already running, and will not stop one it did not start.

.. _asgi-worker:

Running under ``faust worker``
==============================

The other direction: keep ``faust worker`` as your entry point and let it serve
your ASGI application on its own loop.

.. sourcecode:: python

    from faust.contrib.asgi import serve_asgi

    api = FastAPI()
    serve_asgi(faust_app, api)

.. sourcecode:: console

    $ faust -A myapp worker -l info

:func:`serve_asgi` makes the ASGI application the worker's web application. It
replaces the legacy aiohttp server, starts only after table recovery has
finished, and obeys :setting:`web_enabled` and ``--without-web``. By default it
binds to :setting:`web_bind` and :setting:`web_port`; extra keyword arguments
are passed through to :class:`uvicorn.Config`.

.. admonition:: ``faust -A`` looks for ``app``

    The ``-A`` option imports the module and looks for an attribute named
    ``app``.  Bind that name to the **Faust** app, not to the ``FastAPI``
    object::

        app = faust_app = faust.App("myapp", ...)
        api = FastAPI()

    Naming the FastAPI object ``app`` -- the FastAPI convention -- is what
    makes ``faust -A`` fail to find your app.

.. _asgi-web-server:

Replacing ``faust.web``
=======================

Faust keeps its aiohttp-based :mod:`faust.web` stack as the compatibility
default, but applications are no longer required to use it. Registering a web
server service replaces that stack completely:

.. sourcecode:: python

    from mode import Service

    @faust_app.web_server
    class MyFrameworkServer(Service):
        async def on_start(self):
            # Start Sanic, Tornado, or another framework here.
            ...

        async def on_stop(self):
            ...

Faust only owns the service lifecycle. The service owns its framework,
application object, routes, and HTTP server. It starts after table recovery,
uses the same event loop as the worker, and is disabled by the same
``web_enabled`` switch as the compatibility server.

:func:`serve_asgi` is the ready-made implementation of this hook for ASGI.
Because the framework now owns routing, ``@app.page``, ``@app.table_route``,
and Faust's built-in aiohttp endpoints are not mounted when a custom server is
registered. Define their replacements in your chosen framework. The new
performance payload is framework-neutral, so it can be exposed directly:

.. sourcecode:: python

    from faust.sensors.metrics import performance_metrics

    @api.get("/performance")
    async def performance():
        return performance_metrics(faust_app)

.. _asgi-opentelemetry:

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

.. _asgi-examples:

Examples
========

Two complete examples ship with Faust:

``examples/fastapi_example.py``
    A single file -- ``hello_world.py`` plus a FastAPI application.

``examples/fastapi_project/``
    The same thing as a package, with agents, tables, timers and routers in
    their own modules.  ``main.py`` is served by uvicorn; ``worker_main.py``
    shows the same API served from inside ``faust worker``.

.. _asgi-caveats:

Caveats
=======

* **A topic must exist when the worker starts for its agent to consume from
  it.**  This bites co-hosted apps particularly often, because they typically
  produce to and consume from the same topic: on a first run against a fresh
  cluster the topic does not exist yet, the agent subscribes to nothing, and
  messages produced by your endpoints are written but never processed until
  the process is restarted.  Nothing is lost -- the backlog is picked up on
  the next start -- but the first run looks like the agent is broken.

  This is not specific to co-hosting; ``faust worker`` behaves the same way.
  Create your topics ahead of time (or restart once) when bootstrapping a new
  environment.

* ``producer_threaded=True`` spawns a producer with its own thread and loop.
  It is untested in a co-hosted process and is not supported here yet.
* ``uvicorn --reload`` and ``--workers`` fork or re-exec the process.  Faust
  is not designed to be forked; use a single worker.
