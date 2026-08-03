"""Co-host a Faust app with FastAPI (or any other ASGI application).

Faust and an ASGI server must share a single event loop: a Faust producer
created on one loop cannot be awaited from another.  This module provides the
two directions that need:

1. **The ASGI server drives.**  Your own :class:`~fastapi.FastAPI` application
   is served by uvicorn, and Faust is started and stopped from its lifespan::

        import faust
        from fastapi import FastAPI
        from faust.contrib.fastapi import faust_lifespan

        faust_app = faust.App("hello", broker="kafka://localhost:9092")
        greetings = faust_app.topic("greetings", value_type=str)

        api = FastAPI(lifespan=faust_lifespan(faust_app))

        @api.post("/greet")
        async def greet(text: str):
            await greetings.send(value=text)
            return {"ok": True}

   Run it with ``uvicorn myapp:api``.

2. **The Faust worker drives.**  ``faust worker`` runs as usual and your ASGI
   application is served from inside it, on the worker's own loop::

        from faust.contrib.fastapi import serve_asgi

        api = FastAPI()
        serve_asgi(faust_app, api, port=8000)

   Run it with ``faust -A myapp worker -l info``.

Nothing here imports :pypi:`fastapi` or :pypi:`starlette`, so it works just as
well with Starlette, Quart or Litestar.  :pypi:`uvicorn` is imported lazily and
only needed for :func:`serve_asgi`.

Install the optional dependencies with ``pip install faust-streaming[fastapi]``.
"""

import asyncio
import contextlib
import typing
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable, Mapping, Optional, Type

from mode import Service, get_logger

from faust.exceptions import ImproperlyConfigured
from faust.types import AppT

if typing.TYPE_CHECKING:
    from asyncio import AbstractEventLoop
else:
    AbstractEventLoop = Any

__all__ = [
    "LoopMismatch",
    "AsgiService",
    "bind_to_running_loop",
    "faust_app_running",
    "faust_lifespan",
    "serve_asgi",
]

logger = get_logger(__name__)

#: How long to wait for the ASGI server to finish serving during shutdown.
DEFAULT_SERVER_SHUTDOWN_TIMEOUT = 10.0

LOOP_MISMATCH_HELP = """\
The Faust app {app!r} is bound to a different event loop than the one now \
running, so anything it creates (producers, consumers, timers) would be \
unusable from here.

This almost always means something read the app's event loop while no loop was \
running -- usually at import time.  The common causes are:

  * accessing ``app.loop`` at module scope
  * accessing ``app.web``, ``app.transport`` or ``app.producer`` at module
    scope
  * calling ``asyncio.get_event_loop()`` yourself and passing it to
    ``faust.App(loop=...)``

Declare the app, its topics, agents and tables at module scope as usual, but \
leave the event loop alone -- Faust binds it when the app starts.\
"""


class LoopMismatch(ImproperlyConfigured):
    """The Faust app is bound to an event loop other than the running one."""


def bind_to_running_loop(app: AppT) -> AbstractEventLoop:
    """Bind ``app`` to the event loop that is currently running.

    Returns the running loop.

    Raises:
        RuntimeError: if there is no running event loop.
        LoopMismatch: if the app is already bound to a *different* loop.
    """
    running = asyncio.get_running_loop()
    # Read the private attribute on purpose: the public ``app.loop`` property
    # resolves -- and caches -- a loop as a side effect of being read, which is
    # exactly what we are trying to detect.
    current = getattr(app, "_loop", None)
    if current is None:
        app.loop = running
    elif current is not running:
        raise LoopMismatch(LOOP_MISMATCH_HELP.format(app=app.conf.id))
    return running


@asynccontextmanager
async def faust_app_running(
    app: AppT,
    *,
    finalize: bool = True,
    discover: Optional[bool] = None,
    stop_timeout: Optional[float] = None,
) -> AsyncIterator[AppT]:
    """Start ``app`` on the running loop, and stop it on exit.

    Use this when you have a lifespan of your own to compose with::

        @asynccontextmanager
        async def lifespan(api: FastAPI):
            async with faust_app_running(faust_app):
                ml_models["answer"] = load_model()
                yield
                ml_models.clear()

    Arguments:
        app: the Faust app to run.
        finalize: call :meth:`~faust.App.finalize` before starting.
        discover: run autodiscovery.  The default (:const:`None`) discovers
            when the app is configured with ``autodiscover``.
        stop_timeout: seconds to wait for a graceful stop.  :const:`None`
            waits indefinitely.

    The app is started with ``maybe_start()``, so this composes with an app
    that is already running (and will not stop one it did not start).
    """
    bind_to_running_loop(app)
    if finalize:
        app.finalize()
    if discover is None:
        discover = bool(app.conf.autodiscover)
    if discover:
        app.discover()

    started = await app.maybe_start()
    try:
        yield app
    finally:
        if started:
            if stop_timeout is None:
                await app.stop()
            else:
                await asyncio.wait_for(app.stop(), timeout=stop_timeout)


def maybe_instrument_opentelemetry(
    asgi_app: Any, enabled: Optional[bool] = None
) -> bool:
    """Instrument ``asgi_app`` with OpenTelemetry when that makes sense.

    ``enabled=None`` (the default) auto-detects: instrumentation is attached
    only when :pypi:`opentelemetry-instrumentation-fastapi` is installed *and*
    the application has configured a real ``TracerProvider``.  Until an SDK is
    configured the OpenTelemetry API is a no-op, so this never turns on
    telemetry an operator did not ask for.

    Pass ``False`` to opt out entirely, or ``True`` to instrument even when no
    SDK has been configured yet (useful if you configure it later).
    """
    if enabled is False:
        return False
    try:
        from faust.contrib.opentelemetry import instrument_asgi_app
    except Exception as exc:  # pragma: no cover
        logger.debug("OpenTelemetry: integration unavailable: %r", exc)
        return False
    return instrument_asgi_app(asgi_app, force=bool(enabled))


def faust_lifespan(
    app: AppT, *, opentelemetry: Optional[bool] = None, **kwargs: Any
) -> Callable[..., Any]:
    """Build an ASGI ``lifespan`` handler that runs ``app``.

    Accepts the same keyword arguments as :func:`faust_app_running`::

        api = FastAPI(lifespan=faust_lifespan(faust_app))

    If OpenTelemetry is installed and configured, the ASGI application is
    instrumented automatically; pass ``opentelemetry=False`` to opt out.
    """

    @asynccontextmanager
    async def lifespan(*args: Any, **lifespan_kwargs: Any) -> AsyncIterator[None]:
        # ASGI hands the application object to the lifespan handler, which is
        # the thing OpenTelemetry needs to wrap.
        if args:
            maybe_instrument_opentelemetry(args[0], opentelemetry)
        async with faust_app_running(app, **kwargs):
            # Yield None, not the app: Starlette treats a non-None lifespan
            # value as a state mapping to merge into the ASGI scope.
            yield

    return lifespan


@contextlib.contextmanager
def _no_signal_handlers() -> Any:
    """Stand-in for ``uvicorn.Server.capture_signals`` that installs nothing."""
    yield


def _disable_signal_handling(server: Any) -> None:
    """Stop uvicorn from taking over SIGINT/SIGTERM.

    :class:`mode.Worker` owns process signals; if uvicorn also installs
    handlers it wins (it is installed later) and Ctrl-C stops only the web
    server while the Faust worker keeps running.

    The hook moved in uvicorn 0.27 -- older versions call
    ``install_signal_handlers()``, newer ones use the ``capture_signals()``
    context manager -- so neutralize whichever is present.
    """
    if hasattr(server, "capture_signals"):
        server.capture_signals = _no_signal_handlers
    if hasattr(server, "install_signal_handlers"):
        server.install_signal_handlers = lambda *args, **kwargs: None


class AsgiService(Service):
    """Serve an ASGI application with uvicorn, on the Faust worker's loop.

    Usually created for you by :func:`serve_asgi`.
    """

    #: The ASGI application to serve.
    asgi_app: Any = None

    #: Interface to bind to.
    host: str = "0.0.0.0"  # nosec: B104

    #: Port to bind to.
    port: int = 8000

    #: Extra keyword arguments for :class:`uvicorn.Config`.
    uvicorn_options: Mapping[str, Any] = {}

    #: Seconds to wait for the server to finish serving on shutdown.
    server_shutdown_timeout: float = DEFAULT_SERVER_SHUTDOWN_TIMEOUT

    #: Instrument the ASGI app with OpenTelemetry.  :const:`None` auto-detects.
    opentelemetry: Optional[bool] = None

    def __init__(
        self,
        asgi_app: Any = None,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        uvicorn_options: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if asgi_app is not None:
            self.asgi_app = asgi_app
        if host is not None:
            self.host = host
        if port is not None:
            self.port = port
        if uvicorn_options is not None:
            self.uvicorn_options = uvicorn_options
        self._server: Any = None
        self._serve_fut: Optional[asyncio.Future] = None
        Service.__init__(self, **kwargs)

    def _create_server(self) -> Any:
        """Build the uvicorn server (overridden in tests)."""
        try:
            import uvicorn
        except ImportError as exc:  # pragma: no cover
            raise ImproperlyConfigured(
                "serve_asgi() requires uvicorn: "
                'pip install "faust-streaming[fastapi]"'
            ) from exc

        options = dict(self.uvicorn_options)
        # ``loop="none"`` keeps uvicorn from installing its own event loop
        # policy -- we are already running inside the worker's loop.
        options.setdefault("loop", "none")
        options.setdefault("lifespan", "on")
        options.setdefault("log_config", None)
        config = uvicorn.Config(
            self.asgi_app, host=self.host, port=self.port, **options
        )
        server = uvicorn.Server(config)
        _disable_signal_handling(server)
        return server

    async def on_start(self) -> None:
        """Start serving."""
        if self.asgi_app is None:
            raise ImproperlyConfigured("AsgiService requires an ASGI application")
        maybe_instrument_opentelemetry(self.asgi_app, self.opentelemetry)
        self._server = self._create_server()
        self._serve_fut = self.add_future(self._server.serve())

    async def on_stop(self) -> None:
        """Ask the server to exit and wait for it."""
        server, fut = self._server, self._serve_fut
        self._server = self._serve_fut = None
        if server is not None:
            server.should_exit = True
        if fut is not None:
            try:
                await asyncio.wait_for(fut, timeout=self.server_shutdown_timeout)
            except asyncio.TimeoutError:
                logger.warning(
                    "ASGI server did not stop within %ss",
                    self.server_shutdown_timeout,
                )
            except asyncio.CancelledError:  # pragma: no cover
                pass

    @property
    def label(self) -> str:
        """Return description of this service, used in logs."""
        return f"{type(self).__name__}: http://{self.host}:{self.port}"


def serve_asgi(
    app: AppT,
    asgi_app: Any,
    *,
    host: str = "0.0.0.0",  # nosec: B104
    port: int = 8000,
    **uvicorn_options: Any,
) -> Type[AsgiService]:
    """Serve ``asgi_app`` from inside the Faust worker.

    The server is registered as an extra app service, so it starts once the
    app is up -- after table recovery has finished, which is when it is
    actually safe to serve traffic::

        api = FastAPI()
        serve_asgi(faust_app, api, port=8000)

    Note this is separate from Faust's own web server (``@app.page`` and
    friends), which keeps running on ``web_port``.  Set ``web_enabled=False``
    if you do not want it.
    """
    cls: Type[AsgiService] = type(
        "FaustAsgiService",
        (AsgiService,),
        {
            "asgi_app": asgi_app,
            "host": host,
            "port": port,
            "uvicorn_options": uvicorn_options,
        },
    )
    app.service(cls)
    return cls
