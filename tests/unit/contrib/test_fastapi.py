import asyncio
from unittest.mock import Mock, patch

import pytest

import faust
from faust.contrib.fastapi import (
    AsgiService,
    LoopMismatch,
    _disable_signal_handling,
    bind_to_running_loop,
    faust_app_running,
    faust_lifespan,
    maybe_instrument_opentelemetry,
    serve_asgi,
)
from faust.exceptions import ImproperlyConfigured


@pytest.fixture()
def app():
    """An app declared the way a module declares one -- no loop running."""
    return faust.App("test-contrib-fastapi", store="memory://", cache="memory://")


def _completed_future(result=None):
    """A future that is already done -- awaitable without scheduling a task."""
    fut = asyncio.get_running_loop().create_future()
    fut.set_result(result)
    return fut


def _started(value):
    """Stand-in for ``App.maybe_start()``, which returns "did I start it?"."""
    return _completed_future(value)


class Test_bind_to_running_loop:
    def test_requires_a_running_loop(self, *, app):
        with pytest.raises(RuntimeError):
            bind_to_running_loop(app)

    async def test_binds_unbound_app(self, *, app):
        running = asyncio.get_running_loop()
        assert app._loop is None

        assert bind_to_running_loop(app) is running
        assert app._loop is running

    async def test_is_idempotent(self, *, app):
        running = asyncio.get_running_loop()
        bind_to_running_loop(app)

        assert bind_to_running_loop(app) is running
        assert app._loop is running

    async def test_rejects_a_foreign_loop(self, *, app):
        # A Mock, not a real loop: an unclosed loop would trip the
        # error::ResourceWarning filter when it is garbage collected.
        app.loop = Mock(name="other_loop")

        with pytest.raises(LoopMismatch) as excinfo:
            bind_to_running_loop(app)
        # The message must name what people actually did wrong.
        assert "app.loop" in str(excinfo.value)

    async def test_does_not_pin_while_checking(self, *, app):
        """Reading ``app.loop`` would itself bind -- it must read ``_loop``."""
        running = asyncio.get_running_loop()
        bind_to_running_loop(app)
        assert app._loop is running


class Test_faust_app_running:
    async def test_starts_and_stops(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(True))
        app.stop = Mock(side_effect=lambda: _completed_future())

        async with faust_app_running(app, discover=False) as running_app:
            assert running_app is app
            assert app._loop is asyncio.get_running_loop()
            app.stop.assert_not_called()

        app.maybe_start.assert_called_once_with()
        app.stop.assert_called_once_with()

    async def test_does_not_stop_an_app_it_did_not_start(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        app.stop = Mock(side_effect=lambda: _completed_future())

        async with faust_app_running(app, discover=False):
            pass

        app.stop.assert_not_called()

    async def test_finalizes_by_default(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        app.finalize = Mock()

        async with faust_app_running(app, discover=False):
            pass

        app.finalize.assert_called_once_with()

    async def test_finalize_can_be_disabled(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        app.finalize = Mock()

        async with faust_app_running(app, finalize=False, discover=False):
            pass

        app.finalize.assert_not_called()

    async def test_discovers_when_asked(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        app.discover = Mock()

        async with faust_app_running(app, discover=True):
            pass

        app.discover.assert_called_once_with()

    async def test_discover_defaults_to_the_app_setting(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        app.discover = Mock()

        async with faust_app_running(app):
            pass

        # This app is not configured with autodiscover.
        app.discover.assert_not_called()

    async def test_propagates_loop_mismatch(self, *, app):
        app.loop = Mock(name="other_loop")

        with pytest.raises(LoopMismatch):
            async with faust_app_running(app, discover=False):
                pass  # pragma: no cover


class Test_faust_lifespan:
    async def test_yields_none_for_starlette(self, *, app):
        """Starlette merges a non-None lifespan value into the ASGI scope."""
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        lifespan = faust_lifespan(app, discover=False)

        async with lifespan(Mock(name="asgi_app")) as value:
            assert value is None

    async def test_starts_the_app(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(True))
        app.stop = Mock(side_effect=lambda: _completed_future())
        lifespan = faust_lifespan(app, discover=False)

        async with lifespan(Mock(name="asgi_app")):
            app.maybe_start.assert_called_once_with()
        app.stop.assert_called_once_with()


class Test_serve_asgi:
    def test_registers_an_extra_service(self, *, app):
        asgi_app = Mock(name="asgi_app")

        cls = serve_asgi(app, asgi_app, host="127.0.0.1", port=9001, workers=2)

        assert issubclass(cls, AsgiService)
        assert cls.asgi_app is asgi_app
        assert cls.host == "127.0.0.1"
        assert cls.port == 9001
        assert cls.uvicorn_options == {"workers": 2}
        assert cls in app._extra_services

    def test_defaults(self, *, app):
        cls = serve_asgi(app, Mock(name="asgi_app"))

        assert cls.port == 8000
        assert cls.uvicorn_options == {}


class Test_AsgiService:
    def test_init_overrides_class_attributes(self):
        asgi_app = Mock(name="asgi_app")
        service = AsgiService(
            asgi_app, host="1.2.3.4", port=99, uvicorn_options={"a": 1}
        )

        assert service.asgi_app is asgi_app
        assert service.host == "1.2.3.4"
        assert service.port == 99
        assert service.uvicorn_options == {"a": 1}

    def test_label(self):
        service = AsgiService(Mock(), host="1.2.3.4", port=99)

        assert "1.2.3.4" in service.label
        assert "99" in service.label

    async def test_on_start_requires_an_app(self):
        service = AsgiService()

        with pytest.raises(ImproperlyConfigured):
            await service.on_start()

    async def test_on_start_serves(self):
        service = AsgiService(Mock(name="asgi_app"))
        server = Mock(name="server")
        server.serve = Mock(return_value=_completed_future())
        service._create_server = Mock(return_value=server)
        # Patch add_future so no task is created and the lingering-task guard
        # in tests/conftest.py stays happy.
        service.add_future = Mock(side_effect=lambda coro: coro)

        await service.on_start()

        service._create_server.assert_called_once_with()
        server.serve.assert_called_once_with()
        assert service._server is server

    async def test_on_stop_asks_the_server_to_exit(self):
        service = AsgiService(Mock(name="asgi_app"))
        server = Mock(name="server")
        server.serve = Mock(return_value=_completed_future())
        service._create_server = Mock(return_value=server)
        service.add_future = Mock(side_effect=lambda coro: coro)

        await service.on_start()
        await service.on_stop()

        assert server.should_exit is True
        assert service._server is None
        assert service._serve_fut is None

    async def test_on_stop_is_safe_when_never_started(self):
        service = AsgiService(Mock(name="asgi_app"))

        await service.on_stop()  # must not raise

    async def test_on_stop_warns_but_does_not_hang(self):
        service = AsgiService(Mock(name="asgi_app"))
        server = Mock(name="server")
        never = asyncio.get_running_loop().create_future()
        server.serve = Mock(return_value=never)
        service._create_server = Mock(return_value=server)
        service.add_future = Mock(side_effect=lambda coro: coro)
        service.server_shutdown_timeout = 0.01

        await service.on_start()
        await service.on_stop()

        assert server.should_exit is True
        never.cancel()


class Test_disable_signal_handling:
    def test_neutralizes_capture_signals(self):
        server = Mock(name="server", spec=["capture_signals"])

        _disable_signal_handling(server)

        with server.capture_signals():
            pass  # must be a no-op context manager

    def test_neutralizes_legacy_install_signal_handlers(self):
        server = Mock(name="server", spec=["install_signal_handlers"])

        _disable_signal_handling(server)

        assert server.install_signal_handlers() is None

    def test_create_server_applies_it_to_a_real_uvicorn_server(self):
        """The override must actually reach the server ``on_start`` builds.

        Testing ``_disable_signal_handling`` in isolation would not catch
        ``_create_server`` forgetting to call it.
        """
        import signal

        uvicorn = pytest.importorskip("uvicorn")

        service = AsgiService(Mock(name="asgi_app"), port=0)
        server = service._create_server()

        assert isinstance(server, uvicorn.Server)
        before = signal.getsignal(signal.SIGINT)
        with server.capture_signals():
            # A server left to itself would have replaced the SIGINT handler
            # here; ours must leave mode.Worker's in place.
            assert signal.getsignal(signal.SIGINT) is before
        assert signal.getsignal(signal.SIGINT) is before


class Test_maybe_instrument_opentelemetry:
    def test_opt_out(self):
        assert maybe_instrument_opentelemetry(Mock(name="asgi_app"), False) is False

    def test_delegates_to_the_contrib_module(self):
        asgi_app = Mock(name="asgi_app")
        with patch(
            "faust.contrib.opentelemetry.instrument_asgi_app", return_value=True
        ) as instrument:
            assert maybe_instrument_opentelemetry(asgi_app, None) is True

        instrument.assert_called_once_with(asgi_app, force=False)

    def test_force(self):
        asgi_app = Mock(name="asgi_app")
        with patch(
            "faust.contrib.opentelemetry.instrument_asgi_app", return_value=True
        ) as instrument:
            maybe_instrument_opentelemetry(asgi_app, True)

        instrument.assert_called_once_with(asgi_app, force=True)

    async def test_lifespan_instruments_the_asgi_app(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        asgi_app = Mock(name="asgi_app")
        lifespan = faust_lifespan(app, discover=False)

        with patch(
            "faust.contrib.fastapi.maybe_instrument_opentelemetry"
        ) as instrument:
            async with lifespan(asgi_app):
                pass

        instrument.assert_called_once_with(asgi_app, None)

    async def test_lifespan_opt_out_is_propagated(self, *, app):
        app.maybe_start = Mock(side_effect=lambda: _started(False))
        asgi_app = Mock(name="asgi_app")
        lifespan = faust_lifespan(app, discover=False, opentelemetry=False)

        with patch(
            "faust.contrib.fastapi.maybe_instrument_opentelemetry"
        ) as instrument:
            async with lifespan(asgi_app):
                pass

        instrument.assert_called_once_with(asgi_app, False)
