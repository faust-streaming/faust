import os
from unittest.mock import Mock

import pytest

import faust
from faust.transport.producer import Producer
from faust.utils.tracing import set_current_span
from tests.helpers import AsyncMock


@pytest.fixture()
def app(event_loop, request):
    settings = request.node.get_closest_marker("conf")
    kwargs = settings.kwargs or {} if settings else {}

    os.environ.pop("F_DATADIR", None)
    os.environ.pop("FAUST_DATADIR", None)
    os.environ.pop("F_WORKDIR", None)
    os.environ.pop("FAUST_WORKDIR", None)
    instance = faust.App("testid", **kwargs)
    instance.producer = Mock(
        name="producer",
        autospec=Producer,
        maybe_start=AsyncMock(),
        start=AsyncMock(),
        send=AsyncMock(),
        send_and_wait=AsyncMock(),
    )
    instance.finalize()
    set_current_span(None)
    return instance


@pytest.fixture()
def web(app):
    return app.web


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "conf: Faust app configuration marker",
    )
