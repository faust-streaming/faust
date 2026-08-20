"""Fixtures for live-server cache integration tests."""

import asyncio

import pytest


@pytest.fixture(autouse=True)
async def _shutdown_default_executor():
    """Reap the loop's default executor thread after each test.

    redis-py's asyncio client resolves the server address through the event
    loop's default :class:`~concurrent.futures.ThreadPoolExecutor`, which
    otherwise lingers as an ``asyncio_N`` thread and trips the strict
    ``threads_not_lingering`` guard in ``tests/conftest.py``.
    """
    yield
    await asyncio.get_event_loop().shutdown_default_executor()
