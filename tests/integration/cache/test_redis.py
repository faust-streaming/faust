"""Live-server integration tests for the Redis cache backend.

These exercise :mod:`faust.web.cache.backends.redis` against a *real* Redis
server -- the unit tests mock the client, which is exactly why the backend
could ship awaiting a synchronous client without anyone noticing.  Point
``FAUST_TEST_REDIS`` at a reachable server (default ``redis://localhost:6379``)
to run them; they skip when no server is reachable.
"""

import os
import socket

import pytest
from yarl import URL

import faust

REDIS_URL = os.environ.get("FAUST_TEST_REDIS", "redis://localhost:6379")


def _redis_reachable(url: str) -> bool:
    u = URL(url)
    try:
        with socket.create_connection((u.host or "localhost", u.port or 6379), 2):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.asyncio,
    # Live sockets can emit ResourceWarning during teardown; the suite escalates
    # those to errors (pyproject.toml), so relax it here as the broker tests do.
    pytest.mark.filterwarnings("ignore::ResourceWarning"),
    pytest.mark.skipif(
        not _redis_reachable(REDIS_URL),
        reason=f"no reachable Redis at {REDIS_URL} (set FAUST_TEST_REDIS)",
    ),
]


def _make_app() -> faust.App:
    return faust.App(
        "faust-integration-redis-cache",
        cache=REDIS_URL,
        # a unique key prefix per run keeps parallel/repeated runs isolated
        broker="memory://",
    )


async def test_redis_cache_set_get_delete():
    app = _make_app()
    async with app.cache:
        key = "faust-it:roundtrip"
        await app.cache.delete(key)
        assert await app.cache.get(key) is None

        await app.cache.set(key, b"value-1", timeout=60)
        assert await app.cache.get(key) == b"value-1"

        # overwrite
        await app.cache.set(key, b"value-2", timeout=60)
        assert await app.cache.get(key) == b"value-2"

        # delete
        await app.cache.delete(key)
        assert await app.cache.get(key) is None


async def test_redis_cache_missing_key_returns_none():
    app = _make_app()
    async with app.cache:
        assert await app.cache.get("faust-it:definitely-not-set") is None
