"""Fixtures for live-broker integration tests.

These tests need a real Kafka broker.  Point them at one with the
``FAUST_TEST_BROKER`` environment variable (default
``kafka://localhost:9092``).  When no broker is reachable every test in this
package is skipped, so the suite stays green on developer machines and in the
normal (broker-less) CI jobs -- the dedicated integration job in CI starts a
Kafka service and runs them for real.
"""

import os
import socket
from uuid import uuid4

import pytest

DEFAULT_BROKER = "kafka://localhost:9092"


def _broker_from_env() -> str:
    return os.environ.get("FAUST_TEST_BROKER", DEFAULT_BROKER)


def _bootstrap_from_url(url: str) -> str:
    # kafka://host:port  ->  host:port  (aiokafka bootstrap_servers form)
    hostport = url.split("://", 1)[-1]
    hostport = hostport.split("/", 1)[0]
    return hostport or "localhost:9092"


def _broker_reachable(url: str) -> bool:
    host, _, port = _bootstrap_from_url(url).partition(":")
    try:
        with socket.create_connection((host or "localhost", int(port or 9092)), 2):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def broker_url() -> str:
    """Return the broker URL, skipping the test if it is not reachable."""
    url = _broker_from_env()
    if not _broker_reachable(url):
        pytest.skip(
            f"No Kafka broker reachable at {url} - "
            "set FAUST_TEST_BROKER to run live-broker integration tests"
        )
    return url


@pytest.fixture(scope="session")
def kafka_bootstrap(broker_url: str) -> str:
    """Return ``host:port`` for aiokafka's ``bootstrap_servers``."""
    return _bootstrap_from_url(broker_url)


@pytest.fixture
def unique_topic() -> str:
    """A fresh topic name so tests don't read each other's messages."""
    return f"faust-it-{uuid4().hex}"


@pytest.fixture
def unique_group() -> str:
    """A fresh consumer-group / app id per test."""
    return f"faust-it-{uuid4().hex}"
