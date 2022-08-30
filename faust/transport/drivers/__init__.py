"""Transport registry."""
from yarl import URL

from .aiokafka import Transport as AIOKafkaTransport

__all__ = ["by_name", "by_url"]


DRIVERS = {
    "aiokafka": AIOKafkaTransport,
    "kafka": AIOKafkaTransport,
}


def by_name(driver_name: str):
    return DRIVERS[driver_name]


def by_url(url: URL):
    scheme = url.scheme
    return DRIVERS[scheme]
