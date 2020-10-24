"""Transport registry."""
from typing import Type

from yarl import URL

from mode.utils.imports import FactoryMapping

from faust.types import TransportT

from .aiokafka import Transport as AIOKafkaTransport
# from .confluent import Transport as ConfluentTransport


__all__ = ['by_name', 'by_url']


DRIVERS = {
    'aiokafka': AIOKafkaTransport,
    # 'confluent': ConfluentTransport,
    'kafka': AIOKafkaTransport,
}

def by_name(driver_name: str):
    return DRIVERS[driver_name]

def by_url(url: URL):
    scheme = url.scheme
    return DRIVERS[scheme]



# TRANSPORTS: FactoryMapping[Type[TransportT]] = FactoryMapping(
#     aiokafka='faust.transport.drivers.aiokafka:Transport',
#     confluent='faust.transport.drivers.confluent:Transport',
#     kafka='faust.transport.drivers.aiokafka:Transport',
# )
# TRANSPORTS.include_setuptools_namespace('faust.transports')

# by_name = TRANSPORTS.by_name
# by_url = TRANSPORTS.by_url
