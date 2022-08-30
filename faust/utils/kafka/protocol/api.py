"""Kafka protocol extensions."""
# pragma: no cover
import abc
from typing import Type

from kafka.protocol.struct import Struct
from kafka.protocol.types import Schema


class Response(Struct, metaclass=abc.ABCMeta):  # type: ignore
    """API Response."""

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request/response."""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request/response version."""

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """Return instance of Schema() representing the response structure."""


class Request(Struct, metaclass=abc.ABCMeta):  # type: ignore
    """API Request."""

    @property
    @abc.abstractmethod
    def API_KEY(self) -> int:
        """Integer identifier for api request."""

    @property
    @abc.abstractmethod
    def API_VERSION(self) -> int:
        """Integer of api request version."""

    @property
    @abc.abstractmethod
    def SCHEMA(self) -> Schema:
        """Return instance of Schema() representing the request structure."""

    @property
    @abc.abstractmethod
    def RESPONSE_TYPE(self) -> Type[Response]:
        """Response class associated with the api request."""

    def expect_response(self) -> bool:
        """Return True if request type does not always return response."""
        return True
