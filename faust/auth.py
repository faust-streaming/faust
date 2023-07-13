"""Authentication Credentials."""
import ssl
from typing import Any, Optional, Union

from aiokafka.conn import AbstractTokenProvider

from faust.types.auth import AuthProtocol, CredentialsT, SASLMechanism

__all__ = [
    "Credentials",
    "SASLCredentials",
    "OAuthCredentials",
    "GSSAPICredentials",
    "SSLCredentials",
]


class Credentials(CredentialsT):
    """Base class for authentication credentials."""


class SASLCredentials(Credentials):
    """Describe SASL credentials."""

    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: SASLMechanism = SASLMechanism.PLAIN

    username: Optional[str]
    password: Optional[str]

    ssl_context: Optional[ssl.SSLContext]

    def __init__(
        self,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        ssl_context: ssl.SSLContext = None,
        mechanism: Union[str, SASLMechanism] = None,
    ) -> None:
        self.username = username
        self.password = password
        self.ssl_context = ssl_context

        if ssl_context is not None:
            self.protocol = AuthProtocol.SASL_SSL

        if mechanism is not None:
            self.mechanism = SASLMechanism(mechanism)

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: username={self.username}>"


class OAuthCredentials(Credentials):
    """Describe OAuth Bearer credentials over SASL"""

    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: SASLMechanism = SASLMechanism.OAUTHBEARER

    ssl_context: Optional[ssl.SSLContext]

    def __init__(
        self,
        *,
        oauth_cb: AbstractTokenProvider,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.oauth_cb = oauth_cb
        self.ssl_context = ssl_context

        if ssl_context is not None:
            self.protocol = AuthProtocol.SASL_SSL

    def __repr__(self) -> str:
        return "<{0}: oauth credentials {1} SSL support".format(
            type(self).__name__,
            "with" if self.protocol == AuthProtocol.SASL_SSL else "without",
        )


class GSSAPICredentials(Credentials):
    """Describe GSSAPI credentials over SASL."""

    protocol = AuthProtocol.SASL_PLAINTEXT
    mechanism: SASLMechanism = SASLMechanism.GSSAPI

    ssl_context: Optional[ssl.SSLContext]

    def __init__(
        self,
        *,
        kerberos_service_name: str = "kafka",
        kerberos_domain_name: Optional[str] = None,
        ssl_context: ssl.SSLContext = None,
        mechanism: Union[str, SASLMechanism] = None,
    ) -> None:
        self.kerberos_service_name = kerberos_service_name
        self.kerberos_domain_name = kerberos_domain_name
        self.ssl_context = ssl_context

        if ssl_context is not None:
            self.protocol = AuthProtocol.SASL_SSL

        if mechanism is not None:
            self.mechanism = SASLMechanism(mechanism)

    def __repr__(self) -> str:
        return "<{0}: kerberos service={1!r} domain={2!r}".format(
            type(self).__name__,
            self.kerberos_service_name,
            self.kerberos_domain_name,
        )


class SSLCredentials(Credentials):
    """Describe SSL credentials/settings."""

    protocol = AuthProtocol.SSL
    context: ssl.SSLContext

    def __init__(
        self,
        context: ssl.SSLContext = None,
        *,
        purpose: Any = None,
        cafile: Optional[str] = None,
        capath: Optional[str] = None,
        cadata: Optional[str] = None,
    ) -> None:
        if context is None:
            context = ssl.create_default_context(
                purpose=purpose,
                cafile=cafile,
                capath=capath,
                cadata=cadata,
            )
        self.context = context

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: context={self.context}>"
