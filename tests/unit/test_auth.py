import asyncio
import ssl
from unittest.mock import Mock, patch

import pytest
from aiokafka.conn import AbstractTokenProvider

from faust.auth import (
    GSSAPICredentials,
    OAuthCredentials,
    SASLCredentials,
    SSLCredentials,
)
from faust.types.auth import AuthProtocol, SASLMechanism


class Test_SASLCredentials:
    @pytest.mark.parametrize(
        "reason,credentials,expected_fields",
        [
            pytest.param(
                "retains arguments",
                SASLCredentials(username="george", password="pw1"),
                {
                    "username": "george",
                    "password": "pw1",
                    "mechanism": SASLMechanism.PLAIN,
                    "protocol": AuthProtocol.SASL_PLAINTEXT,
                },
            ),
            pytest.param(
                "mechanism from str",
                SASLCredentials(username="george", password="pw1", mechanism="GSSAPI"),
                {"mechanism": SASLMechanism.GSSAPI},
            ),
            pytest.param(
                "mechanism from enum",
                SASLCredentials(
                    username="george",
                    password="pw1",
                    mechanism=SASLMechanism.GSSAPI,
                ),
                {"mechanism": SASLMechanism.GSSAPI},
            ),
            pytest.param(
                "ssl context gives SASL_SSL",
                SASLCredentials(
                    username="george",
                    password="pw1",
                    ssl_context={"xxx": "yyy"},
                ),
                {
                    "username": "george",
                    "password": "pw1",
                    "ssl_context": {"xxx": "yyy"},
                    "protocol": AuthProtocol.SASL_SSL,
                },
            ),
            pytest.param(
                "SCRAM-SHA-512 supported",
                SASLCredentials(
                    username="george",
                    password="pw1",
                    mechanism=SASLMechanism.SCRAM_SHA_512,
                ),
                {"mechanism": SASLMechanism.SCRAM_SHA_512},
            ),
        ],
    )
    def test_constructor(self, credentials, expected_fields, reason):
        assert repr(credentials)
        for field, value in expected_fields.items():
            assert getattr(credentials, field) == value, reason


class Test_GSSAPICredentials:
    @pytest.mark.parametrize(
        "reason,credentials,expected_fields",
        [
            pytest.param(
                "retains arguments",
                GSSAPICredentials(
                    kerberos_service_name="george",
                    kerberos_domain_name="domain",
                ),
                {
                    "kerberos_service_name": "george",
                    "kerberos_domain_name": "domain",
                    "mechanism": SASLMechanism.GSSAPI,
                    "protocol": AuthProtocol.SASL_PLAINTEXT,
                },
            ),
            pytest.param(
                "mechanism given as str",
                GSSAPICredentials(
                    kerberos_service_name="george",
                    kerberos_domain_name="domain",
                    mechanism="PLAIN",
                ),
                {"mechanism": SASLMechanism.PLAIN},
            ),
            pytest.param(
                "mechanism given as enum",
                GSSAPICredentials(
                    kerberos_service_name="george",
                    kerberos_domain_name="domain",
                    mechanism=SASLMechanism.PLAIN,
                ),
                {"mechanism": SASLMechanism.PLAIN},
            ),
            pytest.param(
                "ssl context gives SASL_SSL",
                GSSAPICredentials(
                    kerberos_service_name="george",
                    kerberos_domain_name="domain",
                    ssl_context={"xxx": "yyy"},
                ),
                {
                    "kerberos_service_name": "george",
                    "kerberos_domain_name": "domain",
                    "ssl_context": {"xxx": "yyy"},
                    "protocol": AuthProtocol.SASL_SSL,
                },
            ),
        ],
    )
    def test_constructor(self, credentials, expected_fields, reason):
        assert repr(credentials)
        for field, value in expected_fields.items():
            assert getattr(credentials, field) == value, reason


class Test_SSLCredentials:
    def test_constructor(self):
        with patch("faust.auth.ssl.create_default_context") as cdc:
            c = SSLCredentials(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile="/foo/bar/ca.file",
                capath="/foo/bar/",
                cadata="moo",
            )
            assert c.context is cdc.return_value
            assert repr(c)
            cdc.assert_called_once_with(
                purpose=ssl.Purpose.SERVER_AUTH,
                cafile="/foo/bar/ca.file",
                capath="/foo/bar/",
                cadata="moo",
            )

    def test_having_context(self):
        context = Mock(name="context")
        c = SSLCredentials(context)
        assert c.context is context


class Test_OAuthCredentials:
    class TokenProvider(AbstractTokenProvider):
        async def token(self):
            return await asyncio.get_running_loop().run_in_executor(None, self._token)

        def _token(self):
            return "token"

    @pytest.mark.parametrize(
        "reason,credentials,expected_fields",
        [
            pytest.param(
                "retains arguments",
                OAuthCredentials(oauth_cb=TokenProvider()),
                {
                    "mechanism": SASLMechanism.OAUTHBEARER,
                    "protocol": AuthProtocol.SASL_PLAINTEXT,
                },
            ),
            pytest.param(
                "retains arguments",
                OAuthCredentials(
                    oauth_cb=TokenProvider(),
                    ssl_context={"xxx": "yyy"},
                ),
                {
                    "mechanism": SASLMechanism.OAUTHBEARER,
                    "protocol": AuthProtocol.SASL_SSL,
                },
            ),
        ],
    )
    def test_constructor(self, credentials, expected_fields, reason):
        assert repr(credentials)
        for field, value in expected_fields.items():
            assert getattr(credentials, field) == value, reason
