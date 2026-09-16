import pytest

from faust import SASLCredentials
from faust.exceptions import ImproperlyConfigured
from faust.transport.drivers.aiokafka import credentials_to_aiokafka_auth


def test_plain_credentials_are_forwarded_to_aiokafka() -> None:
    settings = credentials_to_aiokafka_auth(
        SASLCredentials(
            username="test-user",
            password="test-password",
            mechanism="PLAIN",
        )
    )

    assert settings == {
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "test-user",
        "sasl_plain_password": "test-password",
        "ssl_context": None,
    }


@pytest.mark.parametrize(
    "username,password",
    [(None, None), (None, "test-password"), ("test-user", None)],
)
def test_plain_credentials_require_username_and_password(
    username: str | None, password: str | None
) -> None:
    credentials = SASLCredentials(
        username=username,
        password=password,
        mechanism="PLAIN",
    )

    with pytest.raises(ImproperlyConfigured, match="broker_credentials") as exc:
        credentials_to_aiokafka_auth(credentials)

    assert "test-user" not in str(exc.value)
    assert "test-password" not in str(exc.value)
