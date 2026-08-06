"""Tests for :class:`faust.transport.kafkaesq.ClientConfig`.

The class needs :pypi:`kafkaesq` (the ``faust[kafkaesq]`` bundle) and says so
when it is missing; that path is tested by faking the library away rather
than by skipping, so it is covered wherever the suite runs.
"""

import ssl
from unittest.mock import patch

import pytest

import faust
from faust.exceptions import ImproperlyConfigured
from faust.transport import kafkaesq as mod
from faust.transport.kafkaesq import (
    CONSUMER_SETTINGS,
    PRODUCER_SETTINGS,
    ClientConfig,
    require_kafkaesq,
)

#: Skips the tests that need the real library.
needs_kafkaesq = pytest.mark.skipif(
    not mod.HAS_KAFKAESQ, reason="kafkaesq is not installed"
)

CONFLUENT_CONFIG = {
    "bootstrap.servers": "h1:9092,h2:9092",
    "group.id": "billing",
    "client.id": "worker-1",
    "session.timeout.ms": 45000,
    "auto.offset.reset": "latest",
}


@pytest.fixture()
def no_kafkaesq():
    """Pretend the optional dependency is not installed."""
    with patch.object(mod, "kafkaesq", None):
        yield


class Test_require_kafkaesq:
    @needs_kafkaesq
    def test_returns_the_module(self):
        assert require_kafkaesq() is mod.kafkaesq

    def test_raises_when_missing(self, *, no_kafkaesq):
        with pytest.raises(ImproperlyConfigured, match="faust-streaming\\[kafkaesq\\]"):
            require_kafkaesq()

    def test_client_config_requires_it(self, *, no_kafkaesq):
        with pytest.raises(ImproperlyConfigured):
            ClientConfig(CONFLUENT_CONFIG)


@needs_kafkaesq
class TestClientConfig:
    def test_from_confluent_keeps_the_config(self):
        config = ClientConfig.from_confluent(CONFLUENT_CONFIG)
        assert config.as_confluent() == CONFLUENT_CONFIG
        # ... as a copy: mutating the result cannot corrupt the source.
        config.as_confluent()["group.id"] = "other"
        assert config.config["group.id"] == "billing"

    def test_from_aiokafka(self):
        config = ClientConfig.from_aiokafka(
            bootstrap_servers="localhost:9092",
            group_id="billing",
            enable_auto_commit=False,
        )
        assert config.as_confluent() == {
            "bootstrap.servers": "localhost:9092",
            "group.id": "billing",
            "enable.auto.commit": False,
        }

    def test_from_aiokafka_takes_a_mapping(self):
        assert ClientConfig.from_aiokafka(
            {"bootstrap_servers": ["h1:9092", "h2:9092"]}
        ) == ClientConfig.from_confluent({"bootstrap.servers": "h1:9092,h2:9092"})

    def test_from_aiokafka_reports_unmapped_kwargs(self):
        with pytest.raises(KeyError, match="ssl_context"):
            ClientConfig.from_aiokafka(
                ssl_context=ssl.create_default_context(), on_unmapped="raise"
            )

    def test_as_aiokafka(self):
        kwargs = ClientConfig.from_confluent(CONFLUENT_CONFIG).as_aiokafka()
        assert kwargs == {
            "bootstrap_servers": "h1:9092,h2:9092",
            "group_id": "billing",
            "client_id": "worker-1",
            "session_timeout_ms": 45000,
            "auto_offset_reset": "latest",
        }

    def test_as_aiokafka_builds_an_ssl_context(self):
        kwargs = ClientConfig.from_confluent(
            {"bootstrap.servers": "h:9092", "security.protocol": "SSL"}
        ).as_aiokafka()
        assert isinstance(kwargs["ssl_context"], ssl.SSLContext)

    def test_as_aiokafka_reports_unmapped_keys(self):
        with pytest.raises(KeyError, match="socket.keepalive.enable"):
            ClientConfig.from_confluent(
                {**CONFLUENT_CONFIG, "socket.keepalive.enable": True}
            ).as_aiokafka(on_unmapped="raise")

    def test_as_app_settings(self):
        assert ClientConfig.from_confluent(CONFLUENT_CONFIG).as_app_settings() == {
            "broker": "kafka://h1:9092;kafka://h2:9092",
            "id": "billing",
            "broker_client_id": "worker-1",
            "broker_session_timeout": 45,
            "consumer_auto_offset_reset": "latest",
        }

    def test_as_app_settings_configures_an_app(self):
        settings = ClientConfig.from_confluent(CONFLUENT_CONFIG).as_app_settings()
        app = faust.App(settings.pop("id"), **settings)
        assert app.conf.id == "billing"
        assert [str(url) for url in app.conf.broker] == [
            "kafka://h1:9092",
            "kafka://h2:9092",
        ]
        assert app.conf.broker_session_timeout == 45
        assert app.conf.consumer_auto_offset_reset == "latest"

    def test_as_app_settings_reports_authentication(self):
        # Faust takes a broker_credentials object built at runtime, so
        # security keys have no setting to convert to.
        with pytest.warns(UserWarning, match="sasl.username"):
            settings = ClientConfig.from_confluent(
                {
                    **CONFLUENT_CONFIG,
                    "security.protocol": "sasl_ssl",
                    "sasl.username": "user",
                    "sasl.password": "secret",
                }
            ).as_app_settings()
        assert "broker_credentials" not in settings

    def test_round_trips_through_every_spelling(self):
        config = ClientConfig.from_confluent(CONFLUENT_CONFIG)
        assert ClientConfig.from_aiokafka(config.as_aiokafka()) == config
        settings = config.as_app_settings()
        assert ClientConfig.from_app_settings(
            settings, keys=settings, on_unmapped="raise"
        ) == ClientConfig.from_confluent(
            # 'broker' converts back with the scheme stripped.
            {**CONFLUENT_CONFIG, "bootstrap.servers": "h1:9092,h2:9092"}
        )


@needs_kafkaesq
class TestClientConfigFromApp:
    def test_consumer_settings(self, *, app):
        config = ClientConfig.from_app_settings(
            app.conf, CONSUMER_SETTINGS
        ).as_confluent()
        assert config["group.id"] == app.conf.id
        assert config["client.id"] == app.conf.broker_client_id
        assert config["session.timeout.ms"] == int(
            app.conf.broker_session_timeout * 1000.0
        )
        assert config["heartbeat.interval.ms"] == int(
            app.conf.broker_heartbeat_interval * 1000.0
        )
        assert config["max.poll.interval.ms"] == int(
            app.conf.broker_max_poll_interval * 1000.0
        )
        assert config["auto.offset.reset"] == app.conf.consumer_auto_offset_reset
        assert config["max.partition.fetch.bytes"] == app.conf.consumer_max_fetch_size
        assert config["check.crcs"] == app.conf.broker_check_crcs
        # Which brokers to talk to comes from the transport, not the settings.
        assert "bootstrap.servers" not in config

    def test_producer_settings(self, *, app):
        config = ClientConfig.from_app_settings(
            app.conf, PRODUCER_SETTINGS
        ).as_confluent()
        assert config["client.id"] == app.conf.broker_client_id
        assert config["acks"] == "all"
        assert config["message.max.bytes"] == app.conf.producer_max_request_size
        assert "group.id" not in config

    @pytest.mark.conf(broker_session_timeout=45, consumer_auto_offset_reset="latest")
    def test_reflects_configured_values(self, *, app):
        config = ClientConfig.from_app_settings(
            app.conf, CONSUMER_SETTINGS
        ).as_confluent()
        assert config["session.timeout.ms"] == 45000
        assert config["auto.offset.reset"] == "latest"

    def test_accepts_a_plain_mapping(self):
        config = ClientConfig.from_app_settings(
            {"id": "billing", "broker_session_timeout": 45}, CONSUMER_SETTINGS
        )
        assert config.as_confluent() == {
            "group.id": "billing",
            "session.timeout.ms": 45000,
        }

    def test_unset_settings_are_skipped(self, *, app):
        assert app.conf.consumer_group_instance_id is None
        config = ClientConfig.from_app_settings(app.conf, CONSUMER_SETTINGS)
        assert "group.instance.id" not in config.as_confluent()

    @pytest.mark.conf(consumer_group_instance_id="worker-1")
    def test_group_instance_id(self, *, app):
        config = ClientConfig.from_app_settings(app.conf, CONSUMER_SETTINGS)
        assert config.as_confluent()["group.instance.id"] == "worker-1"

    def test_unmapped_settings_can_be_reported(self, *, app):
        with pytest.raises(KeyError, match="table_standby_replicas"):
            ClientConfig.from_app_settings(
                app.conf, ["id", "table_standby_replicas"], on_unmapped="raise"
            )

    def test_from_app_takes_the_brokers_from_the_transport(self, *, app):
        config = ClientConfig.from_app(app, CONSUMER_SETTINGS).as_confluent()
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["group.id"] == app.conf.id

    @pytest.mark.conf(broker="kafka://h1:9092;kafka://h2")
    def test_from_app_with_several_brokers(self, *, app):
        config = ClientConfig.from_app(app).as_confluent()
        assert config["bootstrap.servers"] == "h1:9092,h2:9092"


@needs_kafkaesq
class TestClientConfigLimits:
    def test_producer_timeout_is_clamped(self, *, app):
        # producer_request_timeout defaults to 20 minutes; librdkafka refuses
        # to build a client above 15, so this would be fatal unclamped.
        assert app.conf.producer_request_timeout * 1000.0 > 900000
        config = ClientConfig.from_app_settings(app.conf, PRODUCER_SETTINGS)
        assert config.as_confluent()["request.timeout.ms"] == 900000

    def test_clamping_is_logged(self):
        config = ClientConfig.from_confluent({"request.timeout.ms": 1200000})
        with patch.object(mod, "logger") as logger:
            assert config.as_confluent() == {"request.timeout.ms": 900000}
        logger.warning.assert_called_once()

    def test_value_below_minimum(self):
        config = ClientConfig.from_confluent({"message.max.bytes": 10})
        assert config.as_confluent()["message.max.bytes"] == 1000

    def test_clamping_can_be_turned_off(self):
        config = ClientConfig.from_confluent({"request.timeout.ms": 1200000})
        assert config.as_confluent(clamp=False) == {"request.timeout.ms": 1200000}

    def test_values_in_range_are_untouched(self):
        source = {"request.timeout.ms": 30000, "linger.ms": 0, "check.crcs": True}
        assert ClientConfig.from_confluent(source).as_confluent() == source

    def test_keys_without_a_limit_are_untouched(self):
        source = {"batch.num.messages": 10**12}
        assert ClientConfig.from_confluent(source).as_confluent() == source

    def test_non_integer_values_are_untouched(self):
        source = {"request.timeout.ms": "1200000", "check.crcs": True}
        assert ClientConfig.from_confluent(source).as_confluent() == source
