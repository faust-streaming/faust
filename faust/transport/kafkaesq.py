"""Kafka client configuration, convertible between client spellings.

Faust's two transports talk to Kafka through clients that spell their
configuration differently: the ``kafka://``/``aiokafka://`` transport uses
:pypi:`aiokafka` constructor kwargs (``session_timeout_ms``), the
``confluent://`` transport uses librdkafka's dotted keys
(``session.timeout.ms``), and app settings are a third spelling again
(:setting:`broker_session_timeout`, in seconds).

:class:`ClientConfig` holds one configuration and hands it out in whichever
of those spellings is wanted, so a config written for one client can drive
the other -- or a Faust app::

    from faust.transport.kafkaesq import ClientConfig

    config = ClientConfig.from_confluent({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'billing',
        'session.timeout.ms': 45000,
    })

    consumer = AIOKafkaConsumer('topic', **config.as_aiokafka())

    settings = config.as_app_settings()
    app = faust.App(settings.pop('id'), **settings)

The conversion tables are :pypi:`kafkaesq`'s, installed with the ``kafkaesq``
bundle:

.. sourcecode:: console

    $ pip install "faust-streaming[kafkaesq]"

It is an optional dependency and nothing in Faust needs it: the transports
configure themselves from app settings as they always have. Without it
:data:`HAS_KAFKAESQ` is :const:`False` and :class:`ClientConfig` raises
:exc:`~faust.exceptions.ImproperlyConfigured`.

Note:
    Authentication is out of scope here. Faust configures it with a
    :setting:`broker_credentials` object built at runtime, which no config
    file can describe, so ``security.protocol``, ``sasl.*`` and ``ssl.*``
    keys have no app setting and are reported as unmapped by
    :meth:`~ClientConfig.as_app_settings`.
"""

import typing
from collections.abc import Mapping as _AbcMapping
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union

from mode import get_logger

from faust.exceptions import ImproperlyConfigured

try:
    import kafkaesq
except ImportError:  # pragma: no cover
    kafkaesq = None  # type: ignore[assignment]

if typing.TYPE_CHECKING:
    from faust.types.settings import Settings as _Settings
else:

    class _Settings: ...  # noqa


__all__ = [
    "HAS_KAFKAESQ",
    "CONFLUENT_LIMITS",
    "CONSUMER_SETTINGS",
    "PRODUCER_SETTINGS",
    "ClientConfig",
    "require_kafkaesq",
]

logger = get_logger(__name__)

#: Set when :pypi:`kafkaesq` is installed (the ``faust[kafkaesq]`` bundle).
HAS_KAFKAESQ: bool = kafkaesq is not None

#: App settings that describe a consumer, for :meth:`ClientConfig.from_app`.
#:
#: :setting:`broker` is absent: which brokers to connect to is the transport
#: URL's job, and :meth:`~ClientConfig.from_app` takes it from there.
#: :setting:`broker_request_timeout` is absent too, because librdkafka's
#: ``request.timeout.ms`` is a producer property that a consumer instance
#: warns about and ignores.
CONSUMER_SETTINGS: Tuple[str, ...] = (
    "id",
    "broker_client_id",
    "broker_commit_interval",
    "broker_session_timeout",
    "broker_heartbeat_interval",
    "broker_max_poll_interval",
    "broker_check_crcs",
    "consumer_auto_offset_reset",
    "consumer_group_instance_id",
    "consumer_max_fetch_size",
    "consumer_metadata_max_age_ms",
    "consumer_connections_max_idle_ms",
)

#: App settings that describe a producer, for :meth:`ClientConfig.from_app`.
PRODUCER_SETTINGS: Tuple[str, ...] = (
    "broker_client_id",
    "producer_request_timeout",
    "producer_acks",
    "producer_compression_type",
    "producer_linger",
    "producer_max_request_size",
    "producer_metadata_max_age_ms",
    "producer_connections_max_idle_ms",
)

#: Ranges librdkafka enforces, for :meth:`ClientConfig.as_confluent`. A value
#: outside them fails client construction outright, and Faust's settings are
#: not bounded by them: :setting:`producer_request_timeout` defaults to 20
#: minutes, where ``request.timeout.ms`` stops at 15.
CONFLUENT_LIMITS: Mapping[str, Tuple[int, int]] = {
    "request.timeout.ms": (1, 900000),
    "session.timeout.ms": (1, 3600000),
    "heartbeat.interval.ms": (1, 3600000),
    "max.poll.interval.ms": (1, 86400000),
    "metadata.max.age.ms": (1, 86400000),
    "auto.commit.interval.ms": (0, 86400000),
    "connections.max.idle.ms": (0, 2147483647),
    "max.partition.fetch.bytes": (1, 1000000000),
    "message.max.bytes": (1000, 1000000000),
    "linger.ms": (0, 900000),
}


def require_kafkaesq() -> Any:
    """Return the :pypi:`kafkaesq` module, raising if it is not installed.

    Raises:
        ~faust.exceptions.ImproperlyConfigured: when the library is missing.
    """
    if kafkaesq is None:
        raise ImproperlyConfigured(
            "Converting Kafka client configs requires the kafkaesq library: "
            'pip install "faust-streaming[kafkaesq]"'
        )
    return kafkaesq


class ClientConfig:
    """One Kafka client configuration, in every spelling Faust deals with.

    Held internally as a librdkafka config -- the spelling :pypi:`kafkaesq`
    converts through -- and read back with :meth:`as_confluent`,
    :meth:`as_aiokafka` or :meth:`as_app_settings`.

    Build one with :meth:`from_confluent`, :meth:`from_aiokafka` or
    :meth:`from_app` rather than calling the constructor, unless you already
    have a librdkafka config in hand.

    Raises:
        ~faust.exceptions.ImproperlyConfigured: when :pypi:`kafkaesq` is not
            installed.
    """

    #: The configuration, in librdkafka's dotted-key spelling.
    config: Dict[str, Any]

    def __init__(self, config: Optional[Mapping[str, Any]] = None) -> None:
        require_kafkaesq()
        self.config = dict(config or {})

    def __repr__(self) -> str:
        return f"<{type(self).__name__}: {sorted(self.config)}>"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ClientConfig):
            return self.config == other.config
        return NotImplemented

    @classmethod
    def from_confluent(cls, config: Mapping[str, Any]) -> "ClientConfig":
        """Build from a :pypi:`confluent_kafka` (librdkafka) config."""
        return cls(config)

    @classmethod
    def from_aiokafka(
        cls,
        config: Optional[Mapping[str, Any]] = None,
        *,
        on_unmapped: str = "warn",
        **kwargs: Any,
    ) -> "ClientConfig":
        """Build from :pypi:`aiokafka` constructor kwargs.

        Takes a mapping of kwargs, keyword arguments, or both.

        Arguments:
            on_unmapped: What to do with kwargs that have no librdkafka
                equivalent -- ``"warn"`` (the default; the kwarg is dropped),
                ``"raise"`` or ``"ignore"``. An ``ssl_context`` is one of
                them: it is a Python object that cannot be turned back into
                the certificate paths librdkafka wants.
        """
        lib = require_kafkaesq()
        return cls(lib.aiokafka_to_confluent(config, on_unmapped=on_unmapped, **kwargs))

    @classmethod
    def from_app_settings(
        cls,
        conf: Union[_Settings, Mapping[str, Any]],
        keys: Iterable[str] = CONSUMER_SETTINGS,
        *,
        on_unmapped: str = "ignore",
    ) -> "ClientConfig":
        """Build from app settings.

        Arguments:
            conf: App settings -- ``app.conf``, or a plain mapping of setting
                names to values.
            keys: Which settings to take, :data:`CONSUMER_SETTINGS` by
                default; :data:`PRODUCER_SETTINGS` describes a producer.
                Settings that are unset are skipped, leaving the client its
                own default for them.
            on_unmapped: What to do with settings that have no librdkafka
                equivalent, ``"ignore"`` by default. Most app settings
                describe Faust rather than its client, so ``"warn"`` is
                noisy unless `keys` is a list you chose yourself.

        Note:
            :setting:`broker` is not among :data:`CONSUMER_SETTINGS`: pass
            ``bootstrap.servers`` yourself, or use :meth:`from_app`, which
            takes it from the app's transport.
        """
        lib = require_kafkaesq()
        return cls(
            lib.faust_to_confluent(_extract(conf, tuple(keys)), on_unmapped=on_unmapped)
        )

    @classmethod
    def from_app(
        cls,
        app: Any,
        keys: Iterable[str] = CONSUMER_SETTINGS,
        *,
        on_unmapped: str = "ignore",
    ) -> "ClientConfig":
        """Build from a running app's settings, brokers included.

        As :meth:`from_app_settings`, but ``bootstrap.servers`` is taken from
        the app's transport, so the client this configures connects where the
        app does::

            config = ClientConfig.from_app(app, PRODUCER_SETTINGS)
            producer = confluent_kafka.Producer(config.as_confluent())
        """
        config = cls.from_app_settings(app.conf, keys, on_unmapped=on_unmapped)
        config.config["bootstrap.servers"] = _server_list(app.transport)
        return config

    def as_confluent(self, *, clamp: bool = True) -> Dict[str, Any]:
        """Return the config for a :pypi:`confluent_kafka` client.

        Arguments:
            clamp: Bring values into the range librdkafka accepts for their
                key (:data:`CONFLUENT_LIMITS`), logging each one changed.
                Pass :const:`False` to get the values unaltered and have the
                client reject them itself.
        """
        config = dict(self.config)
        return self._clamp(config) if clamp else config

    def as_aiokafka(
        self, *, on_unmapped: str = "warn", **kwargs: Any
    ) -> Dict[str, Any]:
        """Return the config as :pypi:`aiokafka` constructor kwargs.

        librdkafka ``ssl.*`` file options are folded into a single
        ``ssl_context``, which is the form aiokafka takes.

        Arguments:
            on_unmapped: What to do with keys aiokafka has no kwarg for
                (librdkafka internals, callbacks, ...): ``"warn"`` (the
                default), ``"raise"`` or ``"ignore"``.
            kwargs: Passed through to :pypi:`kafkaesq`, which understands
                ``build_ssl_context=False`` for configs whose certificate
                files are not present on this machine.
        """
        lib = require_kafkaesq()
        return lib.confluent_to_aiokafka(self.config, on_unmapped=on_unmapped, **kwargs)

    def as_app_settings(self, *, on_unmapped: str = "warn") -> Dict[str, Any]:
        """Return the config as :class:`faust.App` keyword arguments.

        ``group.id`` becomes the app ``id`` and ``bootstrap.servers`` becomes
        a :setting:`broker` URL list, so the result goes straight into an
        app::

            settings = config.as_app_settings()
            app = faust.App(settings.pop('id'), **settings)

        Arguments:
            on_unmapped: What to do with keys that have no app setting --
                authentication among them, see the module note: ``"warn"``
                (the default), ``"raise"`` or ``"ignore"``.
        """
        lib = require_kafkaesq()
        return lib.confluent_to_faust(self.config, on_unmapped=on_unmapped)

    @staticmethod
    def _clamp(config: Dict[str, Any]) -> Dict[str, Any]:
        for key, (low, high) in CONFLUENT_LIMITS.items():
            value = config.get(key)
            if not isinstance(value, int) or isinstance(value, bool):
                continue
            if not low <= value <= high:
                limit = low if value < low else high
                logger.warning(
                    "confluent-kafka only accepts %s between %s and %s: "
                    "using %s instead of %s",
                    key,
                    low,
                    high,
                    limit,
                    value,
                )
                config[key] = limit
        return config


def _server_list(transport: Any) -> str:
    """Spell a transport's broker URLs as ``bootstrap.servers``.

    The same host list the drivers build for their own clients.
    """
    return ",".join(
        f"{url.host or '127.0.0.1'}:{url.port or transport.default_port}"
        for url in transport.url
    )


def _extract(
    conf: Union[_Settings, Mapping[str, Any]], keys: Tuple[str, ...]
) -> Dict[str, Any]:
    """Read `keys` out of an app settings object (or a plain mapping).

    Settings left unset are skipped: an absent key means "whatever the client
    defaults to", which is what Faust's own transports do with them too.
    """
    if isinstance(conf, _AbcMapping):
        values = {key: conf.get(key) for key in keys if key in conf}
    else:
        values = {key: getattr(conf, key, None) for key in keys}
    return {key: value for key, value in values.items() if value is not None}
