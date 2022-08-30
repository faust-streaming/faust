import abc
import asyncio
import typing
from datetime import tzinfo
from typing import (
    Any,
    AsyncIterable,
    Awaitable,
    Callable,
    ClassVar,
    ContextManager,
    Iterable,
    Mapping,
    MutableSequence,
    Optional,
    Pattern,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    no_type_check,
)

import opentracing
from mode import Seconds, ServiceT, Signal, SupervisorStrategyT, SyncSignal
from mode.utils.futures import stampede
from mode.utils.objects import cached_property
from mode.utils.queues import FlowControlEvent, ThrowableQueue
from mode.utils.types.trees import NodeT
from mode.utils.typing import NoReturn

from .agents import AgentFun, AgentManagerT, AgentT, SinkT
from .assignor import PartitionAssignorT
from .codecs import CodecArg
from .core import HeadersArg, K, V
from .fixups import FixupT
from .router import RouterT
from .sensors import SensorDelegateT
from .serializers import RegistryT
from .streams import StreamT
from .tables import CollectionT, TableManagerT, TableT
from .topics import ChannelT, TopicT
from .transports import ConductorT, ConsumerT, ProducerT, TransportT
from .tuples import TP, Message, MessageSentCallback, RecordMetadata
from .web import (
    CacheBackendT,
    HttpClientT,
    PageArg,
    ResourceOptions,
    View,
    ViewDecorator,
    Web,
)
from .windows import WindowT

if typing.TYPE_CHECKING:
    from faust.cli.base import AppCommand as _AppCommand
    from faust.livecheck.app import LiveCheck as _LiveCheck
    from faust.sensors.monitor import Monitor as _Monitor
    from faust.worker import Worker as _Worker

    from .events import EventT as _EventT
    from .models import ModelArg as _ModelArg
    from .serializers import SchemaT as _SchemaT
    from .settings import Settings as _Settings
else:

    class _AppCommand:
        ...  # noqa

    class _SchemaT:
        ...  # noqa

    class _LiveCheck:
        ...  # noqa

    class _Monitor:
        ...  # noqa

    class _Worker:
        ...  # noqa

    class _EventT:
        ...  # noqa

    class _ModelArg:
        ...  # noqa

    class _Settings:
        ...  # noqa


__all__ = [
    "TaskArg",
    "AppT",
]

TaskArg = Union[Callable[["AppT"], Awaitable], Callable[[], Awaitable]]
_T = TypeVar("_T")


class TracerT(abc.ABC):
    @property
    @abc.abstractmethod
    def default_tracer(self) -> opentracing.Tracer:
        ...

    @abc.abstractmethod
    def trace(
        self, name: str, sample_rate: Optional[float] = None, **extra_context: Any
    ) -> ContextManager:
        ...

    @abc.abstractmethod
    def get_tracer(self, service_name: str) -> opentracing.Tracer:
        ...


class BootStrategyT:
    app: "AppT"

    enable_kafka: bool = True
    # We want these to take default from `enable_kafka`
    # attribute, but still want to allow subclasses to define
    # them like this:
    #   class MyBoot(BootStrategy):
    #       enable_kafka_consumer = False
    enable_kafka_consumer: Optional[bool] = None
    enable_kafka_producer: Optional[bool] = None

    enable_web: Optional[bool] = None
    enable_sensors: bool = True

    @abc.abstractmethod
    def __init__(
        self,
        app: "AppT",
        *,
        enable_web: Optional[bool] = None,
        enable_kafka: bool = True,
        enable_kafka_producer: Optional[bool] = None,
        enable_kafka_consumer: Optional[bool] = None,
        enable_sensors: bool = True,
    ) -> None:
        ...

    @abc.abstractmethod
    def server(self) -> Iterable[ServiceT]:
        ...

    @abc.abstractmethod
    def client_only(self) -> Iterable[ServiceT]:
        ...

    @abc.abstractmethod
    def producer_only(self) -> Iterable[ServiceT]:
        ...


class AppT(ServiceT):
    """Abstract type for the Faust application.

    See Also:
        :class:`faust.App`.
    """

    Settings: ClassVar[Type[_Settings]]

    BootStrategy: ClassVar[Type[BootStrategyT]]
    boot_strategy: BootStrategyT

    #: Set to true when the app is finalized (can read configuration).
    finalized: bool = False

    #: Set to true when the app has read configuration.
    configured: bool = False

    #: Set to true if the worker is currently rebalancing.
    rebalancing: bool = False
    rebalancing_count: int = 0
    #: Set to true when the worker is in recovery
    in_recovery: bool = False

    consumer_generation_id: int = 0

    #: Set to true if the assignment is empty
    # This flag is set by App._on_partitions_assigned
    unassigned: bool = False

    #: Set to true when app is executing within a worker instance.
    # This flag is set in faust/worker.py
    in_worker: bool = False

    on_configured: SyncSignal[_Settings] = SyncSignal()
    on_before_configured: SyncSignal = SyncSignal()
    on_after_configured: SyncSignal = SyncSignal()
    on_partitions_assigned: Signal[Set[TP]] = Signal()
    on_partitions_revoked: Signal[Set[TP]] = Signal()
    on_rebalance_complete: Signal = Signal()
    on_before_shutdown: Signal = Signal()
    on_worker_init: SyncSignal = SyncSignal()
    on_produce_message: SyncSignal = SyncSignal()

    client_only: bool
    producer_only: bool

    agents: AgentManagerT
    sensors: SensorDelegateT

    fixups: MutableSequence[FixupT]

    tracer: Optional[TracerT] = None

    #: Original id argument + kwargs passed to App.__init__
    _default_options: Tuple[str, Mapping[str, Any]]

    @abc.abstractmethod
    def __init__(
        self, id: str, *, monitor: _Monitor, config_source: Any = None, **options: Any
    ) -> None:
        self.on_startup_finished: Optional[Callable] = None

    @abc.abstractmethod
    def config_from_object(
        self, obj: Any, *, silent: bool = False, force: bool = False
    ) -> None:
        ...

    @abc.abstractmethod
    def finalize(self) -> None:
        ...

    @abc.abstractmethod
    def main(self) -> NoReturn:
        ...

    @abc.abstractmethod
    def worker_init(self) -> None:
        ...

    @abc.abstractmethod
    def worker_init_post_autodiscover(self) -> None:
        ...

    @abc.abstractmethod
    def discover(
        self,
        *extra_modules: str,
        categories: Iterable[str] = ("a", "b", "c"),
        ignore: Iterable[Any] = ("foo", "bar"),
    ) -> None:
        ...

    @abc.abstractmethod
    def topic(
        self,
        *topics: str,
        pattern: Union[str, Pattern] = None,
        schema: Optional[_SchemaT] = None,
        key_type: _ModelArg = None,
        value_type: _ModelArg = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        partitions: Optional[int] = None,
        retention: Optional[Seconds] = None,
        compacting: Optional[bool] = None,
        deleting: Optional[bool] = None,
        replicas: Optional[int] = None,
        acks: bool = True,
        internal: bool = False,
        config: Optional[Mapping[str, Any]] = None,
        maxsize: Optional[int] = None,
        allow_empty: bool = False,
        has_prefix: bool = False,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> TopicT:
        ...

    @abc.abstractmethod
    def channel(
        self,
        *,
        schema: Optional[_SchemaT] = None,
        key_type: _ModelArg = None,
        value_type: _ModelArg = None,
        maxsize: Optional[int] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> ChannelT:
        ...

    @abc.abstractmethod
    def agent(
        self,
        channel: Union[str, ChannelT[_T]] = None,
        *,
        name: Optional[str] = None,
        concurrency: int = 1,
        supervisor_strategy: Type[SupervisorStrategyT] = None,
        sink: Iterable[SinkT] = None,
        isolated_partitions: bool = False,
        use_reply_headers: bool = True,
        **kwargs: Any,
    ) -> Callable[[AgentFun[_T]], AgentT[_T]]:
        ...

    @abc.abstractmethod
    @no_type_check
    def task(
        self, fun: TaskArg, *, on_leader: bool = False, traced: bool = True
    ) -> Callable:
        ...

    @abc.abstractmethod
    def timer(
        self,
        interval: Seconds,
        on_leader: bool = False,
        traced: bool = True,
        name: Optional[str] = None,
        max_drift_correction: float = 0.1,
    ) -> Callable:
        ...

    @abc.abstractmethod
    def crontab(
        self,
        cron_format: str,
        *,
        timezone: tzinfo = None,
        on_leader: bool = False,
        traced: bool = True,
    ) -> Callable:
        ...

    @abc.abstractmethod
    def service(self, cls: Type[ServiceT]) -> Type[ServiceT]:
        ...

    @abc.abstractmethod
    def stream(
        self, channel: AsyncIterable, beacon: Optional[NodeT] = None, **kwargs: Any
    ) -> StreamT:
        ...

    @abc.abstractmethod
    def Table(
        self,
        name: str,
        *,
        default: Callable[[], Any] = None,
        window: Optional[WindowT] = None,
        partitions: Optional[int] = None,
        help: Optional[str] = None,
        **kwargs: Any,
    ) -> TableT:
        ...

    @abc.abstractmethod
    def GlobalTable(
        self,
        name: str,
        *,
        default: Callable[[], Any] = None,
        window: Optional[WindowT] = None,
        partitions: Optional[int] = None,
        help: Optional[str] = None,
        **kwargs: Any,
    ) -> TableT:
        ...

    @abc.abstractmethod
    def SetTable(
        self,
        name: str,
        *,
        window: Optional[WindowT] = None,
        partitions: Optional[int] = None,
        start_manager: bool = False,
        help: Optional[str] = None,
        **kwargs: Any,
    ) -> TableT:
        ...

    @abc.abstractmethod
    def SetGlobalTable(
        self,
        name: str,
        *,
        window: Optional[WindowT] = None,
        partitions: Optional[int] = None,
        start_manager: bool = False,
        help: Optional[str] = None,
        **kwargs: Any,
    ) -> TableT:
        ...

    @abc.abstractmethod
    def page(
        self,
        path: str,
        *,
        base: Type[View] = View,
        cors_options: Mapping[str, ResourceOptions] = None,
        name: Optional[str] = None,
    ) -> Callable[[PageArg], Type[View]]:
        ...

    @abc.abstractmethod
    def table_route(
        self,
        table: CollectionT,
        shard_param: Optional[str] = None,
        *,
        query_param: Optional[str] = None,
        match_info: Optional[str] = None,
        exact_key: Optional[str] = None,
    ) -> ViewDecorator:
        ...

    @abc.abstractmethod
    def command(
        self, *options: Any, base: Type[_AppCommand] = None, **kwargs: Any
    ) -> Callable[[Callable], Type[_AppCommand]]:
        ...

    @abc.abstractmethod
    def create_event(
        self, key: K, value: V, headers: HeadersArg, message: Message
    ) -> _EventT:
        ...

    @abc.abstractmethod
    async def start_client(self) -> None:
        ...

    @abc.abstractmethod
    async def maybe_start_client(self) -> None:
        ...

    @abc.abstractmethod
    def trace(
        self, name: str, trace_enabled: bool = True, **extra_context: Any
    ) -> ContextManager:
        ...

    @abc.abstractmethod
    async def send(
        self,
        channel: Union[ChannelT, str],
        key: K = None,
        value: V = None,
        partition: Optional[int] = None,
        timestamp: Optional[float] = None,
        headers: HeadersArg = None,
        schema: Optional[_SchemaT] = None,
        key_serializer: CodecArg = None,
        value_serializer: CodecArg = None,
        callback: Optional[MessageSentCallback] = None,
    ) -> Awaitable[RecordMetadata]:
        ...

    @abc.abstractmethod
    def LiveCheck(self, **kwargs: Any) -> _LiveCheck:
        ...

    @stampede
    @abc.abstractmethod
    async def maybe_start_producer(self) -> ProducerT:
        ...

    @abc.abstractmethod
    def is_leader(self) -> bool:
        ...

    @abc.abstractmethod
    def FlowControlQueue(
        self,
        maxsize: Optional[int] = None,
        *,
        clear_on_resume: bool = False,
    ) -> ThrowableQueue:
        ...

    @abc.abstractmethod
    def Worker(self, **kwargs: Any) -> _Worker:
        ...

    @abc.abstractmethod
    def on_webserver_init(self, web: Web) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_start(self) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_return(self) -> None:
        ...

    @abc.abstractmethod
    def on_rebalance_end(self) -> None:
        ...

    @property
    def conf(self) -> _Settings:
        ...

    @conf.setter
    def conf(self, settings: _Settings) -> None:
        ...

    @property
    @abc.abstractmethod
    def transport(self) -> TransportT:
        ...

    @transport.setter
    def transport(self, transport: TransportT) -> None:
        ...

    @property
    @abc.abstractmethod
    def producer_transport(self) -> TransportT:
        ...

    @producer_transport.setter
    def producer_transport(self, transport: TransportT) -> None:
        ...

    @property
    @abc.abstractmethod
    def cache(self) -> CacheBackendT:
        ...

    @cache.setter
    def cache(self, cache: CacheBackendT) -> None:
        ...

    @property
    @abc.abstractmethod
    def producer(self) -> ProducerT:
        ...

    @property
    @abc.abstractmethod
    def consumer(self) -> ConsumerT:
        ...

    @cached_property
    @abc.abstractmethod
    def tables(self) -> TableManagerT:
        ...

    @cached_property
    @abc.abstractmethod
    def topics(self) -> ConductorT:
        ...

    @property
    @abc.abstractmethod
    def monitor(self) -> _Monitor:
        ...

    @monitor.setter
    def monitor(self, value: _Monitor) -> None:
        ...

    @cached_property
    @abc.abstractmethod
    def flow_control(self) -> FlowControlEvent:
        return FlowControlEvent(loop=self.loop)

    @property
    @abc.abstractmethod
    def http_client(self) -> HttpClientT:
        ...

    @http_client.setter
    def http_client(self, client: HttpClientT) -> None:
        ...

    @cached_property
    @abc.abstractmethod
    def assignor(self) -> PartitionAssignorT:
        ...

    @cached_property
    @abc.abstractmethod
    def router(self) -> RouterT:
        ...

    @cached_property
    @abc.abstractmethod
    def serializers(self) -> RegistryT:
        ...

    @cached_property
    @abc.abstractmethod
    def web(self) -> Web:
        ...

    @cached_property
    @abc.abstractmethod
    def in_transaction(self) -> bool:
        ...

    @abc.abstractmethod
    def _span_add_default_tags(self, span: opentracing.Span) -> None:
        ...

    @abc.abstractmethod
    def _start_span_from_rebalancing(self, name: str) -> opentracing.Span:
        ...
