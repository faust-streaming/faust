"""Worker.

A "worker" starts a single instance of a Faust application.

See Also:
    :ref:`app-starting`: for more information.
"""
import asyncio
import logging
import os
import sys
from collections import defaultdict
from itertools import chain
from pathlib import Path
from typing import IO, Any, Dict, Iterable, Mapping, Optional, Set, Union

import mode
from aiokafka.structs import TopicPartition
from mode import ServiceT, get_logger
from mode.utils.logging import Severity, formatter2

from .types import TP, AppT, SensorT, TopicT
from .types._env import CONSOLE_PORT, DEBUG
from .utils import terminal
from .utils.functional import consecutive_numbers

try:  # pragma: no cover
    # if installed we use this to set ps.name (argv[0])
    from setproctitle import setproctitle
except ImportError:  # pragma: no cover

    def setproctitle(title: str) -> None:
        ...  # noqa


__all__ = ["Worker"]

#: Name prefix of process in ps/top listings.
PSIDENT = "[Faust:Worker]"

TP_TYPES = (TP, TopicPartition)

logger = get_logger(__name__)


@formatter2
def format_log_arguments(
    arg: Any, record: logging.LogRecord
) -> Any:  # pragma: no cover
    # This adds custom formatting to certain log messages.

    if arg and isinstance(arg, Mapping):
        first_k, first_v = next(iter(arg.items()))
        # Mapping of name to TopicT is changed to terminal table.
        if (
            isinstance(first_k, str)
            and isinstance(first_v, set)
            and isinstance(next(iter(first_v), None), TopicT)
        ):
            return "\n" + terminal.logtable(
                sorted(arg.items()),
                title="Subscription",
                headers=["Topic", "Descriptions"],
            )
        # Mapping where values are TopicPartition tuples are changed
        # to a terminal table.
        elif isinstance(first_v, TP_TYPES):
            return "\n" + terminal.logtable(
                [(k.topic, k.partition, v) for k, v in sorted(arg.items())],
                title="Topic Partition Map",
                headers=["topic", "partition", "offset"],
            )
    elif arg and isinstance(arg, (set, list)):
        if "Subscribed to topic" in record.msg:
            return "\n" + terminal.logtable(
                [[str(v)] for v in sorted(arg)],
                title="Final Subscription",
                headers=["topic name"],
            )
        elif isinstance(next(iter(arg)), TP_TYPES):
            # Sets/Lists of TopicPartition are converted to terminal table.
            return _partition_set_logtable(arg)

    elif arg and isinstance(arg, frozenset):
        if "subscribed topics to" in record.msg:
            # aiokafka emits a frozenset of topics,
            # and we convert this to a terminal table.
            return "\n" + terminal.logtable(
                [[str(v)] for v in sorted(arg)],
                title="Requested Subscription",
                headers=["topic name"],
            )
        elif isinstance(next(iter(arg)), TP_TYPES):
            # Sets/Lists of TopicPartition are converted to terminal table.
            return _partition_set_logtable(arg)


def _partition_set_logtable(arg: Iterable[TP]) -> str:
    topics: Dict[str, Set[int]] = defaultdict(set)
    for tp in arg:
        topics[tp.topic].add(tp.partition)

    return "\n" + terminal.logtable(
        [(k, _repr_partition_set(v)) for k, v in sorted(topics.items())],
        title="Topic Partition Set",
        headers=["topic", "partitions"],
    )


def _repr_partition_set(s: Set[int]) -> str:
    """Convert set of partition numbers to human readable form.

    This will consolidate ranges of partitions to make them easier
    to read.

    Example:
        >>> partitions = {1, 2, 3, 7, 8, 9, 10, 34, 35, 36, 37, 38, 50}
        >>> _repr_partition_set(partitions)
        '{1-3, 7-10, 34-38, 50}'
    """
    elements = ", ".join(_iter_consecutive_numbers(sorted(s)))
    return f"{{{elements}}}"


def _iter_consecutive_numbers(s: Iterable[int]) -> Iterable[str]:
    """Find consecutive number ranges from an iterable of integers.

    The number ranges are represented as strings (e.g. ``"3-14"``)

    Example:
        >>> numbers = {1, 2, 3, 7, 8, 9, 10, 34, 35, 36, 37, 38, 50}
        >>> list(_iter_consecutive_numbers(numbers))
        [1-3, 7-10, 34-38, 50]
    """
    for numbers in consecutive_numbers(s):
        if len(numbers) > 1:
            yield f"{numbers[0]}-{numbers[-1]}"
        else:
            yield f"{numbers[0]}"


class Worker(mode.Worker):
    """Worker.

    See Also:
        This is a subclass of :class:`mode.Worker`.

    Usage:
        You can start a worker using:

            1) the :program:`faust worker` program.

            2) instantiating Worker programmatically and calling
               `execute_from_commandline()`::

                    >>> worker = Worker(app)
                    >>> worker.execute_from_commandline()

            3) or if you already have an event loop, calling ``await start``,
               but in that case *you are responsible for gracefully shutting
               down the event loop*::

                    async def start_worker(worker: Worker) -> None:
                        await worker.start()

                    def manage_loop():
                        loop = asyncio.get_event_loop()
                        worker = Worker(app, loop=loop)
                        try:
                            loop.run_until_complete(start_worker(worker)
                        finally:
                            worker.stop_and_shutdown_loop()

    Arguments:
        app: The Faust app to start.
        *services: Services to start with worker.
            This includes application instances to start.

        sensors (Iterable[SensorT]): List of sensors to include.
        debug (bool): Enables debugging mode [disabled by default].
        quiet (bool): Do not output anything to console [disabled by default].
        loglevel (Union[str, int]): Level to use for logging, can be string
            (one of: CRIT|ERROR|WARN|INFO|DEBUG), or integer.
        logfile (Union[str, IO]): Name of file or a stream to log to.
        stdout (IO): Standard out stream.
        stderr (IO): Standard err stream.
        blocking_timeout (float): When :attr:`debug` is enabled this
            sets the timeout for detecting that the event loop is blocked.
        workdir (Union[str, Path]): Custom working directory for the process
            that the worker will change into when started.
            This working directory change is permanent for the process,
            or until something else changes the working directory again.
        loop (asyncio.AbstractEventLoop): Custom event loop object.
    """

    logger = logger

    #: The Faust app started by this worker.
    app: AppT

    #: Additional sensors to add to the Faust app.
    sensors: Set[SensorT]

    #: Current working directory.
    #: Note that if passed as an argument to Worker, the worker
    #: will change to this directory when started.
    workdir: Path

    #: Class that displays a terminal progress spinner (see :pypi:`progress`).
    spinner: Optional[terminal.Spinner]

    #: Set by signal to avoid printing an OK status.
    _shutdown_immediately: bool = False

    def __init__(
        self,
        app: AppT,
        *services: ServiceT,
        sensors: Optional[Iterable[SensorT]] = None,
        debug: bool = DEBUG,
        quiet: bool = False,
        loglevel: Union[str, int, None] = None,
        logfile: Union[str, IO, None] = None,
        stdout: IO = sys.stdout,
        stderr: IO = sys.stderr,
        blocking_timeout: Optional[float] = None,
        workdir: Union[Path, str, None] = None,
        console_port: int = CONSOLE_PORT,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        redirect_stdouts: Optional[bool] = None,
        redirect_stdouts_level: Optional[Severity] = None,
        logging_config: Optional[Dict] = None,
        **kwargs: Any,
    ) -> None:
        self.app = app
        self.sensors = set(sensors or [])
        self.workdir = Path(workdir or Path.cwd())
        conf = app.conf
        if redirect_stdouts is None:
            redirect_stdouts = conf.worker_redirect_stdouts
        if redirect_stdouts_level is None:
            redirect_stdouts_level = conf.worker_redirect_stdouts_level or logging.INFO
        if logging_config is None and app.conf.logging_config:
            logging_config = dict(app.conf.logging_config)
        super().__init__(
            *services,
            debug=debug,
            quiet=quiet,
            loglevel=loglevel,
            logfile=logfile,
            loghandlers=app.conf.loghandlers,
            stdout=stdout,
            stderr=stderr,
            blocking_timeout=blocking_timeout or 0.0,
            console_port=console_port,
            redirect_stdouts=redirect_stdouts,
            redirect_stdouts_level=redirect_stdouts_level,
            logging_config=logging_config,
            loop=loop,
            **kwargs,
        )
        self.spinner = terminal.Spinner(file=self.stdout)

    async def on_start(self) -> None:
        """Signal called every time the worker starts."""
        # This flag is set when running inside a worker process.
        self.app.in_worker = True

        await super().on_start()

    def _on_sigint(self) -> None:
        self._flag_as_shutdown_by_signal()
        super()._on_sigint()

    def _on_sigterm(self) -> None:
        self._flag_as_shutdown_by_signal()
        super()._on_sigterm()

    def _flag_as_shutdown_by_signal(self) -> None:
        self._shutdown_immediately = True
        if self.spinner:
            self.spinner.stop()

    async def maybe_start_blockdetection(self) -> None:
        """Start blocking detector service if enabled."""
        # the base class implemention of this
        # only starts the block detector if self.debug is set.
        if self.blocking_timeout:
            await self.blocking_detector.maybe_start()

    async def on_startup_finished(self) -> None:
        """Signal called when worker has started."""
        if self._shutdown_immediately:
            return self._on_shutdown_immediately()
        # block detection started here after changelog stuff,
        # and blocking RocksDB bulk updates.
        await self.maybe_start_blockdetection()

        # end progress spinner, or it will mangle terminal output.
        self._on_startup_end_spinner()

    def _on_startup_end_spinner(self) -> None:
        if self.spinner:
            self.spinner.finish()
            if self.spinner.file.isatty():
                self.say(" 😊")
            else:
                self.say(" OK ^")
        else:
            self.log.info("Ready")

    def _on_shutdown_immediately(self) -> None:
        self.say("")  # make sure spinner newlines.

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        """Return service dependencies that must start with the worker."""
        # App service is now a child of worker.
        self.app.beacon.reattach(self.beacon)
        # Transfer sensors to app
        for sensor in self.sensors:
            self.app.sensors.add(sensor)
        # Callback called once the app is running and fully
        # functional, we use it to e.g. print the "ready" message.
        self.app.on_startup_finished = self.on_startup_finished
        return chain(self.services, [self.app])

    async def on_first_start(self) -> None:
        """Signal called the first time the worker starts.

        First time, means this callback is not called if the
        worker is restarted by an exception being raised.
        """
        self.change_workdir(self.workdir)
        self.autodiscover()
        await self.default_on_first_start()

    def change_workdir(self, path: Path) -> None:
        """Change the current working directory (CWD)."""
        if path and path.absolute() != path.cwd().absolute():
            os.chdir(path.absolute())

    def autodiscover(self) -> None:
        """Autodiscover modules and files to find @agent decorators, etc."""
        if self.app.conf.autodiscover:
            self.app.discover()

    def _setproctitle(self, info: str, *, ident: str = PSIDENT) -> None:
        setproctitle(f"{ident} -{info}- {self._proc_ident()}")

    def _proc_ident(self) -> str:
        conf = self.app.conf
        return f"{conf.id} {self._proc_web_ident()} {conf.datadir.absolute()}"

    def _proc_web_ident(self) -> str:
        conf = self.app.conf
        if conf.web_transport.scheme == "unix":
            return f"{conf.web_transport}"
        return f"-p {conf.web_port}"

    async def on_execute(self) -> None:
        """Signal called when the worker is about to start."""
        # This is called as soon as we start
        self._setproctitle("init")
        if self.spinner and self.spinner.file.isatty():
            self._say("starting➢ ", end="", flush=True)

    def on_worker_shutdown(self) -> None:
        """Signal called before the worker is shutting down."""
        self._setproctitle("stopping")
        if self.spinner and self.spinner.file.isatty():
            self.spinner.reset()
            self._say("stopping➢ ", end="", flush=True)

    def on_setup_root_logger(self, logger: logging.Logger, level: int) -> None:
        """Signal called when the root logger is being configured."""
        # This is used to set up the terminal progress spinner
        # so that it spins for every log message emitted.
        self._disable_spinner_if_level_below_WARN(level)
        self._setup_spinner_handler(logger, level)

    def _disable_spinner_if_level_below_WARN(self, level: int) -> None:
        if level and level < logging.WARN:
            self.spinner = None

    def _setup_spinner_handler(self, logger: logging.Logger, level: int) -> None:
        if self.spinner:
            logger.handlers[0].setLevel(level)
            logger.addHandler(
                terminal.SpinnerHandler(self.spinner, level=logging.DEBUG)
            )
            logger.setLevel(logging.DEBUG)
