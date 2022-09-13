"""Actor - Individual Agent instances."""
from typing import Any, AsyncGenerator, AsyncIterator, Coroutine, Optional, Set, cast

from mode import Service
from mode.utils.tracebacks import format_agen_stack, format_coro_stack

from faust.types import TP, ChannelT, StreamT
from faust.types.agents import _T, ActorT, AgentT, AsyncIterableActorT, AwaitableActorT

__all__ = ["Actor", "AsyncIterableActor", "AwaitableActor"]


class Actor(ActorT, Service):
    """An actor is a specific agent instance."""

    mundane_level = "debug"

    # Agent will start n * concurrency actors.

    def __init__(
        self,
        agent: AgentT,
        stream: StreamT,
        it: _T,
        index: Optional[int] = None,
        active_partitions: Optional[Set[TP]] = None,
        **kwargs: Any,
    ) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        self.index = index
        self.active_partitions = active_partitions
        self.actor_task = None
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        """Call when actor is starting."""
        assert self.actor_task
        self.add_future(self.actor_task)

    async def on_stop(self) -> None:
        """Call when actor is being stopped."""
        self.cancel()

    async def on_isolated_partition_revoked(self, tp: TP) -> None:
        """Call when an isolated partition is being revoked."""
        self.log.debug("Cancelling current task in actor for partition %r", tp)
        self.cancel()
        self.log.info("Stopping actor for revoked partition %r...", tp)
        await self.stop()
        self.log.debug("Actor for revoked partition %r stopped")

    async def on_isolated_partition_assigned(self, tp: TP) -> None:
        """Call when an isolated partition is being assigned."""
        self.log.dev("Actor was assigned to %r", tp)

    def cancel(self) -> None:
        """Tell actor to stop reading from the stream."""
        cast(ChannelT, self.stream.channel)._throw(StopAsyncIteration())

    def __repr__(self) -> str:
        return f"<{self.shortlabel}>"

    @property
    def label(self) -> str:
        """Return human readable description of actor."""
        s = self.agent._agent_label(name_suffix="*")
        if self.stream.active_partitions:
            partitions = {tp.partition for tp in self.stream.active_partitions}
            s += f" isolated={partitions}"
        return s


class AsyncIterableActor(AsyncIterableActorT, Actor):
    """Used for agent function that yields."""

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()

    def traceback(self) -> str:
        return format_agen_stack(cast(AsyncGenerator, self.it))


class AwaitableActor(AwaitableActorT, Actor):
    """Used for actor function that do not yield."""

    def __await__(self) -> Any:
        return self.it.__await__()

    def traceback(self) -> str:
        return format_coro_stack(cast(Coroutine, self.it))
