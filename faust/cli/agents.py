"""Program ``faust agents`` used to list agents."""
from operator import attrgetter
from typing import Any, Callable, Optional, Sequence, Type, cast

from faust.types import AgentT

from .base import AppCommand, option


class agents(AppCommand):
    """List agents."""

    title = "Agents"
    headers = ["name", "topic", "help"]
    sortkey = attrgetter("name")

    options = [
        option("--local/--no-local", help="Include agents using a local channel"),
    ]

    async def run(self, local: bool) -> None:
        """Dump list of available agents in this application."""
        self.say(
            self.tabulate(
                [self.agent_to_row(agent) for agent in self.agents(local=local)],
                headers=self.headers,
                title=self.title,
            )
        )

    def agents(self, *, local: bool = False) -> Sequence[AgentT]:
        """Convert list of agents to terminal table rows."""
        sortkey = cast(Callable[[Type[AgentT]], Any], self.sortkey)
        return [
            agent
            for agent in sorted(self.app.agents.values(), key=sortkey)
            if self._maybe_topic(agent) or local
        ]

    def agent_to_row(self, agent: AgentT) -> Sequence[str]:
        """Convert agent fields to terminal table row."""
        return [
            self._name(agent),
            self._topic(agent),
            self._help(agent),
        ]

    def _name(self, agent: AgentT) -> str:
        return "@" + self.abbreviate_fqdn(agent.name)

    def _maybe_topic(self, agent: AgentT) -> Optional[str]:
        try:
            return agent.channel.get_topic_name()
        except NotImplementedError:
            return None

    def _topic(self, agent: AgentT) -> str:
        return self._maybe_topic(agent) or "<LOCAL>"

    def _help(self, agent: AgentT) -> str:
        return agent.help or "<N/A>"
