import asyncio
from typing import Hashable
from unittest.mock import Mock

import pytest

from faust.types import TP
from tests.helpers import AsyncMock


class Test_AgentManager:
    def create_agent(self, name, topic_names=None):
        agent = Mock(
            name=name,
            maybe_start=AsyncMock(),
            stop=AsyncMock(),
            restart=AsyncMock(),
            on_partitions_revoked=AsyncMock(),
            on_partitions_assigned=AsyncMock(),
            get_topic_names=Mock(return_value=topic_names),
        )
        return agent

    @pytest.fixture()
    def agents(self, *, app):
        agents = app.agents
        return agents

    @pytest.fixture()
    def agent1(self):
        return self.create_agent("agent1", ["t1"])

    @pytest.fixture()
    def agent2(self):
        return self.create_agent("agent2", ["t1", "t2", "t3"])

    @pytest.fixture()
    def many(self, *, agents, agent1, agent2):
        agents["foo"] = agent1
        agents["bar"] = agent2
        return agents

    def test_constructor(self, *, agents, app):
        assert agents.app is app
        assert agents.data == {}
        assert agents._by_topic == {}

    @pytest.mark.asyncio
    async def test_on_stop__agent_raises_cancel(self, *, many, agent1, agent2):
        agent1.stop.side_effect = asyncio.CancelledError()
        agent2.stop.side_effect = asyncio.CancelledError()
        await many.on_stop()
        agent1.stop.assert_called_once_with()
        agent2.stop.assert_called_once_with()

    def test_hashable(self, *, agents):
        assert isinstance(agents, Hashable)
        assert hash(agents)

    @pytest.mark.asyncio
    async def test_start(self, *, many):
        await many.start()
        for agent in many.values():
            agent.maybe_start.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_restart(self, *, many):
        await many.restart()
        for agent in many.values():
            agent.stop.assert_called_once_with()

    def test_service_reset(self, *, many):
        many.service_reset()
        for agent in many.values():
            agent.service_reset.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop(self, *, many):
        await many.stop()
        for agent in many.values():
            agent.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_wait_until_agents_started(self, *, agents):
        agents.wait_for_stopped = AsyncMock()
        await agents.wait_until_agents_started()
        agents.wait_for_stopped.assert_called_once_with(
            agents._agents_started,
        )

    @pytest.mark.asyncio
    async def test_wait_until_agents_started__producer_only(self, *, app, agents):
        app.producer_only = True
        agents._agents_started.clear()
        await agents.wait_until_agents_started()

    @pytest.mark.asyncio
    async def test_wait_until_agents_started__client_only(self, *, app, agents):
        app.client_only = True
        agents._agents_started.clear()
        await agents.wait_until_agents_started()

    def test_update_topic_index(self, *, many, agent1, agent2):
        many.update_topic_index()
        assert set(many._by_topic["t1"]) == {agent1, agent2}
        assert set(many._by_topic["t2"]) == {agent2}
        assert set(many._by_topic["t3"]) == {agent2}

    @pytest.mark.asyncio
    async def test_on_rebalance(self, *, many, agent1, agent2):
        many.update_topic_index()
        revoked = {TP("t1", 0), TP("t1", 1), TP("t4", 3)}
        newly_assigned = {TP("t2", 0)}
        await many.on_rebalance(
            revoked=revoked,
            newly_assigned=newly_assigned,
        )
        agent1.on_partitions_revoked.assert_called_once_with(
            {TP("t1", 0), TP("t1", 1)},
        )
        agent2.on_partitions_revoked.assert_called_once_with(
            {TP("t1", 0), TP("t1", 1)},
        )
        agent2.on_partitions_assigned.assert_called_once_with(
            newly_assigned,
        )
