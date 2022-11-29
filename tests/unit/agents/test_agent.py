import asyncio
from unittest.mock import ANY, call, patch

import pytest
from mode import SupervisorStrategy, label
from mode.utils.aiter import aiter
from mode.utils.futures import done_future
from mode.utils.logging import CompositeLogger
from mode.utils.trees import Node

import faust
from faust import App, Channel, Record
from faust.agents.actor import Actor
from faust.agents.models import (
    ModelReqRepRequest,
    ModelReqRepResponse,
    ReqRepRequest,
    ReqRepResponse,
)
from faust.agents.replies import ReplyConsumer
from faust.events import Event
from faust.exceptions import ImproperlyConfigured
from faust.types import TP, Message
from tests.helpers import AsyncMock, FutureMock, Mock


class Word(Record):
    word: str


class Test_AgentService:
    @pytest.fixture
    def agent(self, *, app):
        @app.agent()
        async def myagent(stream):
            async for value in stream:
                yield value

        return myagent

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "concurrency,index,expected_index",
        [
            (1, 3, None),
            (10, 3, 3),
        ],
    )
    async def test_start_one(self, concurrency, index, expected_index, *, agent):
        agent.concurrency = concurrency
        agent._start_task = AsyncMock(name="_start_task")

        tps = {TP("foo", 0)}
        await agent._start_one(index=index, active_partitions=tps)
        agent._start_task.assert_called_once_with(
            index=expected_index,
            active_partitions=tps,
            stream=None,
            channel=None,
            beacon=agent.beacon,
        )

    @pytest.mark.asyncio
    async def test_start_for_partitions(self, *, agent):
        agent._start_one_supervised = AsyncMock(name="_start_one")
        s = agent._start_one_supervised.return_value = Mock(name="service")
        s.maybe_start = AsyncMock(name="agent.maybe_start")
        tps = {TP("foo", 3)}
        await agent._start_for_partitions(tps)
        agent._start_one_supervised.assert_called_once_with(None, tps)

    @pytest.mark.asyncio
    async def test_on_start(self, *, agent):
        agent._new_supervisor = Mock(name="new_supervisor")
        agent._on_start_supervisor = AsyncMock(name="on_start_supervisor")
        await agent.on_start()

        agent._new_supervisor.assert_called_once_with()
        assert agent.supervisor is agent._new_supervisor()
        agent._on_start_supervisor.assert_called_once_with()

    def test_new_supervisor(self, *, agent):
        strategy = agent._get_supervisor_strategy = Mock(name="strategy")
        s = agent._new_supervisor()
        strategy.assert_called_once_with()
        strategy.return_value.assert_called_once_with(
            max_restarts=100.0,
            over=1.0,
            replacement=agent._replace_actor,
            loop=agent.loop,
            beacon=agent.beacon,
        )
        assert s is strategy()()

    def test_get_supervisor_strategy(self, *, agent):
        agent.supervisor_strategy = 100
        assert agent._get_supervisor_strategy() == 100
        agent.supervisor_strategy = None
        agent.app = Mock(name="app", autospec=App)
        assert agent._get_supervisor_strategy() is agent.app.conf.agent_supervisor

    @pytest.mark.asyncio
    async def test_on_start_supervisor(self, *, agent):
        agent.concurrency = 10
        agent._get_active_partitions = Mock(name="_get_active_partitions")
        agent._start_one = AsyncMock(name="_start_one")
        agent.supervisor = Mock(
            name="supervisor",
            autospec=SupervisorStrategy,
            start=AsyncMock(),
        )
        await agent._on_start_supervisor()

        aref = agent._start_one.return_value

        agent._start_one.assert_has_calls(
            [
                call(
                    index=i,
                    channel=aref.stream.channel if i else None,
                    active_partitions=agent._get_active_partitions(),
                )
                for i in range(10)
            ]
        )
        agent.supervisor.add.assert_has_calls([call(aref) for _ in range(10)])
        agent.supervisor.start.assert_called_once_with()

    def test_get_active_partitions(self, *, agent):
        agent.isolated_partitions = None
        assert agent._get_active_partitions() is None
        agent.isolated_partitions = True
        assert agent._get_active_partitions() == set()
        assert agent._pending_active_partitions == set()

    @pytest.mark.asyncio
    async def test_replace_actor(self, *, agent):
        aref = Mock(name="aref", autospec=Actor)
        agent._start_one = AsyncMock(name="_start_one")
        assert await agent._replace_actor(aref, 101) == agent._start_one.return_value
        agent._start_one.assert_called_once_with(
            index=101,
            active_partitions=aref.active_partitions,
            stream=aref.stream,
            channel=aref.stream.channel,
        )

    @pytest.mark.asyncio
    async def test_on_stop(self, *, agent):
        agent._stop_supervisor = AsyncMock(name="_stop_supervisor")
        await agent.on_stop()
        agent._stop_supervisor.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_stop_supervisor(self, *, agent):
        supervisor = agent.supervisor = Mock(
            name="supervisor",
            autospec=SupervisorStrategy,
            stop=AsyncMock(),
        )
        await agent._stop_supervisor()
        assert agent.supervisor is None
        await agent._stop_supervisor()
        supervisor.stop.assert_called_once_with()

    def test_label(self, *, agent):
        assert label(agent)


class Test_Agent:
    @pytest.fixture
    def agent(self, *, app):
        @app.agent()
        async def myagent(stream):
            async for value in stream:
                yield value

        return myagent

    @pytest.fixture
    def isolated_agent(self, *, app):
        @app.agent(isolated_partitions=True)
        async def isoagent(stream):
            async for value in stream:
                yield value

        return isoagent

    @pytest.fixture
    def foo_topic(self, *, app):
        return app.topic("foo")

    @pytest.fixture
    def agent2(self, *, app, foo_topic):
        @app.agent(foo_topic)
        async def other_agent(stream):
            async for value in stream:
                value

        return other_agent

    def test_init_schema_and_channel(self, *, app):
        with pytest.raises(AssertionError):

            @app.agent(app.topic("foo"), schema=faust.Schema(key_type=bytes))
            async def foo():
                ...

    def test_init_key_type_and_channel(self, *, app):
        with pytest.raises(AssertionError):

            @app.agent(app.topic("foo"), key_type=bytes)
            async def foo():
                ...

    def test_init_value_type_and_channel(self, *, app):
        with pytest.raises(AssertionError):

            @app.agent(app.topic("foo"), value_type=bytes)
            async def foo():
                ...

    def test_isolated_partitions_cannot_have_concurrency(self, *, app):
        with pytest.raises(ImproperlyConfigured):

            @app.agent(isolated_partitions=True, concurrency=100)
            async def foo():
                ...

    def test_agent_call_reuse_stream(self, *, agent, app):
        stream = app.stream("foo")
        stream.concurrency_index = 1
        stream.active_partitions = {1, 2}
        actor = agent(stream=stream, index=1, active_partitions={1, 2})
        assert actor.stream is stream

    def test_cancel(self, *, agent):
        actor1 = Mock(name="actor1")
        actor2 = Mock(name="actor2")
        agent._actors = [actor1, actor2]
        agent.cancel()
        actor1.cancel.assert_called_once_with()
        actor2.cancel.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_on_partitions_revoked(self, *, agent):
        revoked = {TP("foo", 0)}
        agent.on_shared_partitions_revoked = AsyncMock(name="ospr")
        await agent.on_partitions_revoked(revoked)
        agent.on_shared_partitions_revoked.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_revoked__isolated(self, *, isolated_agent):
        revoked = {TP("foo", 0)}
        i = isolated_agent.on_isolated_partitions_revoked = AsyncMock(name="i")
        await isolated_agent.on_partitions_revoked(revoked)
        i.assert_called_once_with(revoked)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned(self, *, agent):
        assigned = {TP("foo", 0)}
        agent.on_shared_partitions_assigned = AsyncMock(name="ospr")
        await agent.on_partitions_assigned(assigned)
        agent.on_shared_partitions_assigned.assert_called_once_with(assigned)

    @pytest.mark.asyncio
    async def test_on_partitions_assigned__isolated(self, *, isolated_agent):
        assigned = {TP("foo", 0)}
        i = isolated_agent.on_isolated_partitions_assigned = AsyncMock()
        await isolated_agent.on_partitions_assigned(assigned)
        i.assert_called_once_with(assigned)

    @pytest.mark.asyncio
    async def test_on_isolated_partitions_revoked(self, *, agent):
        tp = TP("foo", 0)
        aref = Mock(
            name="aref",
            autospec=Actor,
            on_isolated_partition_revoked=AsyncMock(),
        )
        agent._actor_by_partition = {tp: aref}

        await agent.on_isolated_partitions_revoked({tp})
        aref.on_isolated_partition_revoked.assert_called_once_with(tp)
        assert not agent._actor_by_partition
        await agent.on_isolated_partitions_revoked({tp})

    @pytest.mark.asyncio
    async def test_on_isolated_partitions_assigned(self, *, agent):
        agent._assign_isolated_partition = AsyncMock(name="aip")
        await agent.on_isolated_partitions_assigned({TP("foo", 0)})
        agent._assign_isolated_partition.assert_called_once_with(TP("foo", 0))

    @pytest.mark.asyncio
    async def test_assign_isolated_partition(self, *, agent):
        agent._on_first_isolated_partition_assigned = Mock(name="ofipa")
        agent._maybe_start_isolated = AsyncMock(name="maybe_start_isolated")
        agent._first_assignment_done = True

        tp = TP("foo", 606)
        await agent._assign_isolated_partition(tp)

        agent._on_first_isolated_partition_assigned.assert_not_called()
        agent._maybe_start_isolated.assert_called_once_with(tp)

        agent._first_assignment_done = False
        agent._actor_by_partition = set()
        await agent._assign_isolated_partition(tp)
        agent._on_first_isolated_partition_assigned.assert_called_once_with(tp)

    def test_on_first_isolated_partition_assigned(self, *, agent):
        aref = Mock(name="actor", autospec=Actor)
        agent._actors = [aref]
        agent._pending_active_partitions = set()
        tp = TP("foo", 303)
        agent._on_first_isolated_partition_assigned(tp)
        assert agent._actor_by_partition[tp] is aref
        assert agent._pending_active_partitions == {tp}
        agent._pending_active_partitions = None
        agent._on_first_isolated_partition_assigned(tp)

    @pytest.mark.asyncio
    async def test_maybe_start_isolated(self, *, isolated_agent):
        aref = Mock(
            name="actor",
            autospec=Actor,
            on_isolated_partition_assigned=AsyncMock(),
        )
        isolated_agent._start_isolated = AsyncMock(
            name="_start_isolated",
            return_value=aref,
        )
        tp = TP("foo", 303)
        await isolated_agent._maybe_start_isolated(tp)

        isolated_agent._start_isolated.assert_called_once_with(tp)
        assert isolated_agent._actor_by_partition[tp] is aref
        aref.on_isolated_partition_assigned.assert_called_once_with(tp)

    @pytest.mark.asyncio
    async def test_start_isolated(self, *, agent):
        agent._start_for_partitions = AsyncMock(
            name="agent._start_for_partitions",
        )
        ret = await agent._start_isolated(TP("foo", 0))
        agent._start_for_partitions.assert_called_once_with({TP("foo", 0)})
        assert ret is agent._start_for_partitions.return_value

    @pytest.mark.asyncio
    async def test_on_shared_partitions_revoked(self, *, agent):
        await agent.on_shared_partitions_revoked(set())

    @pytest.mark.asyncio
    async def test_on_shared_partitions_assigned(self, *, agent):
        await agent.on_shared_partitions_assigned(set())

    def test_info(self, *, agent):
        assert agent.info() == {
            "app": agent.app,
            "fun": agent.fun,
            "name": agent.name,
            "channel": agent.channel,
            "concurrency": agent.concurrency,
            "help": agent.help,
            "sink": agent._sinks,
            "on_error": agent._on_error,
            "supervisor_strategy": agent.supervisor_strategy,
            "isolated_partitions": agent.isolated_partitions,
        }

    def test_clone(self, *, agent):
        assert agent.clone(isolated_partitions=True).isolated_partitions

    def test_stream__active_partitions(self, *, agent):
        assert agent.stream(active_partitions={TP("foo", 0)})

    @pytest.mark.parametrize(
        "input,expected",
        [
            (ReqRepRequest("value", "reply_to", "correlation_id"), "value"),
            ("value", "value"),
        ],
    )
    def test_maybe_unwrap_reply_request(self, input, expected, *, agent):
        assert agent._maybe_unwrap_reply_request(input) == expected

    @pytest.mark.asyncio
    async def test_start_task(self, *, agent):
        agent._prepare_actor = AsyncMock(name="_prepare_actor")
        ret = await agent._start_task(index=0)
        agent._prepare_actor.assert_called_once_with(ANY, agent.beacon)
        assert ret is agent._prepare_actor.return_value

    @pytest.mark.asyncio
    async def test_prepare_actor__AsyncIterable(self, *, agent):
        aref = agent(index=0, active_partitions=None)
        with patch("asyncio.Task") as Task:
            agent._slurp = Mock(name="_slurp")
            agent._execute_actor = Mock(name="_execute_actor")
            beacon = Mock(name="beacon", autospec=Node)
            ret = await agent._prepare_actor(aref, beacon)
            agent._slurp.assert_called()
            coro = agent._slurp()
            agent._execute_actor.assert_called_once_with(coro, aref)
            Task.assert_called_once_with(agent._execute_actor(), loop=agent.loop)
            task = Task()
            assert task._beacon is beacon
            assert aref.actor_task is task
            assert aref in agent._actors
            assert ret is aref

    @pytest.mark.asyncio
    async def test_prepare_actor__Awaitable(self, *, agent2):
        aref = agent2(index=0, active_partitions=None)
        asyncio.ensure_future(aref.it).cancel()  # silence warning
        return
        with patch("asyncio.Task") as Task:
            agent2._execute_actor = Mock(name="_execute_actor")
            beacon = Mock(name="beacon", autospec=Node)
            ret = await agent2._prepare_actor(aref, beacon)
            coro = aref
            agent2._execute_actor.assert_called_once_with(coro, aref)
            Task.assert_called_once_with(agent2._execute_actor(), loop=agent2.loop)
            task = Task()
            assert task._beacon is beacon
            assert aref.actor_task is task
            assert aref in agent2._actors
            assert ret is aref

    @pytest.mark.asyncio
    async def test_prepare_actor__Awaitable_with_multiple_topics(self, *, agent2):
        aref = agent2(index=0, active_partitions=None)
        asyncio.ensure_future(aref.it).cancel()  # silence warning
        agent2.channel.topics = ["foo", "bar"]
        with patch("asyncio.Task") as Task:
            agent2._execute_actor = Mock(name="_execute_actor")
            beacon = Mock(name="beacon", autospec=Node)
            ret = await agent2._prepare_actor(aref, beacon)
            coro = aref
            agent2._execute_actor.assert_called_once_with(coro, aref)
            Task.assert_called_once_with(agent2._execute_actor(), loop=agent2.loop)
            task = Task()
            assert task._beacon is beacon
            assert aref.actor_task is task
            assert aref in agent2._actors
            assert ret is aref

    @pytest.mark.asyncio
    async def test_prepare_actor__Awaitable_cannot_have_sinks(self, *, agent2):
        aref = agent2(index=0, active_partitions=None)
        asyncio.ensure_future(aref.it).cancel()  # silence warning
        agent2._sinks = [agent2]
        with pytest.raises(ImproperlyConfigured):
            await agent2._prepare_actor(
                aref,
                Mock(name="beacon", autospec=Node),
            )

    @pytest.mark.asyncio
    async def test_execute_actor(self, *, agent):
        coro = done_future()
        await agent._execute_actor(coro, Mock(name="aref", autospec=Actor))

    @pytest.mark.asyncio
    async def test_execute_actor__cancelled_stopped(self, *, agent):
        coro = FutureMock()
        coro.side_effect = asyncio.CancelledError()
        await agent.stop()
        with pytest.raises(asyncio.CancelledError):
            await agent._execute_actor(coro, Mock(name="aref", autospec=Actor))
        coro.assert_awaited()

    @pytest.mark.skip(reason="Fix is TBD")
    @pytest.mark.asyncio
    async def test_execute_actor__cancelled_running(self, *, agent):
        agent._on_error = AsyncMock(name="on_error")
        agent.log = Mock(name="log", autospec=CompositeLogger)
        aref = Mock(
            name="aref",
            autospec=Actor,
            crash=AsyncMock(),
        )
        agent.supervisor = Mock(name="supervisor")
        coro = FutureMock()
        exc = coro.side_effect = asyncio.CancelledError()
        await agent._execute_actor(coro, aref)
        coro.assert_awaited()

        aref.crash.assert_called_once_with(exc)
        agent.supervisor.wakeup.assert_called_once_with()
        agent._on_error.assert_not_called()

        agent._on_error = None
        await agent._execute_actor(coro, aref)

    @pytest.mark.asyncio
    async def test_execute_actor__raising(self, *, agent):
        agent._on_error = AsyncMock(name="on_error")
        agent.log = Mock(name="log", autospec=CompositeLogger)
        aref = Mock(
            name="aref",
            autospec=Actor,
            crash=AsyncMock(),
        )
        agent.supervisor = Mock(name="supervisor")
        coro = FutureMock()
        exc = coro.side_effect = KeyError("bar")
        await agent._execute_actor(coro, aref)
        coro.assert_awaited()

        aref.crash.assert_called_once_with(exc)
        agent.supervisor.wakeup.assert_called_once_with()
        agent._on_error.assert_called_once_with(agent, exc)

        agent._on_error = None
        await agent._execute_actor(coro, aref)

    @pytest.mark.asyncio
    async def test_slurp(self, *, agent, app):
        aref = agent(index=None, active_partitions=None)
        stream = aref.stream.get_active_stream()
        agent._delegate_to_sinks = AsyncMock(name="_delegate_to_sinks")
        agent._reply = AsyncMock(name="_reply")

        def on_delegate(value):
            raise StopAsyncIteration()

        word = Word("word")
        word_req = ReqRepRequest(word, "reply_to", "correlation_id")
        message1 = Mock(name="message1", autospec=Message)
        message2 = Mock(name="message2", autospec=Message)
        event1 = Event(app, None, word_req, {}, message1)
        event2 = Event(app, "key", "bar", {}, message2)
        values = [
            (event1, word),
            (event2, "bar"),
        ]

        class AIT:
            async def __aiter__(self):
                for event, value in values:
                    stream.current_event = event
                    yield value

        it = aiter(AIT())
        await agent._slurp(aref, it)

        agent._reply.assert_called_once_with(
            None, word, word_req.reply_to, word_req.correlation_id
        )
        agent._delegate_to_sinks.assert_has_calls(
            [
                call(word),
                call("bar"),
            ]
        )

    @pytest.mark.asyncio
    async def test_slurp__headers(self, *, agent, app):
        agent.use_reply_headers = True
        aref = agent(index=None, active_partitions=None)
        stream = aref.stream.get_active_stream()
        agent._delegate_to_sinks = AsyncMock(name="_delegate_to_sinks")
        agent._reply = AsyncMock(name="_reply")

        def on_delegate(value):
            raise StopAsyncIteration()

        word = Word("word")
        message1 = Mock(name="message1", autospec=Message)
        headers1 = message1.headers = {
            "Faust-Ag-ReplyTo": "reply_to",
            "Faust-Ag-CorrelationId": "correlation_id",
        }
        message2 = Mock(name="message2", autospec=Message)
        headers2 = message2.headers = {}
        event1 = Event(app, None, word, headers1, message1)
        event2 = Event(app, "key", "bar", headers2, message2)
        values = [
            (event1, word, True),
            (event1, word, False),
            (event2, "bar", True),
        ]

        class AIT:
            async def __aiter__(self):
                for event, value, set_cur_event in values:
                    if set_cur_event:
                        stream.current_event = event
                    else:
                        stream.current_event = None
                    yield value

        it = aiter(AIT())
        await agent._slurp(aref, it)

        agent._reply.assert_called_once_with(None, word, "reply_to", "correlation_id")
        agent._delegate_to_sinks.assert_has_calls(
            [
                call(word),
                call("bar"),
            ]
        )

    @pytest.mark.asyncio
    async def test_delegate_to_sinks(self, *, agent, agent2, foo_topic):
        agent2.send = AsyncMock(name="agent2.send")
        foo_topic.send = AsyncMock(name="foo_topic.send")
        sink_callback = Mock(name="sink_callback")
        sink_callback2_mock = Mock(name="sink_callback2_mock")

        async def sink_callback2(value):
            return sink_callback2_mock(value)

        agent._sinks = [
            agent2,
            foo_topic,
            sink_callback,
            sink_callback2,
        ]

        value = Mock(name="value")
        await agent._delegate_to_sinks(value)

        agent2.send.assert_called_once_with(value=value)
        foo_topic.send.assert_called_once_with(value=value)
        sink_callback.assert_called_once_with(value)
        sink_callback2_mock.assert_called_once_with(value)

    @pytest.mark.asyncio
    async def test_reply(self, *, agent):
        agent.app = Mock(
            name="app",
            autospec=App,
            send=AsyncMock(),
        )
        req = ReqRepRequest("value", "reply_to", "correlation_id")
        await agent._reply("key", "reply", req.reply_to, req.correlation_id)
        agent.app.send.assert_called_once_with(
            req.reply_to,
            key=None,
            value=ReqRepResponse(
                key="key",
                value="reply",
                correlation_id=req.correlation_id,
            ),
        )

    @pytest.mark.asyncio
    async def test_cast(self, *, agent):
        agent.send = AsyncMock(name="send")
        await agent.cast("value", key="key", partition=303)
        agent.send.assert_called_once_with(
            key="key",
            value="value",
            partition=303,
            headers=None,
            timestamp=None,
        )

    @pytest.mark.asyncio
    async def test_ask(self, *, agent):
        agent.app = Mock(
            name="app",
            autospec=App,
            maybe_start_client=AsyncMock(),
            _reply_consumer=Mock(
                autospec=ReplyConsumer,
                add=AsyncMock(),
            ),
        )
        pp = done_future()
        agent.ask_nowait = Mock(name="ask_nowait")
        agent.ask_nowait.return_value = done_future(pp)
        pp.correlation_id = "foo"

        await agent.ask(
            value="val",
            key="key",
            partition=303,
            correlation_id="correlation_id",
            headers={"k1": "v1"},
        )
        agent.ask_nowait.assert_called_once_with(
            "val",
            key="key",
            partition=303,
            reply_to=agent.app.conf.reply_to,
            correlation_id="correlation_id",
            force=True,
            timestamp=None,
            headers={"k1": "v1"},
        )
        agent.app._reply_consumer.add.assert_called_once_with(pp.correlation_id, pp)

    @pytest.mark.asyncio
    async def test_ask_nowait(self, *, agent):
        agent._create_req = Mock(name="_create_req")
        agent._create_req.return_value = ["V", None]
        agent.channel.send = AsyncMock(name="channel.send")
        res = await agent.ask_nowait(
            value="value",
            key="key",
            partition=303,
            timestamp=None,
            headers=None,
            reply_to="reply_to",
            correlation_id="correlation_id",
            force=True,
        )

        agent._create_req.assert_called_once_with(
            "key", "value", "reply_to", "correlation_id", None
        )
        agent.channel.send.assert_called_once_with(
            key="key",
            value=agent._create_req()[0],
            partition=303,
            timestamp=None,
            force=True,
            headers=None,
        )

        assert res.reply_to
        assert res.correlation_id

    @pytest.mark.asyncio
    async def test_ask_nowait__missing_reply_to(self, *, agent):
        with pytest.raises(TypeError):
            await agent.ask_nowait(
                value="value",
                key="key",
                partition=3034,
                reply_to=None,
            )

    def test_create_req(self, *, agent):
        agent.use_reply_headers = False
        agent._get_strtopic = Mock(name="_get_strtopic")
        with patch("faust.agents.agent.uuid4") as uuid4:
            uuid4.return_value = "vvv"
            reqrep = agent._create_req(
                key=b"key", value=b"value", reply_to="reply_to", headers={"k": "v"}
            )[0]

            agent._get_strtopic.assert_called_once_with("reply_to")

            assert reqrep.value == b"value"
            assert reqrep.reply_to == agent._get_strtopic()
            assert reqrep.correlation_id == "vvv"

    def test_create_req__use_reply_headers(self, *, agent):
        agent.use_reply_headers = True
        agent._get_strtopic = Mock(name="_get_strtopic")
        with patch("faust.agents.agent.uuid4") as uuid4:
            uuid4.return_value = "vvv"
            value, h = agent._create_req(
                key=b"key", value=b"value", reply_to="reply_to", headers={"k": "v"}
            )

            agent._get_strtopic.assert_called_once_with("reply_to")

            assert value == b"value"
            assert h["Faust-Ag-ReplyTo"] == agent._get_strtopic()
            assert h["Faust-Ag-CorrelationId"] == "vvv".encode()

    def test_create_req__model(self, *, agent):
        agent.use_reply_headers = False
        agent._get_strtopic = Mock(name="_get_strtopic")
        with patch("faust.agents.agent.uuid4") as uuid4:
            uuid4.return_value = "vvv"
            value = Word("foo")
            reqrep = agent._create_req(
                key=b"key", value=value, reply_to="reply_to", headers={"h1": "h2"}
            )[0]
            assert isinstance(reqrep, ReqRepRequest)

            agent._get_strtopic.assert_called_once_with("reply_to")
            assert isinstance(reqrep, ModelReqRepRequest)

            assert reqrep.value is value
            assert reqrep.reply_to == agent._get_strtopic()
            assert reqrep.correlation_id == "vvv"

    def test_create_req__requires_reply_to(self, *, agent):
        with pytest.raises(TypeError):
            agent._create_req(
                key=b"key",
                value=b"value",
                reply_to=None,
            )

    @pytest.mark.parametrize(
        "value,expected_class",
        [
            (b"value", ReqRepResponse),
            (Word("foo"), ModelReqRepResponse),
        ],
    )
    def test_response_class(self, value, expected_class, *, agent):
        assert agent._response_class(value) is expected_class

    @pytest.mark.asyncio
    async def test_send(self, *, agent):
        agent.channel = Mock(
            name="channel",
            autospec=Channel,
            send=AsyncMock(),
        )
        agent._create_req = Mock(name="_create_req")
        agent._create_req.return_value = ("V", {"k2": "v2"})
        callback = Mock(name="callback")

        ret = await agent.send(
            key=b"key",
            value=b"value",
            partition=303,
            timestamp=None,
            headers={"k": "v"},
            key_serializer="raw",
            value_serializer="raw",
            callback=callback,
            reply_to="reply_to",
            correlation_id="correlation_id",
            force=True,
        )

        agent._create_req.assert_called_once_with(
            b"key",
            b"value",
            "reply_to",
            "correlation_id",
            {"k": "v"},
        )

        agent.channel.send.assert_called_once_with(
            key=b"key",
            value=agent._create_req()[0],
            partition=303,
            timestamp=None,
            headers={"k2": "v2"},
            key_serializer="raw",
            value_serializer="raw",
            force=True,
        )

        assert ret is agent.channel.send.return_value

    @pytest.mark.asyncio
    async def test_send__without_reply_to(self, *, agent):
        agent.channel = Mock(
            name="channel",
            autospec=Channel,
            send=AsyncMock(),
        )
        agent._create_req = Mock(name="_create_req")
        callback = Mock(name="callback")

        ret = await agent.send(
            key=b"key",
            value=b"value",
            partition=303,
            timestamp=None,
            headers={"k": "v"},
            key_serializer="raw",
            value_serializer="raw",
            callback=callback,
            reply_to=None,
            correlation_id="correlation_id",
            force=True,
        )

        agent._create_req.assert_not_called()

        agent.channel.send.assert_called_once_with(
            key=b"key",
            value=b"value",
            partition=303,
            timestamp=None,
            headers={"k": "v"},
            key_serializer="raw",
            value_serializer="raw",
            force=True,
        )

        assert ret is agent.channel.send.return_value

    def test_get_strtopic__agent(self, *, agent, agent2):
        assert agent._get_strtopic(agent2) == agent2.channel.get_topic_name()

    def test_get_strtopic__topic(self, *, agent, foo_topic):
        assert agent._get_strtopic(foo_topic) == foo_topic.get_topic_name()

    def test_get_strtopic__str(self, *, agent):
        assert agent._get_strtopic("bar") == "bar"

    def test_get_strtopic__channel_raises(self, *, agent, app):
        with pytest.raises(ValueError):
            agent._get_strtopic(app.channel())

    def test_get_topic_names(self, *, agent, app):
        agent.channel = app.topic("foo")
        assert agent.get_topic_names() == ("foo",)

    def test_get_topic_names__channel(self, *, agent, app):
        agent.channel = app.channel()
        assert agent.get_topic_names() == []

    def test_repr(self, *, agent):
        assert repr(agent)

    def test_channel(self, *, agent):
        agent._prepare_channel = Mock(name="_prepare_channel")
        agent._channel = None
        channel = agent.channel
        agent._prepare_channel.assert_called_once_with(
            agent._channel_arg,
            schema=agent._schema,
            key_type=agent._key_type,
            value_type=agent._value_type,
            **agent._channel_kwargs,
        )
        assert channel is agent._prepare_channel.return_value
        assert agent._channel is channel

    def test_prepare_channel__not_channel(self, *, agent):
        with pytest.raises(TypeError):
            agent._prepare_channel(object())

    def test_add_sink(self, *, agent, agent2):
        agent.add_sink(agent2)
        assert agent2 in agent._sinks
        agent.add_sink(agent2)

    def test_channel_iterator(self, *, agent):
        agent.channel = Mock(name="channel", autospec=Channel)
        agent._channel_iterator = None
        it = agent.channel_iterator

        agent.channel.clone.assert_called_once_with(is_iterator=False)
        assert it is agent.channel.clone()
        agent.channel_iterator = [42]
        assert agent.channel_iterator == [42]

    def test_label(self, *, agent):
        assert label(agent)

    async def test_context_calls_sink(self, *, agent):
        class SinkCalledException(Exception):
            pass

        def dummy_sink(_):
            raise SinkCalledException()

        agent.add_sink(dummy_sink)
        async with agent.test_context() as agent_mock:
            with pytest.raises(SinkCalledException):
                await agent_mock.put("hello")
