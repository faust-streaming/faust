import asyncio
from typing import Any, Optional
from unittest.mock import Mock, PropertyMock, call

import pytest

from faust.transport.producer import Producer, ProducerBuffer
from tests.helpers import AsyncMock


class TestProducerBuffer:
    @pytest.fixture()
    def buf(self, app):
        producer = ProducerBuffer()
        producer.app = app
        return producer

    def test_put(self, *, buf):
        fut = Mock(name="future_message")
        buf.pending = Mock(name="pending")
        buf.put(fut)

        buf.pending.put_nowait.assert_called_once_with(fut)

    @pytest.mark.asyncio
    async def test_on_stop(self, *, buf):
        buf.flush = AsyncMock(name="flush")
        await buf.on_stop()
        buf.flush.assert_called_once_with()

    @pytest.mark.asyncio
    async def test__send_pending(self, *, buf):
        fut = Mock(name="future_message")
        fut.message.channel.publish_message = AsyncMock()
        await buf._send_pending(fut)
        fut.message.channel.publish_message.assert_called_once_with(
            fut,
            wait=False,
        )

    @pytest.mark.asyncio
    async def test__handle_pending(self, *, buf):
        buf.pending = Mock(get=AsyncMock())
        buf._send_pending = AsyncMock()

        async def on_send(fut):
            if buf._send_pending.call_count >= 3:
                buf._stopped.set()

        buf._send_pending.side_effect = on_send

        await buf._handle_pending(buf)

        buf._send_pending.assert_has_calls(
            [
                call(buf.pending.get.return_value),
                call(buf.pending.get.return_value),
            ]
        )

    @pytest.mark.asyncio
    async def test_wait_until_ebb(self, *, buf):
        buf.max_messages = 10

        flush_atmost_call_count = 0

        async def flush_atmost(max_messages: Optional[int]) -> int:
            assert (
                max_messages is None or max_messages == buf.max_messages
            ), "Max messages set not to the max messages buffer"
            nonlocal flush_atmost_call_count
            flush_atmost_call_count += 1

            await asyncio.sleep(0)
            return 0

        buf.flush_atmost = flush_atmost
        await buf.start()
        original_size = buf.__class__.size
        loop = asyncio.get_event_loop()
        try:
            buf.__class__.size = PropertyMock(return_value=20)

            task = loop.create_task(buf.wait_until_ebb())
            await asyncio.sleep(0)
            assert flush_atmost_call_count == 1
            assert not task.done(), (
                "The wait_until_ebb has been finished even "
                "though flush atmost did not return"
            )

            buf.__class__.size = PropertyMock(return_value=10)
            await asyncio.sleep(0)
            assert task.done(), (
                "The wait_until_ebb did not complete even "
                "though the size is beneath the max size"
            )

            assert (
                flush_atmost_call_count > 0
            ), "The wait_until_ebb did not call the flush_atmost function"
            task = loop.create_task(buf.wait_until_ebb())
            await asyncio.sleep(0)
            assert task.done(), (
                "The wait_until_ebb function did not finish even "
                "though the buffer is small enough"
            )
        finally:
            buf.__class__.size = original_size
            await buf.stop()

    @pytest.mark.asyncio
    async def test_flush(self, *, buf):
        buf.flush_atmost = AsyncMock(return_value=0)
        await buf.flush()
        buf.flush_atmost.assert_called()

    def _put(self, buf, items):
        for item in items:
            buf.pending.put_nowait(item)

    @pytest.mark.asyncio
    async def test_flush_atmost(self, *, buf):
        sent_messages = 0

        def create_send_pending_mock(max_messages):
            nonlocal sent_messages

            async def _inner(*args: Any):
                nonlocal sent_messages
                if sent_messages < max_messages:
                    sent_messages += 1
                    return
                else:
                    await asyncio.Future()

            return _inner

        await buf.start()
        buf._send_pending = create_send_pending_mock(13)

        try:
            assert await buf.flush_atmost(10) == 0

            self._put(buf, range(3))
            assert buf.size == 3
            assert await buf.flush_atmost(10) > 0
            assert sent_messages == 3

            self._put(buf, range(10))
            assert buf.size == 10
            await buf.flush_atmost(4)

            await buf.flush_atmost(6)
            assert not buf.size
            assert sent_messages == 13
        finally:
            await buf.stop()

    @pytest.mark.asyncio
    async def test_flush_atmost_with_simulated_threaded_behavior(self, *, buf):
        def create_send_pending_mock(max_messages):
            sent_messages = 0

            async def _inner(*args: Any):
                nonlocal sent_messages
                if sent_messages < max_messages:
                    sent_messages += 1
                    return
                else:
                    await asyncio.Future()

            return _inner

        buf._send_pending = create_send_pending_mock(10)

        class WaitForEverEvent(asyncio.Event):
            test_stopped: bool = False

            def stop_test(self) -> None:
                self.test_stopped = True

            async def wait(self) -> None:
                while not self.test_stopped:
                    await asyncio.sleep(1.0)

        wait_for_event = buf.message_sent = WaitForEverEvent()
        await buf.start()

        try:
            original_size_property = buf.__class__.size
            buf.__class__.size = PropertyMock(return_value=10)
            waiting = buf.flush_atmost(10)
            loop = asyncio.get_event_loop()
            task = loop.create_task(waiting)
            await asyncio.sleep(0)
            assert (
                not task.done()
            ), "Task has completed even though not all events have been issued"
            buf.__class__.size = PropertyMock(return_value=0)
            await asyncio.sleep(0.2)
            assert task.done(), "Task has not been completed even though queue is None"
            assert isinstance(task.result(), int)
        finally:
            wait_for_event.stop_test()
            buf.__class__.size = original_size_property
            await buf.stop()


class ProducerTests:
    @pytest.mark.asyncio
    async def test_send(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send("topic", "key", "value", 1, None, {})

    def test_send_soon(self, *, producer):
        producer.buffer = Mock(name="buffer")
        fut = Mock(name="fut")
        producer.send_soon(fut)
        producer.buffer.put.assert_called_once_with(fut)

    @pytest.mark.asyncio
    async def test_flush(self, *, producer):
        await producer.flush()

    @pytest.mark.asyncio
    async def test_send_and_wait(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.send_and_wait("topic", "key", "value", 1, None, {})

    @pytest.mark.asyncio
    async def test_create_topic(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.create_topic("topic", 1, 1)

    def test_key_partition(self, *, producer):
        with pytest.raises(NotImplementedError):
            producer.key_partition("topic", "key")

    @pytest.mark.asyncio
    async def test_begin_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.begin_transaction("tid")

    @pytest.mark.asyncio
    async def test_commit_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.commit_transaction("tid")

    @pytest.mark.asyncio
    async def test_abort_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.abort_transaction("tid")

    @pytest.mark.asyncio
    async def test_stop_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.stop_transaction("tid")

    @pytest.mark.asyncio
    async def test_maybe_begin_transaction(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.maybe_begin_transaction("tid")

    @pytest.mark.asyncio
    async def test_commit_transactions(self, *, producer):
        with pytest.raises(NotImplementedError):
            await producer.commit_transactions({}, "gid")

    def test_supports_headers(self, *, producer):
        assert not producer.supports_headers()


class Test_Producer(ProducerTests):
    @pytest.fixture
    def producer(self, *, app):
        return Producer(app.transport)
