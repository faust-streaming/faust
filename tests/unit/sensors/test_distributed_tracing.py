from unittest.mock import MagicMock, Mock

import opentracing
import pytest

from faust.sensors.distributed_tracing import TracingSensor
from faust.types import TP

TP1 = TP("topic", 3)


@pytest.fixture()
def tracer(*, monkeypatch):
    # ``app_tracer``/``kafka_tracer`` both return the module-global
    # ``opentracing.tracer``; swap it for a mock so we can assert on the
    # spans the sensor creates.
    tracer = MagicMock(name="tracer")
    monkeypatch.setattr("opentracing.tracer", tracer)
    return tracer


@pytest.fixture()
def sensor():
    return TracingSensor()


def _message(*, headers=None, topic="topic", key=b"key"):
    message = Mock(name="message")
    message.headers = headers if headers is not None else []
    message.topic = topic
    message.key = key
    message.span = None
    # ``stream_meta`` is only set on the message by the sensor; start absent
    # so the getattr(..., None) branch is exercised.
    del message.stream_meta
    return message


class Test_TracingSensor:
    def test_app_and_kafka_tracer(self, *, sensor, tracer):
        assert sensor.app_tracer is tracer
        assert sensor.kafka_tracer is tracer

    @pytest.mark.asyncio
    async def test_stop__no_sessions(self, *, sensor):
        # aiohttp_sessions defaults to None -> just delegates to super().stop().
        assert sensor.aiohttp_sessions is None
        await sensor.stop()

    @pytest.mark.asyncio
    async def test_stop__closes_sessions(self, *, sensor):
        session = Mock(name="session")
        session.close = Mock(return_value=_awaitable())
        sensor.aiohttp_sessions = {"a": session}
        await sensor.stop()
        session.close.assert_called_once_with()

    def test_on_message_in__no_headers(self, *, sensor, tracer, monkeypatch):
        set_current_span = Mock(name="set_current_span")
        monkeypatch.setattr(
            "faust.sensors.distributed_tracing.set_current_span", set_current_span
        )
        span = tracer.start_span.return_value
        message = _message(headers=[])

        sensor.on_message_in(TP1, 0, message)

        tracer.extract.assert_not_called()
        tracer.start_span.assert_called_once_with(operation_name="consume-from-topic")
        set_current_span.assert_called_once_with(span)
        span.set_tag.assert_any_call("kafka-topic", TP1.topic)
        span.set_tag.assert_any_call("kafka-partition", TP1.partition)
        span.set_tag.assert_any_call("kafka-key", message.key)
        span.__enter__.assert_called_once_with()
        assert message.span is span

    def test_on_message_in__with_headers(self, *, sensor, tracer, monkeypatch):
        monkeypatch.setattr(
            "faust.sensors.distributed_tracing.set_current_span", Mock()
        )
        follows_from = Mock(name="follows_from")
        monkeypatch.setattr("opentracing.follows_from", follows_from)
        message = _message(headers=[(b"uber-trace-id", b"123")])

        sensor.on_message_in(TP1, 0, message)

        tracer.extract.assert_called_once()
        follows_from.assert_called_once_with(tracer.extract.return_value)
        tracer.start_span.assert_called_once_with(
            operation_name="consume-from-topic",
            references=follows_from.return_value,
        )

    def test_on_stream_event_in__no_parent_span(self, *, sensor, monkeypatch):
        start_child = Mock(name="start_child_span")
        monkeypatch.setattr("opentracing.start_child_span", start_child)
        message = _message()
        message.span = None
        event = Mock(name="event", message=message)

        sensor.on_stream_event_in(TP1, 0, Mock(name="stream"), event)

        start_child.assert_not_called()
        # stream_meta is created even when there is no parent span.
        assert message.stream_meta == {}

    def test_on_stream_event_in__with_parent_span(self, *, sensor, monkeypatch):
        stream_span = MagicMock(name="stream_span")
        start_child = Mock(name="start_child_span", return_value=stream_span)
        monkeypatch.setattr("opentracing.start_child_span", start_child)
        parent_span = Mock(name="parent_span")
        message = _message()
        message.span = parent_span
        event = Mock(name="event", message=message)
        stream = Mock(name="stream", concurrency_index=2, prefix="p")

        sensor.on_stream_event_in(TP1, 0, stream, event)

        start_child.assert_called_once_with(parent_span, "job-topic")
        stream_span.set_tag.assert_any_call("stream-concurrency-index", 2)
        stream_span.set_tag.assert_any_call("stream-prefix", "p")
        stream_span.__enter__.assert_called_once_with()
        assert message.stream_meta["stream_spans"][stream] is stream_span

    def test_on_stream_event_in__reuses_existing_meta(self, *, sensor, monkeypatch):
        # stream_meta and its stream_spans dict already exist -> the sensor
        # must reuse them rather than recreate (covers the not-None branches).
        stream_span = MagicMock(name="stream_span")
        monkeypatch.setattr(
            "opentracing.start_child_span",
            Mock(return_value=stream_span),
        )
        other_stream, other_span = Mock(name="other"), Mock(name="other_span")
        message = _message()
        message.span = Mock(name="parent_span")
        message.stream_meta = {"stream_spans": {other_stream: other_span}}
        event = Mock(name="event", message=message)
        stream = Mock(name="stream", concurrency_index=0, prefix="p")

        sensor.on_stream_event_in(TP1, 0, stream, event)

        spans = message.stream_meta["stream_spans"]
        assert spans[other_stream] is other_span  # untouched
        assert spans[stream] is stream_span  # added to the same dict

    def test_on_stream_event_out__finishes_span(self, *, sensor):
        span = Mock(name="span")
        stream = Mock(name="stream")
        message = _message()
        message.stream_meta = {"stream_spans": {stream: span}}
        event = Mock(name="event", message=message)

        sensor.on_stream_event_out(TP1, 0, stream, event)

        span.finish.assert_called_once_with()
        assert stream not in message.stream_meta["stream_spans"]

    def test_on_stream_event_out__no_spans(self, *, sensor):
        message = _message()
        event = Mock(name="event", message=message)
        # No stream_meta -> created empty, nothing to finish (no error).
        sensor.on_stream_event_out(TP1, 0, Mock(name="stream"), event)
        assert message.stream_meta == {}

    def test_on_stream_event_out__stream_not_present(self, *, sensor):
        message = _message()
        message.stream_meta = {"stream_spans": {Mock(name="other"): Mock()}}
        event = Mock(name="event", message=message)
        # pop returns None -> no finish, no crash.
        sensor.on_stream_event_out(TP1, 0, Mock(name="stream"), event)

    def test_on_message_out__with_span(self, *, sensor):
        message = _message()
        message.span = Mock(name="span")
        sensor.on_message_out(TP1, 0, message)
        message.span.finish.assert_called_once_with()

    def test_on_message_out__no_span(self, *, sensor):
        message = _message()
        message.span = None
        sensor.on_message_out(TP1, 0, message)  # no crash

    def test_on_send_initiated__with_parent(self, *, sensor, monkeypatch):
        span = MagicMock(name="span")
        start_child = Mock(name="start_child_span", return_value=span)
        monkeypatch.setattr("opentracing.start_child_span", start_child)
        parent = Mock(name="parent_span")
        monkeypatch.setattr(
            "faust.sensors.distributed_tracing.current_span",
            Mock(return_value=parent),
        )
        inject = Mock(name="trace_inject_headers")
        sensor.trace_inject_headers = inject
        message = Mock(name="pending", headers={"h": "1"})

        ret = sensor.on_send_initiated(Mock(), "topic", message, 1, 2)

        start_child.assert_called_once_with(parent, "produce-to-topic")
        span.set_tag.assert_called_once_with("kafka-headers", {"h": "1"})
        inject.assert_called_once_with(span, message.headers)
        span.__enter__.assert_called_once_with()
        assert ret == {"span": span}

    def test_on_send_initiated__no_parent(self, *, sensor, monkeypatch):
        monkeypatch.setattr(
            "faust.sensors.distributed_tracing.current_span",
            Mock(return_value=None),
        )
        message = Mock(name="pending", headers=None)
        ret = sensor.on_send_initiated(Mock(), "topic", message, 1, 2)
        assert ret == {"span": None}

    def test_on_send_completed__with_span(self, *, sensor):
        span = Mock(name="span")
        metadata = Mock(topic="t", partition=1, offset=42)
        sensor.on_send_completed(Mock(), {"span": span}, metadata)
        span.set_tag.assert_any_call("kafka-topic", "t")
        span.set_tag.assert_any_call("kafka-partition", 1)
        span.set_tag.assert_any_call("kafka-offset", 42)
        span.finish.assert_called_once_with()

    def test_on_send_completed__no_span(self, *, sensor):
        sensor.on_send_completed(Mock(), {"span": None}, Mock())  # no crash

    def test_on_send_error__with_span(self, *, sensor):
        span = Mock(name="span")
        exc = ValueError("boom")
        sensor.on_send_error(Mock(), exc, {"span": span})
        span.set_tag.assert_called_once_with(opentracing.ext.tags.ERROR, "true")
        span.log_kv.assert_called_once()
        span.finish.assert_called_once_with(exception=exc)

    def test_on_send_error__no_span(self, *, sensor):
        sensor.on_send_error(Mock(), ValueError(), {"span": None})  # no crash

    def test_trace_inject_headers__success(self, *, sensor, tracer, monkeypatch):
        merge_headers = Mock(name="merge_headers")
        monkeypatch.setattr(
            "faust.sensors.distributed_tracing.merge_headers", merge_headers
        )
        span = Mock(name="span")
        headers = {"a": "1"}

        ret = sensor.trace_inject_headers(span, headers)

        tracer.inject.assert_called_once()
        merge_headers.assert_called_once()
        assert ret is headers

    def test_trace_inject_headers__no_tracer(self, *, sensor, monkeypatch):
        # app_tracer is None -> inject skipped, headers returned unchanged.
        sensor.__dict__["app_tracer"] = None
        headers = {"a": "1"}
        assert sensor.trace_inject_headers(Mock(), headers) is headers

    def test_trace_inject_headers__no_span(self, *, sensor, tracer):
        headers = {"a": "1"}
        assert sensor.trace_inject_headers(None, headers) is headers
        tracer.inject.assert_not_called()

    def test_trace_inject_headers__exception_returns_none(self, *, sensor, tracer):
        tracer.inject.side_effect = RuntimeError("nope")
        assert sensor.trace_inject_headers(Mock(name="span"), {"a": "1"}) is None

    def test_on_threaded_producer_buffer_processed(self, *, sensor):
        assert sensor.on_threaded_producer_buffer_processed(Mock(), 10) is None


def _awaitable(result=None):
    async def _coro():
        return result

    return _coro()
