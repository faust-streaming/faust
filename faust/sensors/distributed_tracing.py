import traceback
from functools import cached_property
from typing import Any, Dict

import aiohttp
import opentracing
from mode import get_logger
from mode.utils.compat import want_str
from opentracing import Format
from opentracing.ext import tags

from faust import App, EventT, Sensor, StreamT
from faust.types import TP, Message, PendingMessage, ProducerT, RecordMetadata
from faust.types.core import OpenHeadersArg, merge_headers
from faust.utils.tracing import current_span, set_current_span

logger = get_logger(__name__)


class TracingSensor(Sensor):
    aiohttp_sessions: Dict[str, aiohttp.ClientSession] = None

    @cached_property
    def app_tracer(self) -> opentracing.Tracer:
        return opentracing.tracer

    async def stop(self) -> None:
        if self.aiohttp_sessions:
            for session in self.aiohttp_sessions.values():
                await session.close()
        await super().stop()

    @cached_property
    def kafka_tracer(self) -> opentracing.Tracer:
        return opentracing.tracer

    # Message received by a consumer.
    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        carrier_headers = {want_str(k): want_str(v) for k, v in message.headers}

        if carrier_headers:
            parent_context = self.app_tracer.extract(
                format=Format.TEXT_MAP, carrier=carrier_headers
            )
            span = self.app_tracer.start_span(
                operation_name=f"consume-from-{message.topic}",
                references=opentracing.follows_from(parent_context),
            )
        else:
            span = self.app_tracer.start_span(
                operation_name=f"consume-from-{message.topic}"
            )
        set_current_span(span)
        span.set_tag("kafka-topic", tp.topic)
        span.set_tag("kafka-partition", tp.partition)
        span.set_tag("kafka-key", message.key)
        span.__enter__()
        message.span = span  # type: ignore

    # Message sent to a stream as an event.
    def on_stream_event_in(
        self, tp: TP, offset: int, stream: StreamT, event: EventT
    ) -> None:
        stream_meta = getattr(event.message, "stream_meta", None)
        if stream_meta is None:
            stream_meta = event.message.stream_meta = {}  # type: ignore
        parent_span = event.message.span  # type: ignore
        if parent_span:
            stream_span = opentracing.start_child_span(
                parent_span, f"job-{event.message.topic}"
            )
            stream_span.set_tag("stream-concurrency-index", stream.concurrency_index)
            stream_span.set_tag("stream-prefix", stream.prefix)
            spans = stream_meta.get("stream_spans")
            if spans is None:
                spans = stream_meta["stream_spans"] = {}
            spans[stream] = stream_span
            stream_span.__enter__()

    # Event was acknowledged by stream.
    def on_stream_event_out(
        self, tp: TP, offset: int, stream: StreamT, event: EventT, state: Dict = None
    ) -> None:
        stream_meta = getattr(event.message, "stream_meta", None)
        if stream_meta is None:
            stream_meta = event.message.stream_meta = {}  # type: ignore
        stream_spans = stream_meta.get("stream_spans")
        if stream_spans:
            span = stream_spans.pop(stream, None)
            if span is not None:
                span.finish()

    # All streams finished processing message.
    def on_message_out(self, tp: TP, offset: int, message: Message) -> None:
        span = message.span  # type: ignore
        if span:
            span.finish()

    # About to send a message.
    def on_send_initiated(
        self,
        producer: ProducerT,
        topic: str,
        message: PendingMessage,
        keysize: int,
        valsize: int,
    ) -> Any:
        parent_span = current_span()
        if parent_span:
            span = opentracing.start_child_span(parent_span, f"produce-to-{topic}")
            header_map = dict(message.headers) if message.headers else {}
            span.set_tag("kafka-headers", header_map)
            self.trace_inject_headers(span, message.headers)
            span.__enter__()
            return {"span": span}
        return {"span": None}

    # Message successfully sent.
    def on_send_completed(
        self, producer: ProducerT, state: Any, metadata: RecordMetadata
    ) -> None:
        span = state.get("span")
        if span is not None:
            span.set_tag("kafka-topic", metadata.topic)
            span.set_tag("kafka-partition", metadata.partition)
            span.set_tag("kafka-offset", metadata.offset)
            span.finish()

    # Error while sending message.
    def on_send_error(
        self, producer: ProducerT, exc: BaseException, state: Any
    ) -> None:
        span = state.get("span")
        if span is not None:
            span.set_tag(tags.ERROR, "true")
            span.log_kv(
                {
                    "python.exception.type": type(exc),
                    "python.exception.val": exc,
                    "python.exception.tb": traceback.format_stack(),
                }
            )
            span.finish(exception=exc)

    def trace_inject_headers(
        self, span: opentracing.Span, headers: OpenHeadersArg
    ) -> Any:
        try:
            if self.app_tracer is not None:
                if span is not None:
                    carrier: Dict = {}
                    self.app_tracer.inject(
                        span_context=span.context,
                        format=opentracing.Format.TEXT_MAP,
                        carrier=carrier,
                    )
                    merge_headers(headers, carrier)
            return headers
        except Exception as ex:
            logger.warning(f"Exception in trace_inject_headers {ex} ")
            return None

    def on_threaded_producer_buffer_processed(self, app: App, size: int) -> None:
        pass
