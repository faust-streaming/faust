# cython: language_level=3
from asyncio import ALL_COMPLETED, ensure_future, wait

from faust.exceptions import KeyDecodeError, ValueDecodeError


cdef class ConductorHandler:

    cdef public:
        object conductor
        object tp
        object channels
        object app
        object topic
        object partition
        object on_topic_buffer_full
        object acquire_flow_control
        object consumer
        object wait_until_producer_ebb
        object consumer_on_buffer_full
        object consumer_on_buffer_drop


    def __init__(self, object conductor, object tp, object channels):
        self.conductor = conductor
        self.tp = tp
        self.channels = channels

        self.app = self.conductor.app
        self.topic, self.partition = self.tp
        self.on_topic_buffer_full = self.app.sensors.on_topic_buffer_full
        self.acquire_flow_control = self.app.flow_control.acquire
        self.wait_until_producer_ebb = self.app.producer.buffer.wait_until_ebb
        self.consumer = self.app.consumer
        # We divide `stream_buffer_maxsize` with Queue.pressure_ratio
        # find a limit to the number of messages we will buffer
        # before considering the buffer to be under high pressure.
        # When the buffer is under high pressure, we call
        # Consumer.on_buffer_full(tp) to remove this topic partition
        # from the fetcher.
        # We still accept anything that's currently in the fetcher (it's
        # already in memory so we are just moving the data) without blocking,
        # but signal the fetcher to stop retrieving any more data for this
        # partition.
        self.consumer_on_buffer_full = self.app.consumer.on_buffer_full

        # when the buffer drops down to half we re-enable fetching
        # from the partition.
        self.consumer_on_buffer_drop = self.app.consumer.on_buffer_drop

    async def __call__(self, object message):
        cdef:
            Py_ssize_t channels_n
            object channels
            object event
            object keyid
            object dest_event
        await self.acquire_flow_control()
        await self.wait_until_producer_ebb()
        channels = self.channels
        channels_n = len(channels)
        if channels_n:
            message.refcount += channels_n  # message.incref(n)
            event = None
            event_keyid = None

            delivered = set()
            full = []
            try:
                for chan in channels:
                    event, event_keyid = self._decode(event, chan, event_keyid)
                    if event is None:
                        event = await chan.decode(message, propagate=True)
                    if not self._put(event, chan, full):
                        continue
                    delivered.add(chan)
                if full:
                    await wait([ensure_future(self._handle_full(event, chan, delivered))
                                        for event, chan in full],
                                        return_when=ALL_COMPLETED)
            except KeyDecodeError as exc:
                remaining = channels - delivered
                message.ack(self.consumer, n=len(remaining))
                for channel in remaining:
                    await channel.on_key_decode_error(exc, message)
                    delivered.add(channel)
            except ValueDecodeError as exc:
                remaining = channels - delivered
                message.ack(self.consumer, n=len(remaining))
                for channel in remaining:
                    await channel.on_value_decode_error(exc, message)
                    delivered.add(channel)

    async def _handle_full(self, event, chan, delivered):
        self.on_topic_buffer_full(chan)
        await chan.put(event)
        delivered.add(chan)

    # callback called when the queue is under high pressure/
    # about to become full.
    def on_pressure_high(self) -> None:
        self.on_topic_buffer_full(self.tp)
        self.consumer_on_buffer_full(self.tp)

    # callback used when pressure drops.
    # added to Queue._pending_pressure_drop_callbacks
    # when the buffer is under high pressure/full.
    def on_pressure_drop(self) -> None:
        self.consumer_on_buffer_drop(self.tp)

    cdef object _decode(self, object event, object channel, object event_keyid):
        keyid = channel.key_type, channel.value_type
        if event_keyid is None or event is None:
            return None, event_keyid
        if keyid == event_keyid:
            return event, keyid

    cdef bint _put(self,
                   object event,
                   object channel,
                   object full):
        cdef:
            object queue
        queue = channel.queue
        if queue.full():
            full.append((event, channel))
            return False
        else:
            queue.put_nowait_enhanced(
                                value=event,
                                on_pressure_high=self.on_pressure_high,
                                on_pressure_drop=self.on_pressure_drop,
                            )
            return True
