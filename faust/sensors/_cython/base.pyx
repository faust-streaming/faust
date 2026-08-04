# cython: language_level=3
"""Cython optimized sensor delegation."""


cdef class SensorDelegateBase:
    """Per-message half of :class:`faust.sensors.base.SensorDelegate`.

    Only the four hooks that run on every message live here; the rest stay
    in the Python subclass, which keeps them readable and overridable.

    The sensor set is read live, exactly as the pure-Python implementation
    does, so mutating ``_sensors`` directly behaves the same either way.
    """

    cdef public object app
    cdef public set _sensors

    def __init__(self, object app):
        self.app = app
        self._sensors = set()

    def add(self, object sensor):
        """Add sensor."""
        # connect beacons
        sensor.beacon = self.app.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove(self, object sensor):
        """Remove sensor."""
        self._sensors.remove(sensor)

    def __iter__(self):
        return iter(self._sensors)

    def on_message_in(self, object tp, object offset, object message):
        """Call before message is delegated to streams."""
        cdef object sensor
        for sensor in self._sensors:
            sensor.on_message_in(tp, offset, message)

    def on_stream_event_in(self, object tp, object offset, object stream,
                           object event):
        """Call when stream starts processing an event."""
        cdef:
            dict states = {}
            object sensor
        for sensor in self._sensors:
            states[sensor] = sensor.on_stream_event_in(tp, offset, stream, event)
        return states

    def on_stream_event_out(self, object tp, object offset, object stream,
                            object event, object state=None):
        """Call when stream is done processing an event."""
        cdef object sensor
        if state:
            for sensor in self._sensors:
                sensor.on_stream_event_out(
                    tp, offset, stream, event, state.get(sensor))
        else:
            for sensor in self._sensors:
                sensor.on_stream_event_out(tp, offset, stream, event, None)

    def on_message_out(self, object tp, object offset, object message):
        """Call when message is fully acknowledged and can be committed."""
        cdef object sensor
        for sensor in self._sensors:
            sensor.on_message_out(tp, offset, message)
