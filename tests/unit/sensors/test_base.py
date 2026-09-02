from unittest.mock import Mock

import pytest

from faust import Event, Stream, Table, Topic, web
from faust.assignor import PartitionAssignor
from faust.sensors import Sensor
from faust.sensors.base import _PySensorDelegateBase, _SensorDelegateBase
from faust.transport.consumer import Consumer
from faust.transport.producer import Producer
from faust.types import TP, Message

TP1 = TP("foo", 0)


@pytest.fixture
def message():
    return Mock(name="message", autospec=Message)


@pytest.fixture
def stream():
    return Mock(name="stream", autospec=Stream)


@pytest.fixture
def event():
    return Mock(name="event", autospec=Event)


@pytest.fixture
def topic():
    return Mock(name="topic", autospec=Topic)


@pytest.fixture
def table():
    return Mock(name="table", autospec=Table)


@pytest.fixture
def consumer():
    return Mock(name="consumer", autospec=Consumer)


@pytest.fixture
def producer():
    return Mock(name="producer", autospec=Producer)


@pytest.fixture
def assignor():
    return Mock(name="assignor", autospec=PartitionAssignor)


@pytest.fixture
def view():
    return Mock(name="view", autospec=web.View)


@pytest.fixture
def req():
    return Mock(name="request", autospec=web.Request)


@pytest.fixture
def response():
    return Mock(name="response", autospec=web.Response)


class TestSensor:
    @pytest.fixture
    def sensor(self, *, app):
        return Sensor()

    def test_on_message_in(self, *, sensor, message):
        sensor.on_message_in(TP1, 3, message)

    def test_on_stream_event_in(self, *, sensor, stream, event):
        sensor.on_stream_event_in(TP1, 3, stream, event)

    def test_on_stream_event_out(self, *, sensor, stream, event):
        state = sensor.on_stream_event_in(TP1, 3, stream, event)
        sensor.on_stream_event_out(TP1, 3, stream, event, state)
        sensor.on_stream_event_out(TP1, 3, stream, event, None)

    def test_on_message_out(self, *, sensor, message):
        sensor.on_message_out(TP1, 3, message)

    def test_on_topic_buffer_full(self, *, sensor):
        sensor.on_topic_buffer_full(TP1)

    def test_on_table_get(self, *, sensor, table):
        sensor.on_table_get(table, "key")

    def test_on_table_set(self, *, sensor, table):
        sensor.on_table_set(table, "key", "value")

    def test_on_table_del(self, *, sensor, table):
        sensor.on_table_del(table, "key")

    def test_on_commit_initiated(self, *, sensor, consumer):
        sensor.on_commit_initiated(consumer)

    def test_on_commit_completed(self, *, sensor, consumer):
        sensor.on_commit_completed(consumer, Mock(name="state"))

    def test_on_send_initiated(self, *, sensor, producer):
        sensor.on_send_initiated(producer, "topic", "message", 30, 40)

    def test_on_send_completed(self, *, sensor, producer):
        sensor.on_send_completed(producer, Mock(name="state"), Mock(name="metadata"))

    def test_on_assignment(self, *, sensor, assignor):
        state = sensor.on_assignment_start(assignor)
        assert state["time_start"]
        sensor.on_assignment_error(assignor, state, KeyError())
        sensor.on_assignment_completed(assignor, state)

    def test_on_rebalance(self, *, sensor, app):
        state = sensor.on_rebalance_start(app)
        assert state["time_start"]
        sensor.on_rebalance_return(app, state)
        sensor.on_rebalance_end(app, state)

    def test_on_web_request(self, *, sensor, app, req, response, view):
        state = sensor.on_web_request_start(app, req, view=view)

        assert state["time_start"]

        sensor.on_web_request_end(app, req, response, state, view=view)

    def test_on_send_error(self, *, sensor, producer):
        sensor.on_send_error(producer, KeyError("foo"), Mock(name="state"))

    def test_asdict(self, *, sensor):
        assert sensor.asdict() == {}


class TestSensorDelegate:
    @pytest.fixture
    def sensor(self):
        return Mock(name="sensor", autospec=Sensor)

    @pytest.fixture
    def sensors(self, *, app, sensor):
        sensors = app.sensors
        sensors.add(sensor)
        return sensors

    def test_remove(self, *, sensors, sensor):
        assert list(iter(sensors))
        sensors.remove(sensor)
        assert not list(iter(sensors))

    def test_on_message_in(self, *, sensors, sensor, message):
        sensors.on_message_in(TP1, 303, message)
        sensor.on_message_in.assert_called_once_with(TP1, 303, message)

    def test_on_stream_event_in_out(self, *, sensors, sensor, stream, event):
        state = sensors.on_stream_event_in(TP1, 303, stream, event)
        sensor.on_stream_event_in.assert_called_once_with(TP1, 303, stream, event)
        sensors.on_stream_event_out(TP1, 303, stream, event, state)
        sensor.on_stream_event_out.assert_called_once_with(
            TP1, 303, stream, event, state[sensor]
        )

    def test_on_topic_buffer_full(self, *, sensors, sensor):
        sensors.on_topic_buffer_full(TP1)
        sensor.on_topic_buffer_full.assert_called_once_with(TP1)

    def test_on_message_out(self, *, sensors, sensor, message):
        sensors.on_message_out(TP1, 303, message)
        sensor.on_message_out.assert_called_once_with(TP1, 303, message)

    def test_on_table_get(self, *, sensors, sensor, table):
        sensors.on_table_get(table, "key")
        sensor.on_table_get.assert_called_once_with(table, "key")

    def test_on_table_set(self, *, sensors, sensor, table):
        sensors.on_table_set(table, "key", "value")
        sensor.on_table_set.assert_called_once_with(table, "key", "value")

    def test_on_table_del(self, *, sensors, sensor, table):
        sensors.on_table_del(table, "key")
        sensor.on_table_del.assert_called_once_with(table, "key")

    def test_on_commit(self, *, sensors, sensor, consumer):
        state = sensors.on_commit_initiated(consumer)
        sensor.on_commit_initiated.assert_called_once_with(consumer)

        sensors.on_commit_completed(consumer, state)
        sensor.on_commit_completed.assert_called_once_with(consumer, state[sensor])

    def test_on_send(self, *, sensors, sensor, producer):
        metadata = Mock(name="metadata")
        state = sensors.on_send_initiated(producer, "topic", "message", 303, 606)
        sensor.on_send_initiated.assert_called_once_with(
            producer, "topic", "message", 303, 606
        )

        sensors.on_send_completed(producer, state, metadata)
        sensor.on_send_completed.assert_called_once_with(
            producer, state[sensor], metadata
        )

        exc = KeyError("foo")
        sensors.on_send_error(producer, exc, state)
        sensor.on_send_error.assert_called_once_with(producer, exc, state[sensor])

    def test_on_assignment(self, *, sensors, sensor, assignor):
        state = sensors.on_assignment_start(assignor)
        sensor.on_assignment_start.assert_called_once_with(assignor)

        sensors.on_assignment_completed(assignor, state)
        sensor.on_assignment_completed.assert_called_once_with(assignor, state[sensor])

        exc = KeyError("bar")
        sensors.on_assignment_error(assignor, state, exc)
        sensor.on_assignment_error.assert_called_once_with(assignor, state[sensor], exc)

    def test_on_rebalance(self, *, sensors, sensor, app):
        state = sensors.on_rebalance_start(app)
        sensor.on_rebalance_start.assert_called_once_with(app)

        sensors.on_rebalance_return(app, state)
        sensor.on_rebalance_return.assert_called_once_with(app, state[sensor])

        sensors.on_rebalance_end(app, state)
        sensor.on_rebalance_end.assert_called_once_with(app, state[sensor])

    def test_on_web_request(self, *, sensors, sensor, app, req, response, view):
        state = sensors.on_web_request_start(app, req, view=view)
        sensor.on_web_request_start.assert_called_once_with(app, req, view=view)

        sensors.on_web_request_end(app, req, response, state, view=view)
        sensor.on_web_request_end.assert_called_once_with(
            app, req, response, state[sensor], view=view
        )

    def test_repr(self, *, sensors):
        assert repr(sensors)


#: Both implementations of the per-message sensor fan-out.
#: ``_SensorDelegateBase`` is the Cython one whenever the extension could be
#: built, and is otherwise the same object as ``_PySensorDelegateBase``.
SENSOR_DELEGATE_BASES = [_PySensorDelegateBase, _SensorDelegateBase]


class Test_SensorDelegateBase:
    """The four hooks that run on every message, in both implementations."""

    def _delegate(self, base, n_sensors=1):
        app = Mock(name="app")
        delegate = base(app)
        sensors = []
        for _ in range(n_sensors):
            sensor = Mock(name="sensor", autospec=Sensor)
            delegate.add(sensor)
            sensors.append(sensor)
        return delegate, sensors

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    def test_add_connects_beacon(self, base):
        delegate, [sensor] = self._delegate(base)
        assert sensor.beacon is delegate.app.beacon.new.return_value
        assert list(delegate) == [sensor]

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    def test_remove(self, base):
        delegate, [sensor] = self._delegate(base)
        delegate.remove(sensor)
        assert not list(delegate)
        delegate.on_message_in(TP1, 3, None)
        sensor.on_message_in.assert_not_called()

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    def test_remove__missing_raises(self, base):
        delegate, _ = self._delegate(base)
        with pytest.raises(KeyError):
            delegate.remove(Mock(name="never-added"))

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    def test_no_sensors(self, base):
        delegate = base(Mock(name="app"))
        delegate.on_message_in(TP1, 3, None)
        assert delegate.on_stream_event_in(TP1, 3, None, None) == {}
        delegate.on_stream_event_out(TP1, 3, None, None, None)
        delegate.on_message_out(TP1, 3, None)

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    @pytest.mark.parametrize("n_sensors", [1, 3])
    def test_on_message_in_out(self, base, n_sensors, message):
        delegate, sensors = self._delegate(base, n_sensors)
        delegate.on_message_in(TP1, 303, message)
        delegate.on_message_out(TP1, 303, message)
        for sensor in sensors:
            sensor.on_message_in.assert_called_once_with(TP1, 303, message)
            sensor.on_message_out.assert_called_once_with(TP1, 303, message)

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    @pytest.mark.parametrize("n_sensors", [1, 3])
    def test_on_stream_event_in_out(self, base, n_sensors, stream, event):
        delegate, sensors = self._delegate(base, n_sensors)
        state = delegate.on_stream_event_in(TP1, 303, stream, event)
        assert set(state) == set(sensors)
        delegate.on_stream_event_out(TP1, 303, stream, event, state)
        for sensor in sensors:
            sensor.on_stream_event_in.assert_called_once_with(TP1, 303, stream, event)
            sensor.on_stream_event_out.assert_called_once_with(
                TP1, 303, stream, event, state[sensor]
            )

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    @pytest.mark.parametrize("state", [None, {}])
    def test_on_stream_event_out__without_state(self, base, state, stream, event):
        # No state recorded for this sensor: it must still be called, with None.
        delegate, [sensor] = self._delegate(base)
        delegate.on_stream_event_out(TP1, 303, stream, event, state)
        sensor.on_stream_event_out.assert_called_once_with(
            TP1, 303, stream, event, None
        )

    @pytest.mark.parametrize("base", SENSOR_DELEGATE_BASES)
    def test_direct_mutation_of_sensors_is_picked_up(self, base):
        # The Cython version walks a list snapshot of the sensor set, so it
        # has to notice a set that was mutated behind its back.
        delegate, _ = self._delegate(base)
        extra = Mock(name="extra", autospec=Sensor)
        delegate._sensors.add(extra)
        delegate.on_message_in(TP1, 3, None)
        extra.on_message_in.assert_called_once_with(TP1, 3, None)
