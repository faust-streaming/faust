import random

import pytest

from faust.transport.utils import (
    DefaultSchedulingStrategy,
    TopicBuffer,
    _py_records_iterator,
    _records_iterator,
)
from faust.types import TP

#: Both round-robin implementations.  ``_records_iterator`` is the Cython one
#: whenever the extension could be built, and is otherwise the same object as
#: ``_py_records_iterator``.
RECORDS_ITERATOR_IMPLS = [_py_records_iterator, _records_iterator]

TP1 = TP("foo", 0)
TP2 = TP("foo", 1)
TP3 = TP("bar", 0)
TP4 = TP("bar", 1)
TP5 = TP("baz", 3)


BUF1 = [0, 1, 2, 3, 4]
BUF2 = [5, 6, 7, 8]
BUF3 = [9, 10]
BUF4 = [11, 12, 13]
BUF5 = [14, 15]


class Test_TopicBuffer:
    def test_iter(self):
        buffer = TopicBuffer()
        buffer.add(TP1, BUF1)
        buffer.add(TP2, BUF2)
        buffer.add(TP3, BUF3)
        buffer.add(TP4, BUF4)
        buffer.add(TP5, BUF5)

        consumed = []
        for tp, item in buffer:
            consumed.append((tp, item))

        assert consumed == [
            (TP1, 0),
            (TP2, 5),
            (TP3, 9),
            (TP4, 11),
            (TP5, 14),
            (TP1, 1),
            (TP2, 6),
            (TP3, 10),
            (TP4, 12),
            (TP5, 15),
            (TP1, 2),
            (TP2, 7),
            (TP4, 13),
            (TP1, 3),
            (TP2, 8),
            (TP1, 4),
        ]

    def test_map_from_records(self):
        records = {TP1: BUF1, TP2: BUF2, TP3: BUF3}
        m = DefaultSchedulingStrategy.map_from_records(records)
        assert isinstance(m["foo"], TopicBuffer)
        buf1 = m["foo"]._buffers
        assert len(buf1) == 2
        assert list(buf1[TP1]) == BUF1
        assert list(buf1[TP2]) == BUF2

        assert isinstance(m["bar"], TopicBuffer)
        buf2 = m["bar"]._buffers
        assert list(buf2[TP3]) == BUF3

    def test_next(self):
        buffer = TopicBuffer()
        buffer.add(TP1, BUF1)

        consumed = []
        while True:
            try:
                consumed.append(next(buffer))
            except StopIteration:
                break
        assert consumed == [
            (TP1, 0),
            (TP1, 1),
            (TP1, 2),
            (TP1, 3),
            (TP1, 4),
        ]


class Test_records_iterator:
    def _index(self, records):
        return DefaultSchedulingStrategy.map_from_records(records)

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_round_robin_over_topics_and_partitions(self, impl):
        records = {TP1: BUF1, TP2: BUF2, TP3: BUF3, TP4: BUF4, TP5: BUF5}

        # Round-robin across topics, and across the partitions within each
        # topic.  "baz" only has one partition, so it drains early and the
        # interleaving shifts once it and "bar" are exhausted.
        assert list(impl(self._index(records))) == [
            (TP1, 0),
            (TP3, 9),
            (TP5, 14),
            (TP2, 5),
            (TP4, 11),
            (TP5, 15),
            (TP1, 1),
            (TP3, 10),
            (TP2, 6),
            (TP4, 12),
            (TP1, 2),
            (TP4, 13),
            (TP2, 7),
            (TP1, 3),
            (TP2, 8),
            (TP1, 4),
        ]

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_empty(self, impl):
        assert list(impl(self._index({}))) == []

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_empty_buffers(self, impl):
        assert list(impl(self._index({TP1: [], TP3: []}))) == []

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_drains_the_index_it_was_given(self, impl):
        index = self._index({TP1: BUF1, TP3: BUF3})
        list(impl(index))
        assert not index

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_propagates_exceptions(self, impl):
        def raising():
            yield (TP1, 1)
            raise RuntimeError("buffer exploded")

        with pytest.raises(RuntimeError):
            list(impl({"foo": raising()}))

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_topic_buffer_subclass(self, impl):
        # A custom TopicBuffer must still be driven through next(), so that
        # any overridden __iter__/__next__ is honoured.
        class MyBuffer(TopicBuffer):
            pass

        buffer = MyBuffer()
        buffer.add(TP1, BUF1)
        assert list(impl({"foo": buffer})) == [(TP1, i) for i in BUF1]

    def test_implementations_agree(self):
        # Randomised differential test: the accelerated iterator must emit
        # exactly the same sequence as the pure-Python one for any topology.
        rng = random.Random(20220613)
        for _ in range(200):
            records = {
                TP(f"t{topic}", partition): [
                    f"t{topic}-{partition}-{i}" for i in range(rng.randint(0, 6))
                ]
                for topic in range(rng.randint(0, 4))
                for partition in range(rng.randint(1, 4))
            }
            assert list(_records_iterator(self._index(records))) == list(
                _py_records_iterator(self._index(records))
            )

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_partition_added_mid_iteration(self, impl):
        # Both implementations re-read the buffer map on each pass.
        buffer = TopicBuffer()
        buffer.add(TP1, [1, 2])
        index = {"foo": buffer}

        it = impl(index)
        buffer.add(TP2, [7, 8])
        assert list(it) == [(TP1, 1), (TP2, 7), (TP1, 2), (TP2, 8)]

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_topic_added_mid_iteration(self, impl):
        buffer = TopicBuffer()
        buffer.add(TP1, [1, 2])
        index = {"foo": buffer}

        it = impl(index)
        late = TopicBuffer()
        late.add(TP3, [9])
        index["bar"] = late
        assert list(it) == [(TP1, 1), (TP3, 9), (TP1, 2)]

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_drains_the_buffer_map_too(self, impl):
        # Exhausted partitions are popped from TopicBuffer._buffers.
        buffer = TopicBuffer()
        buffer.add(TP1, [1, 2])
        index = {"foo": buffer}

        list(impl(index))
        assert not index
        assert not buffer._buffers

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_topic_buffer_replaced_mid_iteration(self, impl):
        # A TopicBuffer swapped in under an existing topic name keeps the
        # index the same size, so a length-only staleness check would miss
        # it and keep draining the old buffer -- silently dropping every
        # record in the replacement.
        first = TopicBuffer()
        first.add(TP1, [1, 2])
        index = {"foo": first}

        it = impl(index)
        assert next(it) == (TP1, 1)

        replacement = TopicBuffer()
        replacement.add(TP1, [10, 11])
        index["foo"] = replacement

        assert list(it) == [(TP1, 10), (TP1, 11)]

    @pytest.mark.parametrize("impl", RECORDS_ITERATOR_IMPLS)
    def test_partition_buffer_replaced_mid_iteration(self, impl):
        # Same hazard one level down: an iterator swapped in under an
        # existing TP leaves the partition cursor stale.
        buffer = TopicBuffer()
        buffer.add(TP1, [1, 2, 3])

        it = impl({"foo": buffer})
        assert next(it) == (TP1, 1)

        buffer._buffers[TP1] = iter([10, 11])
        assert list(it) == [(TP1, 10), (TP1, 11)]

    # Note: swapping a topic for one under a *different* name mid-iteration
    # is deliberately not covered.  The pure-Python generator iterates the
    # live dict, so CPython raises "dictionary keys changed during
    # iteration" -- that is undefined behaviour in Python itself, not a
    # guarantee either implementation should be pinned to.
