# cython: language_level=3
"""Cython optimized consumer record scheduler."""
from cpython.list cimport PyList_GET_ITEM, PyList_GET_SIZE


cdef object _SENTINEL = object()

# Resolved lazily: faust.transport.utils imports this module at the bottom of
# its own body, so importing it back at module scope here would be circular.
cdef object _TOPIC_BUFFER = None


cdef inline object _topic_buffer_type():
    global _TOPIC_BUFFER
    if _TOPIC_BUFFER is None:
        from faust.transport.utils import TopicBuffer
        _TOPIC_BUFFER = TopicBuffer
    return _TOPIC_BUFFER


cdef class _TopicCursor:
    """Round-robin cursor over the partition buffers of a single topic.

    A C-level rewrite of :meth:`faust.transport.utils.TopicBuffer.__iter__`:
    one record is taken from each partition in turn, and partitions that run
    dry are popped from the buffer map at the end of the current pass.

    Anything that is not a plain, untouched ``TopicBuffer`` is driven through
    ``next()`` instead, so a subclass that overrides ``__iter__``/``__next__``
    keeps working.
    """

    cdef:
        object source     # the TopicBuffer, kept so we can detect replacement
        object buffers    # TopicBuffer._buffers, read live
        object compat     # set instead of `buffers` for non-TopicBuffer entries
        set to_remove
        list tps
        list iters
        Py_ssize_t pi
        Py_ssize_t n

    def __cinit__(self, object buffer):
        self.source = buffer
        self.buffers = None
        self.compat = None
        self.to_remove = set()
        self.tps = []
        self.iters = []
        self.pi = 0
        self.n = 0
        if type(buffer) is _topic_buffer_type() and (<object>buffer)._it is None:
            # Untouched TopicBuffer: drive its partition iterators directly,
            # which takes its generator out of the per-record path.
            self.buffers = (<object>buffer)._buffers
        else:
            self.compat = buffer

    cdef object next(self):
        """Return the next ``(tp, record)`` pair, or the sentinel if drained."""
        cdef:
            Py_ssize_t i
            object item

        if self.compat is not None:
            return next(self.compat, _SENTINEL)

        while True:
            if self.pi >= self.n:
                # `while buffers:` in the pure-Python generator.
                if not self.buffers:
                    return _SENTINEL
                self._start_pass()
                if self.n == 0:
                    return _SENTINEL
            i = self.pi
            self.pi += 1
            item = next(<object>PyList_GET_ITEM(self.iters, i), _SENTINEL)
            if item is _SENTINEL:
                self.to_remove.add(<object>PyList_GET_ITEM(self.tps, i))
                continue
            return (<object>PyList_GET_ITEM(self.tps, i), item)

    cdef _start_pass(self):
        cdef:
            object tp
            object it

        if self.to_remove:
            for tp in self.to_remove:
                self.buffers.pop(tp, None)
            self.to_remove.clear()
        elif PyList_GET_SIZE(self.tps) == len(self.buffers):
            # Nothing drained and nothing added since the last pass, so the
            # snapshot is still accurate.  TopicBuffer.add() asserts the
            # partition is new, so the size can only change on a real change.
            self.pi = 0
            return
        self.tps = []
        self.iters = []
        for tp, it in self.buffers.items():
            self.tps.append(tp)
            self.iters.append(it)
        self.n = PyList_GET_SIZE(self.tps)
        self.pi = 0


cdef class RoundRobinRecordIterator:
    """Iterate a topic index map in round-robin order.

    A C-level rewrite of the generator returned by
    :meth:`faust.transport.utils.DefaultSchedulingStrategy.records_iterator`,
    flattening both round-robins (across topics, and across the partitions
    within a topic) so no generator frame is resumed per record.

    The index map and each topic's buffer map are re-read at the start of
    every pass, and drained entries are popped from them, exactly as the
    pure-Python version does.
    """

    cdef:
        object index
        set to_remove
        dict cursors
        list topics
        list topic_cursors
        Py_ssize_t ti
        Py_ssize_t n

    def __cinit__(self, object index):
        self.index = index
        self.to_remove = set()
        self.cursors = {}
        self.topics = []
        self.topic_cursors = []
        self.ti = 0
        self.n = 0

    def __iter__(self):
        return self

    def __next__(self):
        cdef:
            Py_ssize_t i
            _TopicCursor cursor
            object item

        while True:
            if self.ti >= self.n:
                # `while index:` in the pure-Python generator.
                if not self.index:
                    raise StopIteration()
                self._start_pass()
                if self.n == 0:
                    raise StopIteration()
            i = self.ti
            self.ti += 1
            cursor = <_TopicCursor>PyList_GET_ITEM(self.topic_cursors, i)
            item = cursor.next()
            if item is _SENTINEL:
                # This topic is now empty, but we cannot remove it from the
                # map while iterating over it, so it goes to the next pass.
                self.to_remove.add(<object>PyList_GET_ITEM(self.topics, i))
                continue
            return item

    cdef _start_pass(self):
        cdef:
            object topic
            object buffer
            _TopicCursor cursor

        if self.to_remove:
            for topic in self.to_remove:
                self.index.pop(topic, None)
                self.cursors.pop(topic, None)
            self.to_remove.clear()
        elif PyList_GET_SIZE(self.topics) == len(self.index):
            self.ti = 0
            return
        self.topics = []
        self.topic_cursors = []
        for topic, buffer in self.index.items():
            cursor = <_TopicCursor>self.cursors.get(topic)
            if cursor is None or cursor.source is not buffer:
                cursor = _TopicCursor(buffer)
                self.cursors[topic] = cursor
            self.topics.append(topic)
            self.topic_cursors.append(cursor)
        self.n = PyList_GET_SIZE(self.topics)
        self.ti = 0


cpdef object records_iterator(object index):
    """Iterate over a topic index map in round-robin order."""
    return RoundRobinRecordIterator(index)
