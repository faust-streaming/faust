# cython: language_level=3
"""Cython optimized functional utilities."""
from cpython.list cimport PyList_GET_ITEM, PyList_GET_SIZE
from cpython.long cimport PyLong_AsLongLongAndOverflow, PyLong_CheckExact


cdef object _SENTINEL = object()


cpdef list first_consecutive_run(object numbers):
    """Return the first run of consecutive numbers in ``numbers``.

    Equivalent to ``next(consecutive_numbers(numbers), [])``, but without
    building the intermediate tuples, the per-element Python key function
    and the group generators that :func:`itertools.groupby` needs.

    A run continues for as long as each number is exactly one greater than
    the one before it, which is the same rule ``groupby`` applies when
    grouping on ``index - value``.  Repeated numbers therefore end a run.

    Like the pure-Python version this stops consuming as soon as the run
    ends, so a non-sequence iterable is left positioned just past the first
    number that broke the run.
    """
    if type(numbers) is list:
        return _run_from_list(<list>numbers)
    return _run_from_iterable(iter(numbers))


cdef list _run_from_list(list seq):
    cdef:
        list run
        Py_ssize_t i
        object prev
        object cur
        long long c_prev
        long long c_cur
        int overflow

    if PyList_GET_SIZE(seq) == 0:
        return []

    prev = <object>PyList_GET_ITEM(seq, 0)
    run = [prev]
    i = 1

    # Fast path: plain ints that fit in a C long long.  Kafka offsets always
    # do, so this is what actually runs in production.  Nothing in this loop
    # can run Python code, so the size only has to be re-read for the slow
    # path below.
    if PyLong_CheckExact(prev):
        c_prev = PyLong_AsLongLongAndOverflow(prev, &overflow)
        if not overflow:
            while i < PyList_GET_SIZE(seq):
                cur = <object>PyList_GET_ITEM(seq, i)
                if not PyLong_CheckExact(cur):
                    break
                c_cur = PyLong_AsLongLongAndOverflow(cur, &overflow)
                # ``c_cur <= c_prev`` is tested first so the subtraction
                # below can never underflow.
                if overflow or c_cur <= c_prev or c_cur - 1 != c_prev:
                    break
                run.append(cur)
                c_prev = c_cur
                i += 1
            if i >= PyList_GET_SIZE(seq):
                return run
            prev = <object>PyList_GET_ITEM(seq, i - 1)

    # Slow path: arbitrary objects supporting ``-`` and comparison to 1.
    # ``cur - prev`` runs arbitrary Python code that may resize the list,
    # so the length is re-read on every iteration.
    while i < PyList_GET_SIZE(seq):
        cur = <object>PyList_GET_ITEM(seq, i)
        if cur - prev != 1:
            break
        run.append(cur)
        prev = cur
        i += 1
    return run


cdef list _run_from_iterable(object it):
    cdef:
        list run
        object prev
        object cur

    prev = next(it, _SENTINEL)
    if prev is _SENTINEL:
        return []
    run = [prev]
    while True:
        cur = next(it, _SENTINEL)
        if cur is _SENTINEL or cur - prev != 1:
            break
        run.append(cur)
        prev = cur
    return run
