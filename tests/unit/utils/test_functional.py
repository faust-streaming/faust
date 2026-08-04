import pytest

from faust.utils.functional import (
    _py_first_consecutive_run,
    consecutive_numbers,
    first_consecutive_run,
    translate,
)

#: Both implementations of ``first_consecutive_run``.  ``first_consecutive_run``
#: is the Cython one whenever the extension could be built, and is otherwise the
#: same object as ``_py_first_consecutive_run`` (in which case this just runs
#: the pure-Python one twice).
FIRST_CONSECUTIVE_RUN_IMPLS = [_py_first_consecutive_run, first_consecutive_run]

RUN_CASES = [
    ([1, 2, 3, 4, 6, 7, 8], [1, 2, 3, 4]),
    ([1, 4, 6, 8, 10], [1]),
    ([1], [1]),
    ([103, 104, 105, 106, 100000000000], [103, 104, 105, 106]),
    # a run of numbers too large for a C long long must still work
    ([2**80, 2**80 + 1, 2**80 + 3], [2**80, 2**80 + 1]),
    # ... including when the overflow happens part-way through a run
    ([1, 2, 2**80], [1, 2]),
    # repeated numbers end a run, matching itertools.groupby
    ([1, 1, 2], [1]),
    ([0, 1, 2], [0, 1, 2]),
    # descending numbers are not a run
    ([5, 4, 3], [5]),
]


@pytest.mark.parametrize(
    "numbers,expected",
    [(numbers, expected) for numbers, expected in RUN_CASES if numbers],
)
def test_consecutive_numbers(numbers, expected):
    assert next(consecutive_numbers(numbers), None) == expected


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
@pytest.mark.parametrize("numbers,expected", RUN_CASES)
def test_first_consecutive_run(impl, numbers, expected):
    assert impl(list(numbers)) == expected


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__empty(impl):
    assert impl([]) == []


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__accepts_any_iterable(impl):
    assert impl(iter([1, 2, 3, 5])) == [1, 2, 3]
    assert impl(range(4)) == [0, 1, 2, 3]


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__does_not_mutate_argument(impl):
    numbers = [1, 2, 5]
    assert impl(numbers) == [1, 2]
    assert numbers == [1, 2, 5]


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
@pytest.mark.parametrize("numbers,expected", RUN_CASES)
def test_first_consecutive_run__matches_consecutive_numbers(impl, numbers, expected):
    # the helper must return exactly what taking the first group of
    # consecutive_numbers() would have returned.
    assert impl(list(numbers)) == next(consecutive_numbers(numbers), [])


@pytest.mark.parametrize(
    "table,s,expected",
    [
        ({".": "_", "@": "."}, "foo.bar@baz", "foo_bar.baz"),
        ({".": "_"}, "foo.bar", "foo_bar"),
        # multi-character patterns/replacements, not just single chars
        ({"foo": "bar"}, "foofoo", "barbar"),
        ({}, "unchanged", "unchanged"),
    ],
)
def test_translate(table, s, expected):
    assert translate(table, s) == expected


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__is_lazy(impl):
    # Must stop consuming as soon as the run is broken, leaving the rest of
    # the iterator for the caller (same as next(consecutive_numbers(...))).
    it = iter([1, 2, 5, 6, 7])
    assert impl(it) == [1, 2]
    assert list(it) == [6, 7]


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__does_not_over_consume(impl):
    # An iterable that blows up after the run has ended must not be reached.
    def numbers():
        yield 1
        yield 2
        yield 99
        raise AssertionError("consumed past the end of the run")

    assert impl(numbers()) == [1, 2]


@pytest.mark.parametrize("impl", FIRST_CONSECUTIVE_RUN_IMPLS)
def test_first_consecutive_run__list_resized_while_scanning(impl):
    # __sub__ can run arbitrary Python code, including code that shrinks the
    # list being scanned -- which must not read past the end of it.
    class Shrinks:
        def __init__(self, container=None):
            self.container = container

        def __sub__(self, other):
            if self.container is not None:
                del self.container[2:]
            return 1

    numbers = []
    numbers.extend([Shrinks(), Shrinks(numbers)] + [Shrinks() for _ in range(20)])
    assert len(impl(numbers)) <= 2
