"""Functional utilities."""

import os
from functools import reduce
from itertools import groupby
from typing import Iterable, Iterator, List, Mapping, Sequence, Tuple, TypeVar

__all__ = [
    "consecutive_numbers",
    "first_consecutive_run",
    "translate",
]

T = TypeVar("T")

NO_CYTHON = bool(os.environ.get("NO_CYTHON", False))


def consecutive_numbers(it: Iterable[int]) -> Iterator[Sequence[int]]:
    """Find runs of consecutive numbers.

    Notes:
        See https://docs.python.org/2.6/library/itertools.html#examples
    """
    for _, g in groupby(enumerate(it), lambda a: a[0] - a[1]):
        yield [a[1] for a in g]


def _py_first_consecutive_run(numbers: Iterable[int]) -> List[int]:
    """Return the first run of consecutive numbers in ``numbers``.

    Equivalent to ``next(consecutive_numbers(numbers), [])``, but without
    building the intermediate tuples, the per-element key function and the
    group generators that :func:`itertools.groupby` needs.

    Callers that only need the first run (such as the consumer working out
    the next offset to commit) should use this instead of
    :func:`consecutive_numbers`, as it is the part of the commit path that
    scales with the number of un-committed offsets.
    """
    it = iter(numbers)
    try:
        prev = next(it)
    except StopIteration:
        return []
    run = [prev]
    for cur in it:
        if cur - prev != 1:
            break
        run.append(cur)
        prev = cur
    return run


if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.functional import first_consecutive_run
    except ImportError:
        first_consecutive_run = _py_first_consecutive_run
else:  # pragma: no cover
    first_consecutive_run = _py_first_consecutive_run


def translate(table: Mapping, s: str) -> str:
    """Replace characters and patterns in string ``s``.

    Works similar to :meth:`str.translate`, but replacements and patterns
    can be full length strings instead of character by character.

    Arguments:
        table: A mapping of characters/patterns to their replacement string.
        s: The string to translate

    Note:
        Table is the first argument in the signature for compatibility
        with :func:`~functools.partial`:

        .. sourcecode:: pycon

           >>> t = partial(translate, {'.': '_'})
           >>> t('foo.bar')
           'foo_bar'

    Examples:
        >>> translate('foo.bar@baz', {'.': '_', '@': '.'})
        'foo_bar.baz'
    """

    def on_reduce(acc: str, kv: Tuple[str, str]) -> str:
        return acc.replace(kv[0], kv[1])  # table key/value

    return reduce(on_reduce, table.items(), s)
