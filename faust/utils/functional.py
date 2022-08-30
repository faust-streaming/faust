"""Functional utilities."""
from functools import reduce
from itertools import groupby
from typing import Iterable, Iterator, Mapping, Sequence, Tuple, TypeVar

__all__ = [
    "consecutive_numbers",
    "translate",
]

T = TypeVar("T")


def consecutive_numbers(it: Iterable[int]) -> Iterator[Sequence[int]]:
    """Find runs of consecutive numbers.

    Notes:
        See https://docs.python.org/2.6/library/itertools.html#examples
    """
    for _, g in groupby(enumerate(it), lambda a: a[0] - a[1]):
        yield [a[1] for a in g]


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
