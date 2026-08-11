import pytest

from faust.utils.functional import consecutive_numbers, translate


@pytest.mark.parametrize(
    "numbers,expected",
    [
        ([1, 2, 3, 4, 6, 7, 8], [1, 2, 3, 4]),
        ([1, 4, 6, 8, 10], [1]),
        ([1], [1]),
        ([103, 104, 105, 106, 100000000000], [103, 104, 105, 106]),
    ],
)
def test_consecutive_numbers(numbers, expected):
    assert next(consecutive_numbers(numbers), None) == expected


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
