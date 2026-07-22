"""Compatibility tests for model field union annotations.

These tests exercise the boundary between ``mode.utils.objects`` union
detection and Faust's model type-expression compiler.  They cover both
``typing.Union`` and PEP 604 (``X | Y``) syntax so dependency upgrades cannot
silently change which annotations reach ``UnionNode``.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

import pytest

from faust import Record
from faust.models.typing import TypeExpression


class Child(Record, namespace="test.union.Child"):
    value: int


@pytest.mark.parametrize(
    "annotation,value,expected",
    [
        (str | int | None, "hello", "hello"),
        (str | int | None, 42, 42),
        (str | int | None, None, None),
        (Union[str, int, None], "hello", "hello"),
        (Union[str, int, None], 42, 42),
        (Union[str, int, None], None, None),
        (Child | None, {"value": 3}, Child(value=3)),
        (Optional[Child], {"value": 3}, Child(value=3)),
        (List[Optional[int]], [1, None, 2], [1, None, 2]),
        (
            Dict[str, Optional[int]],
            {"one": 1, "none": None},
            {"one": 1, "none": None},
        ),
    ],
)
def test_supported_union_type_expressions_compile(annotation, value, expected):
    """Existing supported union forms must continue compiling."""
    converter = TypeExpression(annotation).as_function()
    assert converter(value) == expected


JSON_PASSTHROUGH_UNIONS = [
    str | list | dict | None,
    Union[str, list, dict, None],
    str | list[str] | dict[str, Any] | None,
    Union[str, List[str], Dict[str, Any], None],
]


@pytest.mark.parametrize("annotation", JSON_PASSTHROUGH_UNIONS)
@pytest.mark.xfail(
    reason="mode#80: Faust does not yet pass through heterogeneous JSON unions",
    strict=True,
)
def test_json_native_heterogeneous_unions_compile(annotation):
    """JSON-shaped heterogeneous unions should be safe pass-through fields."""
    converter = TypeExpression(annotation).as_function()
    for value in ("secret", ["secret"], {"secret": True}, None):
        assert converter(value) == value


@pytest.mark.xfail(
    reason="mode#80: class creation currently rejects the reported PEP 604 union",
    strict=True,
)
def test_issue_80_record_declaration_regression():
    """Exercise the exact model declaration reported in mode issue #80."""

    class SensitiveInfo(Record, namespace="test.union.SensitiveInfo"):
        data: str | list | dict | None
        meta: dict

    value = SensitiveInfo(data={"token": "redacted"}, meta={"source": "test"})
    assert value.data == {"token": "redacted"}


@pytest.mark.parametrize(
    "annotation",
    [
        str | Child,
        Union[str, Child],
        str | Decimal,
        Union[str, datetime],
        list[Child] | dict[str, Child],
    ],
)
def test_ambiguous_or_coercion_sensitive_unions_remain_unsupported(annotation):
    """Do not hide unions that require runtime type selection or coercion."""
    with pytest.raises(NotImplementedError, match="Union of types"):
        TypeExpression(annotation).as_function()
