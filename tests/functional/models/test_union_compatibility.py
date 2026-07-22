"""Compatibility tests for model field union annotations.

These tests exercise the boundary between mode.utils.objects union detection and
Faust's model type-expression compiler.  They intentionally cover both
``typing.Union`` and PEP 604 (``X | Y``) syntax so dependency upgrades cannot
silently change which annotations reach ``UnionNode``.
"""

from typing import Any, Dict, List, Optional, Union

import pytest

from faust import Record
from faust.models.typing import TypeExpression


class Child(Record, namespace="test.union.Child"):
    value: int


@pytest.mark.parametrize(
    "annotation,value",
    [
        (str | int | None, "hello"),
        (str | int | None, 42),
        (str | int | None, None),
        (Union[str, int, None], "hello"),
        (Union[str, int, None], 42),
        (Union[str, int, None], None),
        (Child | None, {"value": 3}),
        (Optional[Child], {"value": 3}),
        (List[Optional[int]], [1, None, 2]),
        (Dict[str, Optional[int]], {"one": 1, "none": None}),
    ],
)
def test_supported_union_type_expressions_compile(annotation, value):
    """Existing supported union forms must continue compiling."""
    converter = TypeExpression(annotation).as_function()
    result = converter(value)
    if annotation in {Child | None, Optional[Child]}:
        assert result == Child(value=3)
    else:
        assert result == value


@pytest.mark.parametrize(
    "annotation",
    [
        str | list | dict | None,
        Union[str, list, dict, None],
        str | list[str] | dict[str, Any] | None,
        Union[str, List[str], Dict[str, Any], None],
    ],
)
@pytest.mark.xfail(
    reason="mode#80: Faust does not yet pass through heterogeneous JSON-native unions",
    strict=True,
)
def test_json_native_heterogeneous_unions_compile(annotation):
    """JSON-native heterogeneous unions should be safe pass-through fields."""
    converter = TypeExpression(annotation).as_function()
    for value in ("secret", ["secret"], {"secret": True}, None):
        assert converter(value) == value


def test_issue_80_record_declaration_regression():
    """The exact mode#80 declaration must remain import/class-definition safe."""

    class SensitiveInfo(Record, namespace="test.union.SensitiveInfo"):
        data: str | list | dict | None
        meta: dict

    value = SensitiveInfo(data={"token": "redacted"}, meta={"source": "test"})
    assert value.data == {"token": "redacted"}


@pytest.mark.parametrize(
    "annotation",
    [
        datetime_union := (str | Child),
        Union[str, Child],
    ],
)
def test_ambiguous_model_and_scalar_union_remains_explicitly_unsupported(annotation):
    """Do not accidentally coerce ambiguous model/scalar unions as pass-through."""
    with pytest.raises(NotImplementedError, match="Union of types"):
        TypeExpression(annotation).as_function()
