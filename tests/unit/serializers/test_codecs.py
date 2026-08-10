import base64
import pickle
import warnings
from typing import Mapping
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis.strategies import binary, dictionaries, text
from mode.utils.compat import want_str

from faust.exceptions import ImproperlyConfigured, SecurityWarning
from faust.serializers.codecs import (
    Codec,
    RestrictedUnpickler,
    binary as _binary,
    codecs,
    dumps,
    get_codec,
    json,
    loads,
    register,
    uses_unsafe_pickle,
    warn_if_unsafe_pickle,
)
from faust.serializers.registry import Registry
from faust.serializers.schemas import Schema
from faust.utils import json as _json

DATA = {"a": 1, "b": "string", 1: 2}


def test_interface():
    s = Codec()
    with pytest.raises(NotImplementedError):
        s._loads(b"foo")
    with pytest.raises(NotImplementedError):
        s.dumps(10)
    assert s.__or__(1) is NotImplemented


@pytest.mark.parametrize("codec", ["json", "pickle", "pickle_restricted", "yaml"])
def test_json_subset(codec: str) -> None:
    if codec == "json":
        # special exception for json since integers can be serialized
        assert loads(codec, dumps(codec, DATA)) == {
            "a": 1,
            "b": "string",
            "1": 2,
        }
    else:
        assert loads(codec, dumps(codec, DATA)) == DATA


def test_missing_yaml_library() -> None:
    msg = "Missing yaml: pip install PyYAML"

    with patch("faust.serializers.codecs._yaml", None):
        with pytest.raises(ImproperlyConfigured):
            loads("yaml", dumps("yaml", DATA))
            pytest.fail(msg)

        with pytest.raises(ImproperlyConfigured):
            get_codec("yaml").loads(b"")
            pytest.fail(msg)


@given(binary())
def test_binary(input: bytes) -> None:
    assert loads("binary", dumps("binary", input)) == input


@given(dictionaries(text(), text()))
def test_combinators(input: Mapping[str, str]) -> None:
    s = json() | _binary()
    assert repr(s).replace("u'", "'") == "json() | binary()"

    d = s.dumps(input)
    assert isinstance(d, bytes)
    assert _json.loads(want_str(base64.b64decode(d))) == input


def test_get_codec():
    assert get_codec("json|binary")
    assert get_codec(Codec) is Codec


def test_register():
    try:

        class MyCodec(Codec): ...

        register("mine", MyCodec)
        assert get_codec("mine") is MyCodec
    finally:
        codecs.pop("mine")


def test_raw():
    bits = get_codec("raw").dumps("foo")
    assert isinstance(bits, bytes)
    assert get_codec("raw").loads(bits) == b"foo"


def test_pickle_loads_warns_of_security_risk() -> None:
    payload = dumps("pickle", DATA)
    with pytest.warns(SecurityWarning):
        assert loads("pickle", payload) == DATA


class _EvilPayload:
    def __reduce__(self):
        return (eval, ("1 + 1",))


def test_pickle_restricted_blocks_disallowed_classes() -> None:
    payload = dumps("pickle_restricted", _EvilPayload())
    with pytest.raises(pickle.UnpicklingError):
        loads("pickle_restricted", payload)


def test_pickle_restricted_does_not_warn() -> None:
    payload = dumps("pickle_restricted", DATA)
    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        assert loads("pickle_restricted", payload) == DATA


class _Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __reduce__(self):
        return (self.__class__, (self.x, self.y))


def test_restricted_unpickler_allows_extending_allowlist() -> None:
    original = dict(RestrictedUnpickler.ALLOWED_CLASSES)
    try:
        payload = dumps("pickle_restricted", _Point(1, 2))
        with pytest.raises(pickle.UnpicklingError):
            loads("pickle_restricted", payload)

        RestrictedUnpickler.ALLOWED_CLASSES = {
            **RestrictedUnpickler.ALLOWED_CLASSES,
            __name__: frozenset({"_Point"}),
        }
        point = loads("pickle_restricted", payload)
        assert (point.x, point.y) == (1, 2)
    finally:
        RestrictedUnpickler.ALLOWED_CLASSES = original


@pytest.mark.parametrize(
    "codec,expected",
    [
        ("pickle", True),
        ("pickle|binary", True),
        ("json", False),
        ("pickle_restricted", False),
        (None, False),
        (get_codec("pickle"), True),
        (get_codec("pickle_restricted"), False),
    ],
)
def test_uses_unsafe_pickle(codec, expected) -> None:
    assert uses_unsafe_pickle(codec) is expected


def test_warn_if_unsafe_pickle() -> None:
    with pytest.warns(SecurityWarning):
        warn_if_unsafe_pickle("pickle")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        warn_if_unsafe_pickle("json")  # should not raise/warn


def test_registry_warns_when_configured_with_pickle() -> None:
    with pytest.warns(SecurityWarning):
        Registry(value_serializer="pickle")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        Registry(value_serializer="pickle_restricted")


def test_schema_warns_when_configured_with_pickle() -> None:
    with pytest.warns(SecurityWarning):
        Schema(value_serializer="pickle")

    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        Schema(value_serializer="pickle_restricted")
