import base64
import importlib
import pickle
import sys
import types
import warnings
from typing import Mapping
from unittest.mock import patch

import pytest
from hypothesis import given
from hypothesis.strategies import binary, dictionaries, text
from mode.utils.compat import want_str

import faust.serializers._fickling as fickling_codec
from faust.exceptions import ImproperlyConfigured, SecurityWarning
from faust.serializers.codecs import (
    Codec,
    RestrictedUnpickler,
    _restricted_pickle_validate_globals,
    binary as _binary,
    codecs,
    dumps,
    get_codec,
    json,
    loads,
    pickle_fickling,
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


def _install_fake_fickling(monkeypatch, loads, unsafe_file_error) -> None:
    fake_fickling = types.ModuleType("fickling")
    fake_fickling.__path__ = []
    fake_loader = types.ModuleType("fickling.loader")
    fake_loader.loads = loads
    fake_exception = types.ModuleType("fickling.exception")
    fake_exception.UnsafeFileError = unsafe_file_error
    monkeypatch.setitem(sys.modules, "fickling", fake_fickling)
    monkeypatch.setitem(sys.modules, "fickling.loader", fake_loader)
    monkeypatch.setitem(sys.modules, "fickling.exception", fake_exception)


def test_pickle_fickling_requires_optional_dependency(monkeypatch) -> None:
    real_import_module = importlib.import_module
    fickling_modules = {"fickling.loader", "fickling.exception"}

    def import_module(name: str):
        if name in fickling_modules:
            raise ImportError(name)
        return real_import_module(name)

    monkeypatch.setattr(fickling_codec.importlib, "import_module", import_module)

    payload = dumps("pickle_fickling", DATA)
    with pytest.raises(ImproperlyConfigured, match=r"faust-streaming\[fickling\]"):
        loads("pickle_fickling", payload)


def test_pickle_fickling_uses_fickling_loads(monkeypatch) -> None:
    class FakeUnsafeFileError(Exception):
        pass

    calls = []

    def fake_loads(payload: bytes, **kwargs):
        calls.append((payload, kwargs))
        return pickle.loads(payload)

    _install_fake_fickling(monkeypatch, fake_loads, FakeUnsafeFileError)

    payload = dumps("pickle_fickling", DATA)
    assert loads("pickle_fickling", payload) == DATA
    assert calls == [(base64.b64decode(payload), {})]


def test_pickle_fickling_configures_max_acceptable_severity(monkeypatch) -> None:
    class FakeUnsafeFileError(Exception):
        pass

    class FakeSeverity:
        SUSPICIOUS = object()

    calls = []

    def fake_loads(payload: bytes, **kwargs):
        calls.append((payload, kwargs))
        return pickle.loads(payload)

    _install_fake_fickling(monkeypatch, fake_loads, FakeUnsafeFileError)

    codec = pickle_fickling(max_acceptable_severity=FakeSeverity.SUSPICIOUS)
    payload = codec.dumps(DATA)
    assert codec.loads(payload) == DATA
    assert calls == [
        (
            base64.b64decode(payload),
            {"max_acceptable_severity": FakeSeverity.SUSPICIOUS},
        )
    ]


def test_pickle_fickling_converts_fickling_rejections(monkeypatch) -> None:
    class FakeUnsafeFileError(Exception):
        pass

    def fake_loads(payload: bytes, **kwargs):
        raise FakeUnsafeFileError("blocked")

    _install_fake_fickling(monkeypatch, fake_loads, FakeUnsafeFileError)

    payload = dumps("pickle_fickling", DATA)
    with pytest.raises(
        pickle.UnpicklingError, match="Fickling rejected unsafe pickle payload"
    ):
        loads("pickle_fickling", payload)


def test_pickle_fickling_blocks_known_malicious_payload_when_installed() -> None:
    pytest.importorskip("fickling")
    payload = base64.b64encode(b"cos\nsystem\n(S'echo hello world'\ntR.")
    with pytest.raises(
        pickle.UnpicklingError, match="Fickling rejected unsafe pickle payload"
    ):
        loads("pickle_fickling", payload)


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


def test_pickle_restricted_round_trips_bytes_and_bytearray() -> None:
    val = {"a": bytes(b"hello"), "b": bytearray(b"world")}
    payload = dumps("pickle_restricted", val)
    assert loads("pickle_restricted", payload) == val


class _BytesAllocationGadget:
    def __reduce__(self):
        return (bytes, (2_000_000_000,))


class _ByteArrayAllocationGadget:
    def __reduce__(self):
        return (bytearray, (2_000_000_000,))


def test_pickle_restricted_blocks_bytes_allocation_gadget() -> None:
    payload = dumps("pickle_restricted", _BytesAllocationGadget())
    with pytest.raises(pickle.UnpicklingError):
        loads("pickle_restricted", payload)


def test_pickle_restricted_blocks_bytearray_allocation_gadget() -> None:
    payload = dumps("pickle_restricted", _ByteArrayAllocationGadget())
    with pytest.raises(pickle.UnpicklingError):
        loads("pickle_restricted", payload)


def _loads_raw_restricted_pickle(payload: bytes) -> object:
    return loads("pickle_restricted", base64.b64encode(payload))


def test_pickle_restricted_validates_protocol_0_global_opcode() -> None:
    assert _loads_raw_restricted_pickle(b"cbuiltins\nlist\n.") is list


def test_pickle_restricted_validates_put_and_get_memo_opcodes() -> None:
    assert _loads_raw_restricted_pickle(b"Vbuiltins\np0\ng0\n.") == "builtins"


@pytest.mark.parametrize(
    "payload,error",
    [
        (b"e.", "MARK not found"),
        (b"(e.", "APPENDS underflow"),
        (b".", "STOP underflow"),
        (b"\x80\x05\x93.", "STACK_GLOBAL underflow"),
        (
            b"\x80\x05K\x01K\x02\x93.",
            "STACK_GLOBAL expected module/name strings",
        ),
    ],
)
def test_pickle_restricted_rejects_malformed_stack_global_payloads(
    payload: bytes, error: str
) -> None:
    with pytest.raises(pickle.UnpicklingError, match=error):
        _loads_raw_restricted_pickle(payload)


class _PickleOpcode:
    def __init__(self, name: str) -> None:
        self.name = name
        self.stack_before = []
        self.stack_after = []


@pytest.mark.parametrize(
    "opcode,error",
    [
        ("GLOBAL", "GLOBAL missing module/name"),
        ("PUT", "PUT missing memo index"),
        ("GET", "GET missing memo index"),
    ],
)
def test_restricted_pickle_scanner_rejects_missing_opcode_arguments(
    monkeypatch, opcode: str, error: str
) -> None:
    monkeypatch.setattr(
        "faust.serializers.codecs.pickletools.genops",
        lambda payload: [(_PickleOpcode(opcode), None, 0)],
    )

    with pytest.raises(pickle.UnpicklingError, match=error):
        _restricted_pickle_validate_globals(b"", RestrictedUnpickler.ALLOWED_CLASSES)


class _PickleOptions:
    serializer = "pickle"


class _PickleModelType:
    _options = _PickleOptions()


class _JsonOptions:
    serializer = "json"


class _JsonModelType:
    _options = _JsonOptions()


def test_schema_warns_when_value_type_derives_pickle_serializer() -> None:
    # No explicit value_serializer -- the schema must derive it from
    # value_type._options.serializer and still warn.
    with pytest.warns(SecurityWarning):
        Schema(value_type=_PickleModelType)

    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        Schema(value_type=_JsonModelType)


def test_schema_warns_when_key_type_derives_pickle_serializer() -> None:
    with pytest.warns(SecurityWarning):
        Schema(key_type=_PickleModelType)

    with warnings.catch_warnings():
        warnings.simplefilter("error", SecurityWarning)
        Schema(key_type=_JsonModelType)


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
