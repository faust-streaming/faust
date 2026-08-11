"""Serialization utilities.

Supported codecs
================

* **raw**     - No encoding/serialization (bytes only).
* **json**    - json with UTF-8 encoding.
* **yaml**    - YAML (safe version)
* **pickle**  - pickle with base64 encoding (not urlsafe).
* **pickle_restricted** - pickle with base64 encoding, restricted at load
  time to a safe allowlist of classes (see :class:`RestrictedUnpickler`).
* **binary**  - base64 encoding (not urlsafe).

.. warning::

    The **pickle** codec calls :func:`pickle.loads` on the raw message
    value, which can execute arbitrary code if the data comes from an
    untrusted source. Kafka topics do not authenticate producers, so any
    client able to write to a topic consumed with ``value_serializer=
    "pickle"`` (or ``key_serializer="pickle"``) can achieve remote code
    execution in the worker process. Only use the pickle codec for topics
    where every producer is trusted. If you need pickle's object support
    but cannot fully trust every producer, use **pickle_restricted**
    instead, and extend :attr:`RestrictedUnpickler.ALLOWED_CLASSES` with
    whatever application types you expect to receive.

Serialization by name
=====================

The :func:`dumps` function takes a codec name and the object to encode,
then returns bytes:

.. sourcecode:: pycon

    >>> s = dumps('json', obj)

For the reverse direction, the :func:`loads` function takes a codec
name and bytes to decode:

.. sourcecode:: pycon

    >>> obj = loads('json', s)

You can also combine encoders in the name, like in this case
where json is combined with gzip compression:

.. sourcecode:: pycon

    >>> obj = loads('json|gzip', s)

Codec registry
==============

Codecs are configured by name and this module maintains
a mapping from name to :class:`Codec` instance: the :attr:`codecs`
attribute.

You can add a new codec to this mapping by:

.. sourcecode:: pycon

    >>> from faust.serializers import codecs
    >>> codecs.register(custom, custom_serializer())

A codec subclass requires two methods to be implemented: ``_loads()``
and ``_dumps()``:

.. sourcecode:: python

    import msgpack

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(obj)

        def _loads(self, s: bytes) -> Any:
            return msgpack.loads(s)

Our codec now encodes/decodes to raw msgpack format, but we
may also need to transfer this payload over a transport easily confused
by binary data, such as JSON where everything is Unicode.

You can chain codecs together, so to add a binary text encoding like Base64,
to your codec, we use the ``|`` operator to form a combined codec:

.. sourcecode:: python

    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

    codecs.register('msgpack', msgpack())

At this point we monkey-patched Faust to support
our codec, and we can use it to define records like this:

.. sourcecode:: pycon

    >>> from faust.serializers import Record
    >>> class Point(Record, serializer='msgpack'):
    ...     x: int
    ...     y: int

The problem with monkey-patching is that we must make sure the patching
happens before we use the feature.

Faust also supports registering *codec extensions*
using setuptools entry points, so instead we can create an installable msgpack
extension.

To do so we need to define a package with the following directory layout:

.. sourcecode:: text

    faust-msgpack/
        setup.py
        faust_msgpack.py

The first file, :file:`faust-msgpack/setup.py`, defines metadata about our
package and should look like the following example:

.. sourcecode:: python

    from setuptools import setup, find_packages

    setup(
        name='faust-msgpack',
        version='1.0.0',
        description='Faust msgpack serialization support',
        author='Ola A. Normann',
        author_email='ola@normann.no',
        url='http://github.com/example/faust-msgpack',
        platforms=['any'],
        license='BSD',
        packages=find_packages(exclude=['ez_setup', 'tests', 'tests.*']),
        zip_safe=False,
        install_requires=['msgpack-python'],
        tests_require=[],
        entry_points={
            'faust.codecs': [
                'msgpack = faust_msgpack:msgpack',
            ],
        },
    )

The most important part being the ``entry_points`` key which tells
Faust how to load our plugin. We have set the name of our
codec to ``msgpack`` and the path to the codec class
to be ``faust_msgpack:msgpack``. This will be imported by Faust
as ``from faust_msgpack import msgpack``, so we need to define
that part next in our :file:`faust-msgpack/faust_msgpack.py` module:

.. sourcecode:: python

    from faust.serializers import codecs

    class raw_msgpack(codecs.Codec):

        def _dumps(self, obj: Any) -> bytes:
            return msgpack.dumps(s)


    def msgpack() -> codecs.Codec:
        return raw_msgpack() | codecs.binary()

That's it! To install and use our new extension we do:

.. sourcecode:: console

    $ python setup.py install

At this point may want to publish this on PyPI to share
the extension with other Faust users.
"""

import io
import pickle as _pickle  # nosec B403
import pickletools
import warnings
from base64 import b64decode, b64encode
from types import ModuleType
from typing import Any, Dict, FrozenSet, MutableMapping, Optional, Tuple, Union, cast

from mode.utils.compat import want_bytes, want_str
from mode.utils.imports import load_extension_classes

from faust.exceptions import ImproperlyConfigured, SecurityWarning
from faust.types.codecs import CodecArg, CodecT
from faust.utils import json as _json

try:
    import yaml as _yaml
except ImportError:  # pragma: no cover
    _yaml = cast(ModuleType, None)  # noqa


__all__ = [
    "Codec",
    "CodecArg",
    "register",
    "get_codec",
    "dumps",
    "loads",
]


_STACK_MARK = object()
_STACK_PLACEHOLDER = object()


class Codec(CodecT):
    """Base class for codecs."""

    #: next steps in the recursive codec chain.
    #: ``x = pickle | binary`` returns codec with
    #: children set to ``(pickle, binary)``.
    children: Tuple[CodecT, ...]

    #: cached version of children including this codec as the first node.
    #: could use chain below, but seems premature so just copying the list.
    nodes: Tuple[CodecT, ...]

    #: subclasses can support keyword arguments,
    #: the base implementation of :meth:`clone` uses this to
    #: preserve keyword arguments in copies.
    kwargs: Dict

    def __init__(
        self, children: Optional[Tuple[CodecT, ...]] = None, **kwargs: Any
    ) -> None:
        self.children = children or ()
        self.nodes = (self,) + self.children
        self.kwargs = kwargs

    def _loads(self, s: bytes) -> Any:
        # subclasses must implement this method.
        raise NotImplementedError()

    def _dumps(self, s: Any) -> bytes:
        # subclasses must implement this method.
        raise NotImplementedError()

    def dumps(self, obj: Any) -> bytes:
        """Encode object ``obj``."""
        # send _dumps to this instance, and all children.
        for node in self.nodes:
            obj = cast(Codec, node)._dumps(obj)
        return obj

    def loads(self, s: bytes) -> Any:
        """Decode object from string."""
        # send _loads to this instance, and all children in reverse order
        for node in reversed(self.nodes):
            s = cast(Codec, node)._loads(s)
        return s

    def clone(self, *children: CodecT) -> CodecT:
        """Create a clone of this codec, with optional children added."""
        new_children = self.children + children
        return type(self)(children=new_children, **self.kwargs)

    def __or__(self, other: Any) -> Any:
        # codecs can be chained together, e.g. binary() | json()
        if isinstance(other, CodecT):
            return self.clone(other)
        return NotImplemented

    def __repr__(self) -> str:
        return " | ".join(
            "{0}({1})".format(
                type(n).__name__, ", ".join(map(repr, cast(Codec, n).kwargs.values()))
            )
            for n in self.nodes
        )


class json(Codec):
    """:mod:`json` serializer."""

    def _loads(self, s: bytes) -> Any:
        return _json.loads(want_str(s))

    def _dumps(self, s: Any) -> bytes:
        return want_bytes(_json.dumps(s))


class yaml(Codec):
    """:pypi:`PyYAML` serializer."""

    def _loads(self, s: bytes) -> Any:
        if _yaml is None:
            raise ImproperlyConfigured("Missing yaml: pip install PyYAML")
        return _yaml.safe_load(want_str(s))

    def _dumps(self, s: Any) -> bytes:
        if _yaml is None:
            raise ImproperlyConfigured("Missing yaml: pip install PyYAML")
        return want_bytes(_yaml.safe_dump(s))


#: Warning shown whenever the unrestricted pickle codec is configured
#: or used to deserialize a message. See :class:`raw_pickle`.
UNSAFE_PICKLE_WARNING = (
    "The pickle codec calls pickle.loads() on message data, which can "
    "execute arbitrary code if the data does not come from a trusted "
    "producer. Only use value_serializer/key_serializer='pickle' on "
    "topics where every producer is trusted. If that cannot be "
    "guaranteed, use the 'pickle_restricted' codec instead, which limits "
    "unpickling to a safe allowlist of classes."
)


def uses_unsafe_pickle(codec: CodecArg) -> bool:
    """Return :const:`True` if ``codec`` resolves to the unrestricted pickle codec.

    This is used to warn as soon as an app/topic/model is *configured*
    to use ``value_serializer="pickle"``/``key_serializer="pickle"``,
    rather than waiting for the first message to be deserialized.
    """
    if isinstance(codec, str):
        return any(node == "pickle" for node in codec.split("|"))
    if isinstance(codec, Codec):
        return any(isinstance(node, raw_pickle) for node in codec.nodes)
    return False


def warn_if_unsafe_pickle(codec: CodecArg, *, stacklevel: int = 2) -> None:
    """Emit :class:`~faust.exceptions.SecurityWarning` if ``codec`` uses pickle.

    No-op if ``codec`` does not resolve to the unrestricted pickle codec.
    """
    if uses_unsafe_pickle(codec):
        warnings.warn(UNSAFE_PICKLE_WARNING, SecurityWarning, stacklevel=stacklevel + 1)


class raw_pickle(Codec):
    """:mod:`pickle` serializer with no encoding.

    .. danger::

        Calls :func:`pickle.loads` on the raw message value with no
        restrictions. Never use this (or the ``pickle`` codec that wraps
        it) on a topic where you cannot fully trust every producer -- see
        :data:`UNSAFE_PICKLE_WARNING`. Prefer :class:`restricted_pickle`
        (the ``pickle_restricted`` codec) when in doubt.
    """

    def _loads(self, s: bytes) -> Any:
        warnings.warn(UNSAFE_PICKLE_WARNING, SecurityWarning, stacklevel=3)
        return _pickle.loads(s)  # nosec B301

    def _dumps(self, obj: Any) -> bytes:
        return _pickle.dumps(obj)  # nosec B403


def pickle() -> Codec:
    """:mod:`pickle` serializer with base64 encoding."""
    return raw_pickle() | binary()


class RestrictedUnpickler(_pickle.Unpickler):  # type: ignore[misc]
    """A :class:`pickle.Unpickler` that only constructs allowlisted classes.

    Overrides :meth:`~pickle.Unpickler.find_class` to reject any
    class/function not listed in :attr:`ALLOWED_CLASSES`, closing off the
    classic ``__reduce__``-based RCE gadgets (``os.system``,
    ``subprocess.Popen``, ``builtins.exec``, and friends) that plain
    :func:`pickle.loads` will happily invoke for untrusted input.

    The default allowlist only covers common stdlib container/value
    types. Extend :attr:`ALLOWED_CLASSES` (or subclass and override
    :meth:`find_class`) to allow application-specific types you expect
    producers to send, for example::

        from faust.serializers.codecs import RestrictedUnpickler

        RestrictedUnpickler.ALLOWED_CLASSES = {
            **RestrictedUnpickler.ALLOWED_CLASSES,
            "myapp.models": frozenset({"Withdrawal", "Order"}),
        }
    """

    #: Mapping of module name to the class/function names allowed from it.
    #:
    #: ``bytes`` and ``bytearray`` are deliberately excluded even though
    #: they are safe *value* types: as REDUCE-invoked *callables* they
    #: each accept a single integer and allocate that many zero bytes
    #: (``bytearray(2_000_000_000)`` allocates ~2GB from a payload of a
    #: few dozen bytes), so allowing them here would let an attacker turn
    #: a tiny message into a memory-exhaustion DoS. Plain bytes/bytearray
    #: *values* still round-trip fine -- pickle has dedicated opcodes for
    #: literal bytes/bytearray data that never go through find_class.
    ALLOWED_CLASSES: Dict[str, FrozenSet[str]] = {
        "builtins": frozenset(
            {
                "dict",
                "list",
                "set",
                "frozenset",
                "tuple",
                "str",
                "int",
                "float",
                "complex",
                "bool",
                "object",
            }
        ),
        "collections": frozenset({"OrderedDict", "defaultdict", "deque", "Counter"}),
        "datetime": frozenset({"datetime", "date", "time", "timedelta", "timezone"}),
        "decimal": frozenset({"Decimal"}),
        "uuid": frozenset({"UUID"}),
    }

    def find_class(self, module: str, name: str) -> Any:
        """Look up ``module.name``, raising unless it is allowlisted."""
        _restricted_pickle_check_global(module, name, self.ALLOWED_CLASSES)
        return super().find_class(module, name)


def _restricted_pickle_check_global(
    module: str,
    name: str,
    allowed_classes: MutableMapping[str, FrozenSet[str]],
) -> None:
    allowed = allowed_classes.get(module)
    if allowed is None or name not in allowed:
        raise _pickle.UnpicklingError(
            f"Refusing to unpickle disallowed class/function "
            f"{module}.{name}: not in "
            "RestrictedUnpickler.ALLOWED_CLASSES. Extend the allowlist "
            "if this type is expected from your producers, or use the "
            "unrestricted 'pickle' codec if they are fully trusted."
        )


def _restricted_pickle_pop_mark(stack: list) -> None:
    while stack:
        if stack.pop() is _STACK_MARK:
            return
    raise _pickle.UnpicklingError("Malformed pickle stream: MARK not found")


def _restricted_pickle_memoize(
    memo: MutableMapping[int, Any], next_index: int, value: Any
) -> int:
    memo[next_index] = value
    return next_index + 1


def _restricted_pickle_validate_globals(
    payload: bytes,
    allowed_classes: MutableMapping[str, FrozenSet[str]],
) -> None:
    # PyPy's unpickler can load STACK_GLOBAL payloads without consulting an
    # overridden find_class(), so pre-scan the pickle bytecode and reject any
    # disallowed globals before unpickling executes them.
    stack = []
    memo: Dict[int, Any] = {}
    next_memo_index = 0
    for opcode, arg, _pos in pickletools.genops(payload):
        name = opcode.name
        if name in {"SHORT_BINUNICODE", "BINUNICODE", "BINUNICODE8", "UNICODE"}:
            stack.append(arg)
        elif name == "GLOBAL":
            if arg is None:
                raise _pickle.UnpicklingError(
                    "Malformed pickle stream: GLOBAL missing module/name"
                )
            module, _, global_name = arg.partition(" ")
            _restricted_pickle_check_global(module, global_name, allowed_classes)
            stack.append(_STACK_PLACEHOLDER)
        elif name == "STACK_GLOBAL":
            try:
                global_name = stack.pop()
                module = stack.pop()
            except IndexError as exc:
                raise _pickle.UnpicklingError(
                    "Malformed pickle stream: STACK_GLOBAL underflow"
                ) from exc
            if not isinstance(module, str) or not isinstance(global_name, str):
                raise _pickle.UnpicklingError(
                    "Malformed pickle stream: STACK_GLOBAL expected module/name strings"
                )
            _restricted_pickle_check_global(module, global_name, allowed_classes)
            stack.append(_STACK_PLACEHOLDER)
        elif name in {"PUT", "BINPUT", "LONG_BINPUT"}:
            if arg is None:
                raise _pickle.UnpicklingError(
                    f"Malformed pickle stream: {name} missing memo index"
                )
            memo_index = int(arg)
            memo[memo_index] = stack[-1]
            next_memo_index = max(next_memo_index, memo_index + 1)
        elif name in {"GET", "BINGET", "LONG_BINGET"}:
            if arg is None:
                raise _pickle.UnpicklingError(
                    f"Malformed pickle stream: {name} missing memo index"
                )
            stack.append(memo[int(arg)])
        elif name == "MEMOIZE":
            next_memo_index = _restricted_pickle_memoize(
                memo, next_memo_index, stack[-1]
            )
        else:
            before = [item.name for item in opcode.stack_before]
            if "mark" in before:
                _restricted_pickle_pop_mark(stack)
                for _ in range(before.index("mark")):
                    if not stack:
                        raise _pickle.UnpicklingError(
                            f"Malformed pickle stream: {name} underflow"
                        )
                    stack.pop()
            else:
                for _ in before:
                    if not stack:
                        raise _pickle.UnpicklingError(
                            f"Malformed pickle stream: {name} underflow"
                        )
                    stack.pop()
            for item in opcode.stack_after:
                stack.append(_STACK_MARK if item.name == "mark" else _STACK_PLACEHOLDER)


class restricted_pickle(Codec):
    """:mod:`pickle` serializer with no encoding, restricted to safe classes.

    Like :class:`raw_pickle`, but deserializes using
    :class:`RestrictedUnpickler` instead of :func:`pickle.loads`, so a
    payload that tries to construct anything outside the allowlist raises
    :exc:`pickle.UnpicklingError` instead of running arbitrary code.
    """

    def _loads(self, s: bytes) -> Any:
        _restricted_pickle_validate_globals(s, RestrictedUnpickler.ALLOWED_CLASSES)
        return RestrictedUnpickler(io.BytesIO(s)).load()

    def _dumps(self, obj: Any) -> bytes:
        # Protocol 5 gives bytearray values their own opcode (BYTEARRAY8)
        # instead of falling back to a `bytearray(...)` global + REDUCE,
        # which RestrictedUnpickler refuses (see ALLOWED_CLASSES above).
        return _pickle.dumps(obj, protocol=5)  # nosec B403


def pickle_restricted() -> Codec:
    """:mod:`pickle` serializer (allowlisted classes) with base64 encoding."""
    return restricted_pickle() | binary()


class binary(Codec):
    """Codec for binary content (uses Base64 encoding)."""

    def _loads(self, s: bytes) -> Any:
        return b64decode(s)

    def _dumps(self, s: bytes) -> bytes:
        return b64encode(want_bytes(s))


class raw(Codec):
    """Codec that does nothing at all."""

    def _loads(self, s: bytes) -> bytes:
        return want_bytes(s)

    def _dumps(self, s: bytes) -> bytes:
        return want_bytes(s)


#: Codec registry, mapping of name to :class:`Codec` instance.
codecs: MutableMapping[str, CodecT] = {
    "json": json(),
    "pickle": pickle(),  # nosec B403
    "pickle_restricted": pickle_restricted(),
    "binary": binary(),
    "raw": raw(),
    "yaml": yaml(),
}

#: Cached extension classes.
#: We have to defer extension loading to runtime as the
#: extensions will import from this module causing a circular import.
_extensions_finalized: MutableMapping[str, bool] = {}


def register(name: str, codec: CodecT) -> None:
    """Register new codec in the codec registry."""
    codecs[name] = codec


def _maybe_load_extension_classes(namespace: str = "faust.codecs") -> None:
    if namespace not in _extensions_finalized:
        _extensions_finalized[namespace] = True
        codecs.update({name: cls() for name, cls in load_extension_classes(namespace)})


def get_codec(name_or_codec: CodecArg) -> CodecT:
    """Get codec by name."""
    _maybe_load_extension_classes()
    if isinstance(name_or_codec, str):
        if "|" in name_or_codec:
            nodes = name_or_codec.split("|")
            # XXX ``codecs.get(node, node)`` falls back to the *name* when the
            # codec is unknown, so ``codec`` can hold a plain ``str`` and this
            # ``|=`` then blows up with ``TypeError: unsupported operand
            # type(s) for |=: 'str' and 'Codec'`` instead of a KeyError naming
            # the bad codec.  Real bug (e.g. ``get_codec('bad|json')``), but
            # the fallback also makes ``get_codec('|json')`` work, so changing
            # it is a behaviour change and out of scope for a typing pass.
            codec: Optional[Union[CodecT, str]] = None
            for node in nodes:
                if codec:
                    codec |= codecs[node]  # type: ignore[operator]
                else:
                    codec = codecs.get(node, node)

            return cast(Codec, codec)
        return codecs[name_or_codec]
    return cast(Codec, name_or_codec)


def dumps(codec: Optional[CodecArg], obj: Any) -> bytes:
    """Encode object into bytes."""
    return get_codec(codec).dumps(obj) if codec else obj


def loads(codec: Optional[CodecArg], s: bytes) -> Any:
    """Decode object from bytes."""
    return get_codec(codec).loads(s) if codec else s
