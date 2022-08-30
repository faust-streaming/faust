import typing
from typing import (
    Any,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from mode.utils.compat import want_bytes, want_str

if typing.TYPE_CHECKING:
    from .models import ModelT as _ModelT
else:

    class _ModelT:
        ...  # noqa


__all__ = ["K", "V"]

#: Shorthand for the type of a key
K = Optional[Union[bytes, _ModelT, Any]]

#: Shorthand for the type of a value
V = Union[bytes, _ModelT, Any]

#: Kafka message headers (immutable)
Headers = Union[List[Tuple[str, bytes]], Mapping[str, bytes]]

#: Kafka message headers as optional argument (immutable)
HeadersArg = Optional[Headers]

#: Kafka message headers (mutable)
OpenHeaders = Union[List[Tuple[str, bytes]], MutableMapping[str, bytes]]

#: Kafka message headers as optional argument (mutable)
OpenHeadersArg = Optional[OpenHeaders]

#: Tuple of types/classes
_TYPTUP = Tuple[Type, ...]

#: List of classes to check if object is a tuple.
_TUPLE_TYPES: _TYPTUP = (tuple,)

#: List of classes to check if object is a mapping.
_MUTABLE_MAP_TYPES: _TYPTUP = (dict, MutableMapping)

#: List of classes to check if object is a sequence.
_MUTABLE_SEQ_TYPES: _TYPTUP = (list, MutableSequence)


def prepare_headers(
    target: HeadersArg,
    tuple_types: _TYPTUP = _TUPLE_TYPES,
    mutable_map_types: _TYPTUP = _MUTABLE_MAP_TYPES,
    mutable_seq_types: _TYPTUP = _MUTABLE_SEQ_TYPES,
) -> OpenHeadersArg:
    if isinstance(target, tuple_types):
        return list(cast(tuple, target))
    elif isinstance(target, mutable_map_types):
        return cast(dict, target)
    elif isinstance(target, mutable_seq_types):
        return cast(list, target)
    else:
        return list(cast(list, target))


def merge_headers(target: OpenHeadersArg, source: Mapping[str, Any]) -> None:
    # XXX modify in-place
    if source:
        source = {want_str(k): want_bytes(v) for k, v in source.items()}
        if isinstance(target, Mapping):
            target = cast(MutableMapping, target)
            target.update({k: v for k, v in source.items()})
        elif isinstance(target, list):
            target.extend((h for h in source.items()))
