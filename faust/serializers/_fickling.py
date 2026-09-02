"""Optional Fickling-backed pickle safety checks.

Fickling is isolated in this module and in ``requirements/extras/fickling.txt``
so Faust apps opt into it deliberately. Keeping this adapter separate also
makes the dependency easy to replace if Fickling has a bug, dependency issue,
or policy mismatch for Faust users. Callers may pass Fickling's
``fickling.loader.Severity`` constants through ``max_acceptable_severity`` to
choose a different scanner tolerance.
"""

import importlib
import pickle as _pickle  # nosec B403
from typing import Any, Tuple, Type, cast

from faust.exceptions import ImproperlyConfigured

__all__ = ["loads"]


def _load_fickling() -> Tuple[Any, Type[BaseException]]:
    try:
        fickling_loader = importlib.import_module("fickling.loader")
        fickling_exception = importlib.import_module("fickling.exception")
    except ImportError as exc:
        raise ImproperlyConfigured(
            "Missing fickling: pip install faust-streaming[fickling]"
        ) from exc
    unsafe_file_error = cast(Type[BaseException], fickling_exception.UnsafeFileError)
    return fickling_loader, unsafe_file_error


def loads(payload: bytes, *, max_acceptable_severity: Any = None) -> Any:
    """Load ``payload`` through Fickling's in-process safety check."""
    fickling_loader, unsafe_file_error = _load_fickling()
    kwargs = {}
    if max_acceptable_severity is not None:
        kwargs["max_acceptable_severity"] = max_acceptable_severity
    try:
        return fickling_loader.loads(payload, **kwargs)
    except unsafe_file_error as exc:
        raise _pickle.UnpicklingError(
            f"Fickling rejected unsafe pickle payload: {exc}"
        ) from exc
