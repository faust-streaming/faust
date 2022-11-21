import asyncio
import builtins
import sys
import types
import unittest
import unittest.mock
from contextlib import contextmanager
from time import time
from types import ModuleType
from typing import Any, Iterator, cast
from unittest.mock import Mock

from faust.events import Event
from faust.types.tuples import Message

__all__ = ["message", "new_event", "FutureMock", "mask_module", "patch_module"]


def message(
    key=None,
    value=None,
    *,
    topic="topic",
    partition=0,
    timestamp=None,
    headers=None,
    offset=1,
    checksum=None,
    generation_id=0,
):
    return Message(
        key=key,
        value=value,
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=timestamp or time(),
        timestamp_type=1 if timestamp else 0,
        headers=headers,
        checksum=checksum,
        generation_id=generation_id,
    )


def new_event(app, key=None, value=None, *, headers=None, **kwargs):
    return Event(
        app,
        key,
        value,
        headers,
        message(key=key, value=value, headers=headers, **kwargs),
    )


class FutureMock(unittest.mock.Mock):
    """Mock a :class:`asyncio.Future`."""

    awaited = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_event_loop()

    def __await__(self) -> Any:
        self.awaited = True
        yield self()

    def assert_awaited(self) -> None:
        assert self.awaited

    def assert_not_awaited(self) -> None:
        assert not self.awaited


@contextmanager
def patch_module(*names: str, new_callable: Any = Mock) -> Iterator:
    """Mock one or modules such that every attribute is a :class:`Mock`."""
    prev = {}

    class MockModule(types.ModuleType):
        def __getattr__(self, attr: str) -> Any:
            setattr(self, attr, new_callable())
            return types.ModuleType.__getattribute__(self, attr)

    mods = []
    for name in names:
        try:
            prev[name] = sys.modules[name]
        except KeyError:
            pass
        mod = sys.modules[name] = MockModule(name)
        mods.append(mod)
    try:
        yield mods
    finally:
        for name in names:
            try:
                sys.modules[name] = prev[name]
            except KeyError:
                try:
                    del sys.modules[name]
                except KeyError:
                    pass


@contextmanager
def mask_module(*modnames: str) -> Iterator:
    """Ban some modules from being importable inside the context.

    For example::

        >>> with mask_module('sys'):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print('sys not found')
        sys not found

        >>> import sys  # noqa
        >>> sys.version
        (2, 5, 2, 'final', 0)

    Taken from
    http://bitbucket.org/runeh/snippets/src/tip/missing_modules.py

    """
    realimport = builtins.__import__

    def myimp(name: str, *args: Any, **kwargs: Any) -> ModuleType:
        if name in modnames:
            raise ImportError(f"No module named {name}")
        else:
            return cast(ModuleType, realimport(name, *args, **kwargs))

    builtins.__import__ = myimp
    try:
        yield
    finally:
        builtins.__import__ = realimport
