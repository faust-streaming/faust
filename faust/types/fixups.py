import abc
import typing
from typing import Iterable

if typing.TYPE_CHECKING:
    from .app import AppT as _AppT
else:

    class _AppT:
        ...  # noqa


__all__ = ["FixupT"]


class FixupT(abc.ABC):

    app: _AppT

    @abc.abstractmethod
    def __init__(self, app: _AppT) -> None:
        ...

    @abc.abstractmethod
    def enabled(self) -> bool:
        ...

    @abc.abstractmethod
    def autodiscover_modules(self) -> Iterable[str]:
        ...

    @abc.abstractmethod
    def on_worker_init(self) -> None:
        ...
