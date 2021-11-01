"""In-memory table storage."""
from typing import Any, Callable, Iterable, MutableMapping, Optional, Set, Tuple

from faust.types import TP, EventT
from faust.types.stores import KT, VT

from . import base


class Store(base.Store, base.StoreT[KT, VT]):
    """Table storage using an in-memory dictionary."""

    def __post_init__(self) -> None:
        self.data: MutableMapping = {}
        self._key_partition: MutableMapping[KT, int] = {}

    def _clear(self) -> None:
        self.data.clear()

    def apply_changelog_batch(
        self,
        batch: Iterable[EventT],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
    ) -> None:
        """Apply batch of changelog events to in-memory table."""
        # default store does not do serialization, so we need
        # to convert these raw json serialized keys to proper structures
        # (E.g. regenerate tuples in WindowedKeys etc).
        to_delete: Set[Any] = set()
        delete_key = self.data.pop
        self.data.update(
            self._create_batch_iterator(to_delete.add, to_key, to_value, batch)
        )
        for key in to_delete:
            # If the key was assigned a value again, it will not be deleted.
            if not self.data[key]:
                delete_key(key, None)
                self._key_partition.pop(key, None)

    def _create_batch_iterator(
        self,
        mark_as_delete: Callable[[Any], None],
        to_key: Callable[[Any], Any],
        to_value: Callable[[Any], Any],
        batch: Iterable[EventT],
    ) -> Iterable[Tuple[Any, Any]]:
        for event in batch:
            key = to_key(event.key)
            # to delete keys in the table we set the raw value to None
            if event.message.value is None:
                mark_as_delete(key)
            self._key_partition[key] = event.message.partition
            yield key, to_value(event.value)

    async def on_recovery_completed(
        self, active_tps: Set[TP], standby_tps: Set[TP]
    ) -> None:
        partitions = {partition for _, partition in active_tps}

        delete_keys = []

        for k, p in self._key_partition.items():
            if p not in partitions:
                delete_keys.append(k)

        for k in delete_keys:
            self.data.pop(k, None)
            self._key_partition.pop(k, None)

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the persisted offset.

        This always returns :const:`None` when using the in-memory store.
        """
        return None

    def reset_state(self) -> None:
        """Remove local file system state.

        This does nothing when using the in-memory store.

        """
        ...
