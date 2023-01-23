from abc import abstractmethod
from collections.abc import Sequence
import logging
from multiprocessing import Queue
from pathlib import Path
from typing import Generic, Literal, NewType, NoReturn, Optional, TypeVar, TypedDict
from typing_extensions import override
from config import GlobalConfig

from util import get_syncthing, retry_on_error

T = TypeVar("T")


class ChangeListener(Generic[T]):
    """
    Listens for changes described by type `T` with the `get_change` method and puts them into all
    of the provided queues.
    """

    def __init__(self, queues: Sequence[Queue[T]], error_message: Optional[str] = None) -> None:
        self.queues = queues

        retry_on_error(self.listen, error_message=error_message)

    def listen(self) -> NoReturn:
        """
        The main function of this class. See class docstring.

        :return NoReturn: does not return
        """

        while True:
            update = self.get_change()
            for queue in self.queues:
                queue.put(update)

    @abstractmethod
    def get_change(self) -> T:
        """
        Should return a change of type `T`.

        :raises NotImplementedError: because the function is abstract
        :return T: the properties of the observed change
        """

        raise NotImplementedError()


SyncthingLocalChangeDetectedData = TypedDict("SyncthingLocalChangeDetectedData", {
    "action": Literal["deleted"] | Literal["modified"],
    "folder": str,
    "folderID": str,
    "label": str,
    "path": str,
    "type": Literal["file"] | Literal["directory"] | Literal["dir"]
})

SyncthingLocalChangeDetected = TypedDict("SyncthingLocalChangeDetected", {
    "id": int,
    "globalID": int,
    "time": str,
    "type": Literal["LocalChangeDetected"],
    "data": SyncthingLocalChangeDetectedData
})


SyncthingRemoteChangeDetectedData = TypedDict("SyncthingRemoteChangeDetectedData", {
    "type": str,
    "action": str,
    "folder": str,
    "folderID": str,
    "path": str,
    "label": str,
    "modifiedBy": str
})

SyncthingRemoteChangeDetected = TypedDict("SyncthingRemoteChangeDetected", {
    "time": str,
    "globalID": int,
    "data": SyncthingRemoteChangeDetectedData,
    "type": Literal["RemoteChangeDetected"],
    "id": int
})


SyncthingChanges = NewType(
    "SyncthingChanges", "list[SyncthingLocalChangeDetected | SyncthingRemoteChangeDetected]")


SyncthingDbBrowseData = TypedDict("SyncthingDbBrowseData", {
    "modTime": str,
    "name": str,
    "size": int,
    "type": Literal["FILE_INFO_TYPE_DIRECTORY"] | Literal["FILE_INFO_TYPE_FILE"],
    "children": "list[SyncthingDbBrowseData]"
})


class SyncthingChangeListener(ChangeListener[SyncthingChanges]):
    """
    Listens for changes in Syncthing and puts every change in all of the provided queues.
    Subclass of ChangeListener[SyncthingChanges].
    """

    def __init__(self, config: GlobalConfig, queues: "Sequence[Queue[SyncthingChanges]]",
                 last_event_file: Path | str, last_event: Optional[int] = None, timeout=3600) \
            -> None:
        self.config: GlobalConfig = config
        self.last_event_file = last_event_file
        if last_event is None:
            last_event = self.get_last_event()
        self.last_event = last_event
        self.timeout = timeout

        super().__init__(queues, "Hiba történt a Syncthing események lekérdezése közben.")

    @override
    def get_change(self) -> SyncthingChanges:
        """
        Gets the changes from Syncthing, stores last event id in `self.last_event` and the
        corresponding file.

        :return SyncthingChanges: the changes from Syncthing
        """

        changes: SyncthingChanges = self.get_syncthing_changes()

        if not changes:
            return changes

        if "id" not in changes[-1]:
            logging.warning("The last change doesn't contain an 'id' field: %s", changes[-1])
            return changes

        self.last_event = changes[-1]["id"]
        self.write_last_event()
        return changes

    def get_syncthing_changes(self, last_event: Optional[int] = None,
                              timeout: Optional[int] = None) -> SyncthingChanges:
        """
        Returns the changes from Syncthing since `last_event` with timeout `timeout`.

        :param Optional[int] last_event: the id of the last event, defaults to `None`
            => `self.last_event`
        :param Optional[int] timeout: the max wait time in seconds, defaults to `None`
            => `self.timeout`
        :return SyncthingChanges: returned value from Syncthing
        """

        if last_event is None:
            last_event = self.last_event
        if timeout is None:
            timeout = self.timeout

        return get_syncthing("events/disk", self.config, {"since": last_event, "timeout": timeout})

    def get_last_event(self) -> int:
        """
        Returns the last processed Syncthing event id if possible.
        Defaults to 0 if no stored id is found or Syncthing has reset its counter.

        :return int: the last event's id
        """

        try:
            with open(self.last_event_file, encoding="utf-8") as f:
                found = int(f.read())
        except:
            return 0

        if found <= 0:
            return 0

        last_changes = self.get_syncthing_changes(last_event=found - 1, timeout=5)

        if not last_changes:
            return 0

        return found

    def write_last_event(self) -> None:
        """
        Writes `self.last_event` into `self.last_event_file`.
        """

        with open(self.last_event_file, "w", encoding="utf-8") as f:
            f.write(str(self.last_event))
