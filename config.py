import json
from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from multiprocessing import Process, Queue  # pylint: disable=unused-import
from pathlib import Path
from types import NoneType
from typing import (Any, Iterable, Literal, NewType, Optional, Self, Type,
                    TypeAlias, TypedDict, TypeVar, Union, get_args, get_origin)
from zoneinfo import ZoneInfo

from sqlalchemy import ColumnElement, Engine, create_engine, or_
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


UploadAction = Literal["copy", "move"]
UploaderQueue = NewType("UploaderQueue",
                        'Queue[tuple[Iterable[Path | str], UploadAction, Path | str, Path | str]]')

UploaderAction = UploadAction | Literal["delete_files", "delete_folders"]
FolderUploaderQueue = NewType(
    "FolderUploaderQueue", "Queue[tuple[Iterable[Path | str], UploaderAction]]")



TRASH_FOLDER_DEFAULT_NAME = ".trash"
METADATA_FOLDER_DEFAULT_NAME = ".backupdata"
DATABASE_DEFAULT_NAME = "files"
DEFAULT_LOCAL_IGNORES = (
    ".stfolder",
    ".stignore",
    ".stversions",
    TRASH_FOLDER_DEFAULT_NAME,
    METADATA_FOLDER_DEFAULT_NAME
)
DEFAULT_RCLONE_GUI_URL_PATTERN = r"http://(?P<user>\S+):(?P<password>\S+)@" \
    r"(?P<host>\S+):(?P<port>\d+)/\?.*login_token=(?P<login_token>\S+) "

NoHash: TypeAlias = Any

@dataclass
class DataClassWithFromDict:
    """
    Dataclass with a from_dict method.
    """

    T = TypeVar("T")
    @classmethod
    def convert_type(cls, value: Any, target_type: Type[T]) -> T:
        """
        Converts a value to a given type.

        :param Any value: The value to convert.
        :param Type[T] target_type: The type to convert to.
        :raises ValueError: If the value cannot be converted to the given type.
        :return T: The converted value.
        """

        if get_origin(target_type) == Union:
            union_args = get_args(target_type)

            if NoneType in union_args and value is None:
                return None  # type: ignore

            union_args = filter(lambda x: x != NoneType, union_args)

            for t in union_args:
                try:
                    return cls.convert_type(value, t)
                except TypeError:
                    continue

            raise ValueError(f"Cannot convert {value} to {target_type}")

        if target_type is bytes:
            return bytes.fromhex(value) # type: ignore

        return target_type(value)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Self:
        """
        Creates an instance of the class from a dictionary.
        
        :param dict[str, Any] d: The dictionary to create the instance from.
        :return Self: The created instance.
        """

        typed_config = {key: cls.convert_type(value, field.type) for key, value in d.items()
                        for field in fields(cls) if field.name == key}
        return cls(**typed_config)


@dataclass
class RcloneGUIConfig(DataClassWithFromDict):
    """
    Configuration for the Rclone GUI.
    """

    host: str
    port: int
    user: str
    password: str
    login_token: str
    special_commands: dict[str, tuple[str, list[str]]] = field(default_factory = lambda: {
        "copy": ("sync/copy", ["srcFs", "dstFs", "createEmptySrcDirs"]),
        "move": ("sync/move", ["srcFs", "dstFs", "createEmptySrcDirs", "deleteEmptySrcDirs"]),
        "delete": ("operations/delete", ["fs"]),
        "purge": ("operations/purge", ["fs", "remote"]),
    })
    filter_params: set[str] = field(default_factory = lambda: {
        "--delete-excluded", "--exclude-file", "--exclude-from", "--exclude-rule", "--files-from",
        "--files-from-raw", "--filter-from", "--filter-rule", "--ignore-case", "--include-from",
        "--include-rule", "--max-age", "--max-size", "--min-age", "--min-size"})
    list_filter_params: set[str] = field(default_factory = lambda: {
        "--exclude-file", "--exclude-from", "--exclude-rule", "--files-from", "--files-from-raw",
        "--filter-from", "--filter-rule", "--include-from", "--include-rule"})
    max_async_poll_interval: int = 60  # seconds


@dataclass
class GlobalConfig(DataClassWithFromDict):
    """
    Global configuration.
    """

    api_key: str
    message_listener_address: tuple[str, int]
    message_listener_auth_token: bytes
    # config_file: Path
    logging_folder: Path
    logging_file: Path
    last_event_file: Path
    folder_configs: Path
    time_format: str = "%Y-%m-%d_%H.%M.%S,%f"
    timezone: ZoneInfo = ZoneInfo("Europe/Budapest")
    syncthing_retry_count: int = 10
    syncthing_retry_delay: int = 120  # seconds
    failure_expiry_days: int = 14
    max_failures_per_hour: int = 5
    max_failures_per_day: int = 20
    default_hashsum: NoHash = None
    rclone_gui_url_pattern: str = DEFAULT_RCLONE_GUI_URL_PATTERN
    rclone_gui: Optional[RcloneGUIConfig] = None

    @classmethod
    def read_from_file(cls, file: Path | str) -> Self:
        """
        Reads the global configuration from a file.

        :param Path | str file: _description_
        :return GlobalConfig: _description_
        """
        with open(file, encoding="utf-8") as f:
            config = json.load(f)
        return cls.from_dict(config)


@dataclass
class ArchiveConfig(DataClassWithFromDict):
    """
    Configuration for archival.
    """

    archive_folder: Path
    mount_folder: Optional[Path] = None
    archive_device: Optional[str] = None

    def get_summary(self) -> dict[str, Any]:
        """
        Returns a summary of the configuration.
        """
        return {
            "archive_folder": str(self.archive_folder),
            "mount_folder": str(self.mount_folder),
            "archive_device": self.archive_device
        }


class FolderConfig:
    """
    Configuraion for a folder.
    """

    def __init__(self,
                 global_config: GlobalConfig,
                 folder_id: str,
                 local_folder: Path | str,
                 remote_folder: Path | str,
                 database_name: Optional[str] = None,
                 trash_folder: Optional[Path | str] = None,
                 metadata_folder: Optional[Path | str] = None,
                 archive_config: Optional[ArchiveConfig] = None,
                 cloud_only_defaults: Sequence[str | tuple[str | Sequence[str],
                                                           str | Sequence[str]]] = (),
                 trash_keep_time: timedelta = timedelta(days=60),
                 local_keep_time: Optional[timedelta] = timedelta(days=60),
                 local_ignore_patterns: Sequence[str] = DEFAULT_LOCAL_IGNORES) -> None:

        self.global_config: GlobalConfig = global_config
        self.folder_id: str = folder_id
        self.local_folder: Path = Path(local_folder)
        self.remote_folder: Path = Path(remote_folder)
        if trash_folder is None:
            trash_folder = self.local_folder.joinpath(TRASH_FOLDER_DEFAULT_NAME)
        self.trash_folder: Path = Path(trash_folder)
        if metadata_folder is None:
            metadata_folder = self.local_folder.joinpath(METADATA_FOLDER_DEFAULT_NAME)
        self.metadata_folder: Path = Path(metadata_folder)
        self.archive_config: Optional[ArchiveConfig] = archive_config
        self.cloud_only_defaults: list[tuple[list[str], list[str]]] = [
            ([item], []) if isinstance(item, str) else
             tuple(map(lambda x: [x] if isinstance(x, str) else list(x), item))
             for item in cloud_only_defaults
        ]
        self.trash_keep_time: timedelta = trash_keep_time
        self.local_keep_time: Optional[timedelta] = local_keep_time
        self.local_ignore_patterns: list[str] = list(local_ignore_patterns)
        self.database: Engine = self.create_database(database_name)

        self.default_syncthing_ignores: list[str] = [
            f"/{self.trash_folder.name}",
            f"/{self.metadata_folder.name}"
        ]

    def get_summary(self) -> dict[str, Any]:
        """
        Return a summary of the configuration.

        :return dict[str, Any]: Different properties and their values.
        """

        return {
            "folder_id": self.folder_id,
            "local_folder": str(self.local_folder),
            "remote_folder": str(self.remote_folder),
            "trash_folder": str(self.trash_folder),
            "metadata_folder": str(self.metadata_folder),
            "archive": self.archive_config.get_summary()
                if self.archive_config is not None else None,
            "cloud_only_defaults": self.cloud_only_defaults,
            "trash_keep_time": self.trash_keep_time.total_seconds(),
            "local_keep_time": self.local_keep_time.total_seconds()
                if self.local_keep_time is not None else None,
            "local_ignore_patterns": self.local_ignore_patterns
        }

    @classmethod
    def read_from_file(cls, file: Path | str, global_config: GlobalConfig) -> "FolderConfig":
        """
        Reads the configuration from a file.

        :param Path | str file: The path to the file.
        :param GlobalConfig global_config: The global configuration
        :raises ValueError: If the global configuration is not provided and the one in the file is
            neither a dictionary nor a string containing the path to another file.
        :return FolderConfig: The resulting configuration.
        """

        with open(file, encoding="utf-8") as f:
            config: dict = json.load(f)

        if "archive_config" in config:
            config["archive_config"] = ArchiveConfig.from_dict(config["archive_config"])

        return FolderConfig(global_config=global_config, **config)



    def create_database(self, database_name: Optional[str] = DATABASE_DEFAULT_NAME) -> Engine:
        """
        Creates an Sqlite database with the given name.

        :param Optional[str] database_name: The name of the database, defaults to
            DATABASE_DEFAULT_NAME
        :return Engine: The database engine.
        """

        if database_name is None:
            database_name = DATABASE_DEFAULT_NAME
        engine = create_engine(f"sqlite:///{self.folder_id}-{database_name}.sqlite", echo=True)
        Base.metadata.create_all(engine)
        return engine


class Base(DeclarativeBase):
    """
    Database base
    """


class AllFiles(Base):
    """
    Database containing file information.
    """
    __tablename__ = "all_files"

    id: Mapped[int] = mapped_column(primary_key=True)
    path: Mapped[str]
    size: Mapped[int | None]
    hash: Mapped[str | None]
    modified: Mapped[datetime | None]
    uploaded: Mapped[datetime | None]  # last modification of the uploaded version
    cloud_only: Mapped[bool] = mapped_column(default=False)

    @hybrid_method
    def is_relative_to(self, path: str) -> bool:
        """
        Hybrid method for deciding whether a path in the database is relative to a given one.

        :param str path: The path to compare against.
        :return bool: True if the path in the database is relative to the given one.
        """

        return self.path.startswith(path)

    @hybrid_method
    def is_relative_to_any(self, paths: Iterable[str]) -> bool:  # type: ignore
        """
        Hybrid method for deciding whether a path in the database is relative to amy of the given
        ones.

        :param Iterable[str] paths: The paths to compare against.
        :return bool: True if the path in the database is relative to any of the given ones.
        """
        return any(self.path.startswith(path) for path in paths)

    @is_relative_to_any.expression
    def is_relative_to_any(cls, paths: Iterable[str]) -> ColumnElement[bool]:  # pylint: disable=no-self-argument
        """SQL expression for is_relative_to_any."""
        return or_(*[cls.path.startswith(path) for path in paths])  # type: ignore


# class SyncEvents(Base):
#     __tablename__ = "sync_events"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     syncthing_id: Mapped[int]
#     file_id: Mapped[int] = mapped_column(ForeignKey("all_files.id"))
#     time: Mapped[datetime]
#     action: Mapped[str]
#     event_type: Mapped[str]
#     file_type: Mapped[str]


FolderProperties = TypedDict("FolderProperties", {
    "config": FolderConfig,
    "uploader_queue": FolderUploaderQueue,
    "uploader_process": Process,
    "folder_changes_queue": "Queue[SyncthingChanges]",  # type: ignore
    "upload_syncer_process": Process
})
