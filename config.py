import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional, TypeAlias
from zoneinfo import ZoneInfo

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.hybrid import hybrid_method
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# MOUNT_FOLDER = "/media/kgerg/TOSHIBA EXT"
# CONFIG_DATA = METADATA_FOLDER.joinpath("config.json")
# LOGGING_FILE = Path("~/Shared/Syncthing-dev/logs.txt").expanduser()

# API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
# FOLDER_ID = "pfkxq-prxga"


# MESSAGE_LISTENER_ADDRESS = ("localhost", 6102)
# MESSAGE_LISTENER_AUTH_TOKEN = b"7iaJmp6vFgwzHb02KCMqEa77xqQaYRx3"

# DEFAULT_HASHSUM = None


TRASH_FOLDER_DEFAULT_NAME = ".trash"
METADATA_FOLDER_DEFAULT_NAME = ".backupdata"
DATABASE_DEFAULT_NAME = "files"
DEFAULT_LOCAL_IGNORES = (
    ".backupdata",
    ".trash",
    ".stfolder",
    ".stignore",
    ".stversions"
)

NoHash: TypeAlias = Any

@dataclass
class GlobalConfig:
    api_key: str
    message_listener_address: tuple[str, int]
    message_listener_auth_token: bytes
    # config_file: Path
    logging_folder: Path
    logging_file: Path
    time_format: str = "%Y-%m-%d_%H.%M.%S,%f"
    timezone: ZoneInfo = ZoneInfo("Europe/Budapest")
    syncthing_retry_count: int = 10
    syncthing_retry_delay: int = 120  # seconds
    failure_expiry_days: int = 14
    max_failures_per_hour: int = 5
    max_failures_per_day: int = 20
    default_hashsum: NoHash = None

    @classmethod
    def read_from_file(cls, file: Path | str) -> "GlobalConfig":
        """
        Reads the global configuration from a file.

        :param Path | str file: _description_
        :return GlobalConfig: _description_
        """
        with open(file, encoding="utf-8") as f:
            config = json.load(f)
        return cls.from_dict(config)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "GlobalConfig":
        typed_config = {key: field.type(value) for key, value in d.items() for field in fields(cls) if field.name == key}
        return GlobalConfig(**typed_config)

@dataclass
class ArchiveConfig:
    """
    Configuration for archival.
    """

    archive_folder: Path
    mount_folder: Path
    archive_device: str


class FolderConfig:
    """
    Configuraion for a folder.
    """

    def __init__(self,
                 global_config: GlobalConfig,
                 folder_id: str,
                 database: Engine,
                 local_folder: Path | str,
                 remote_folder: Path | str,
                 trash_folder: Optional[Path | str] = None,
                 metadata_folder: Optional[Path | str] = None,
                 archive_config: Optional[ArchiveConfig] = None,
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
        self.trash_keep_time: timedelta = trash_keep_time
        self.local_keep_time: Optional[timedelta] = local_keep_time
        self.local_ignore_patterns: list[str] = list(local_ignore_patterns)
        self.database: Engine = database

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
            "archive": asdict(self.archive_config) if self.archive_config is not None else None,
            "trash_keep_time": self.trash_keep_time.total_seconds()
        }
        
    @classmethod
    def read_from_file(cls, file: Path | str, global_config: Optional[GlobalConfig] = None) -> "FolderConfig":
        """
        Reads the configuration from a file.

        :param Path | str file: The path to the file.
        :param Optional[GlobalConfig] global_config: The global configuration, if not included,
            will be read from the file, defaults to None
        :raises ValueError: If the global configuration is not provided and the one in the file is
            neither a dictionary nor a string containing the path to another file.
        :return FolderConfig: The resulting configuration.
        """

        with open(file, encoding="utf-8") as f:
            config: dict = json.load(f)

        if global_config is None:
            global_config = config.pop("global_config")

            if isinstance(global_config, dict):
                global_config = GlobalConfig.from_dict(global_config)
            elif isinstance(global_config, str):
                global_config = GlobalConfig.read_from_file(global_config)
            else:
                raise ValueError("'global_config' must be a string ora  dictionary.")
        else:
            config.pop("global_config", None)


        database = cls.create_database(config.pop("database", None))

        return FolderConfig(global_config=global_config, database=database, **config)



    @staticmethod
    def create_database(database_name: Optional[str] = DATABASE_DEFAULT_NAME) -> Engine:
        """
        Creates an Sqlite database with the given name.

        :param Optional[str] database_name: The name of the database, defaults to
            DATABASE_DEFAULT_NAME
        :return Engine: The database engine.
        """

        if database_name is None:
            database_name = DATABASE_DEFAULT_NAME
        return create_engine(f"sqlite://{database_name}", echo=True)


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
    size: Mapped[int]
    hash: Mapped[str | None]
    modified: Mapped[datetime]
    uploaded: Mapped[datetime | None]  # last modification of the uploaded version
    cloud_only: Mapped[bool] = mapped_column(default=False)

    @hybrid_method
    def is_relative_to(self, path: Path | str) -> bool:
        """
        Hybrid method for deciding whether a path in the database is relative to a given one.

        :param Path | str path: The path to compare against.
        :return bool: True if the path in the database is relative to the given one.
        """

        return Path(self.path).is_relative_to(path)

    @hybrid_method
    def is_relative_to_any(self, paths: Iterable[Path | str]) -> bool:
        """
        Hybrid method for deciding whether a path in the database is relative to amy of the given
        ones.

        :param Iterable[Path | str] paths: The paths to compare against.
        :return bool: True if the path in the database is relative to any of the given ones.
        """
        return any(Path(self.path).is_relative_to(path) for path in paths)


# class SyncEvents(Base):
#     __tablename__ = "sync_events"

#     id: Mapped[int] = mapped_column(primary_key=True)
#     syncthing_id: Mapped[int]
#     file_id: Mapped[int] = mapped_column(ForeignKey("all_files.id"))
#     time: Mapped[datetime]
#     action: Mapped[str]
#     event_type: Mapped[str]
#     file_type: Mapped[str]
