from datetime import timedelta
from pathlib import Path


LOCAL_FOLDER = Path("~/bmt").expanduser()
REMOTE_FOLDER = Path("onedrive-kifu:bmt")
ARCHIVE_FOLDER = Path("~/bmt-tavoli").expanduser()
TRASH_FOLDER = Path("~/bmt/.trash").expanduser()

METADATA_FOLDER = LOCAL_FOLDER.joinpath(".backupdata")
ALL_FILES = METADATA_FOLDER.joinpath("all_files.txt")
SYNCED_FILES = METADATA_FOLDER.joinpath("synced_files.txt")
ARCHIVED_FILES = METADATA_FOLDER.joinpath("archived_files.txt")
UPLOADED_FILES = METADATA_FOLDER.joinpath("uploaded_files.txt")
CLOUD_ONLY_FILES = METADATA_FOLDER.joinpath("cloud_only_files.txt")
IGNORE_FILE = METADATA_FOLDER.joinpath("ignore_local.txt")
LAST_SYNC_EVENT_FILE = METADATA_FOLDER.joinpath("last_sync_event.txt")

CONFIG_DATA = METADATA_FOLDER.joinpath("config.json")
LOGGING_FILE = Path("~/Shared/Syncthing-dev/logs.txt").expanduser()

KEEP_AGE = timedelta(days=60)

API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
FOLDER_ID = "pfkxq-prxga"

SYNCTHING_RETRY_COUNT = 5
SYNCTHING_RETRY_DELAY = 120  # seconds

TRASH_KEEP_DAYS = 60

TIME_FORMAT = "%Y-%m-%d_%H.%M.%S,%f"

FAILURE_EXPIRY_DAYS = 30
MAX_FAILURES_PER_HOUR = 5
MAX_FAILURES_PER_DAY = 20


MESSAGE_LISTENER_ADDRESS = ("localhost", 6102)
MESSAGE_LISTENER_AUTH_TOKEN = b"7iaJmp6vFgwzHb02KCMqEa77xqQaYRx3"

# DEFAULT_HASHSUM = "0000000000000000000000000000000000000000"
DEFAULT_HASHSUM = None