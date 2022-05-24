from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from util import get_file_info, is_same_file


LOCAL_FOLDER = Path("~/bmt").expanduser()

ARCHIVE_FOLDER = Path("/mnt/...")

IGNORE_FILE = Path("~/bmt/")
KEEP_ARCHIVED = Path("~/bmt/")
SYNCED_FILES = Path("~/bmt/")

KEEP_AGE = timedelta(days=60)
# ARCHIVED_LIST = ARCHIVE_FOLDER.joinpath("")


def get_files(folder, ignores):
    files = {}

    for path in folder.glob("**/*"):
        stat = path.stat()
        relative_path = path.relative_to(folder)
        if not any(relative_path.match(pattern) for pattern in ignores):
            files[relative_path] = (datetime.fromtimestamp(stat.st_mtime).astimezone(ZoneInfo("Europe/Budapest")),
                                    stat.st_size)

    return files


def archive():
    with open(IGNORE_FILE) as f:
        ignore_patterns = f.readlines()

    synced_files = get_file_info(SYNCED_FILES)

    archived_files = get_files(ARCHIVE_FOLDER, ignore_patterns)
    local_files = get_files(LOCAL_FOLDER, ignore_patterns)

    files_to_delete = []
    files_to_copy = []

    for file, data in local_files.items():
        date, size = data
        if file in archived_files and is_same_file(data, archived_files[file]):
            if datetime.now() - date > KEEP_AGE:
                files_to_delete.append(file)
        else:
            files_to_copy.append(file)
