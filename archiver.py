from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from shutil import copy2
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zoneinfo import ZoneInfo

import data_logger
from util import extend_ignores, get_file_info, write_checkfile


LOCAL_FOLDER = Path("~/bmt").expanduser()
REMOTE_FOLDER = Path("onedrive-kifu:bmt")
ARCHIVE_FOLDER = Path("/mnt/...")

IGNORE_FILE = Path("~/bmt/")
KEEP_ARCHIVED = Path("~/bmt/")

ALL_FILES = Path("~/bmt/")
SYNCED_FILES = Path("~/bmt/")
ARCHIVED_FILES = Path("~/bmt/")

KEEP_AGE = timedelta(days=60)

CONFIG_DATA = Path("~/bmt/...")

TRASH_FOLDER = Path("~/bmt/")
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


def eject(drive):
    r = subprocess.run(["sudo", "eject", drive], capture_output=True)
    if r.returncode:
        logging.error("A külső merevlemez (%s) leválasztása sikertelen. (kód: %d, '%s', '%s')",
                      drive, r.returncode, r.stdout, r.stderr)
        raise OSError(f"Failed to eject drive {drive}")


def reconnect(drive):
    r = subprocess.run(["findmnt", drive, "-J"], capture_output=True)
    if r.returncode:
        logging.warning("A merevlemez (%s) csatlakozottsági állapotának lekérése sikertelen. (kód: %d, '%s', '%s')",
                        drive, r.returncode, r.stdout, r.stderr)
    else:
        response = json.loads(r.stdout)
        if response["filesystems"]:
            logging.warning("A külső merevlemez (%s) az már használatban van.", drive)
            return

    r = subprocess.run(["sudo", "eject", "-t", drive], capture_output=True)
    if r.returncode:
        logging.error("A külső merevlemez (%s) csatlakoztatása sikertelen. (kód: %d, '%s', '%s')",
                      drive, r.returncode, r.stdout, r.stderr)
        raise OSError(f"Failed to reconnect drive {drive}")


def archive(freeup_needed=0):
    logging.info("Archválás...")
    
    with open(CONFIG_DATA) as f:
        config = json.load(f)

    try:
        archive_device = config["device"]
    except KeyError:
        logging.error("Az archiváláshoz szükséges eszköz nincs specifikálva, a konfigurációs fájl "
                      "nem tartalmmaza a 'device' mezőt.")
        return

    reconnect(archive_device)

    with open(IGNORE_FILE) as f:
        ignore_patterns = f.readlines()

    global_files = get_file_info(ALL_FILES)

    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)

        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, global_files)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_archived_files_path = dir_path.joinpath("sync.txt")
        deleted_files_path = dir_path.joinpath("deleted.txt")

        r = subprocess.run(["rclone", "check", str(checkfile), ARCHIVE_FOLDER,
                            "--checkcfile", "QuickXorHash",
                            "--differ", str(differing_files_path),
                            "--missing-on-dst", str(not_archived_files_path),
                            "--missing-on-src", str(deleted_files_path)], capture_output=True)

        if r.returncode:
            logging.error("Az archiválandó fájlok meghatározása nem sikerült, az összehasonlítás meghiúsult."
                          " (hibakód: %d, '%s', '%s')", r.returncode, r.stdout, r.stderr)

        with open(differing_files_path) as f:
            copy_to_archive = f.read().splitlines()

        with open(not_archived_files_path) as f:
            copy_to_archive += f.read().splitlines()

        with open(deleted_files_path) as f:
            delete_from_archive = f.read().splitlines()

    local_files = get_files(LOCAL_FOLDER, ignore_patterns)

    delete_from_local = []

    if freeup_needed < 0:
        logging.warning("A kért felszabadítandó tárhely mérete negatív (%d).", freeup_needed)
        freeup_needed = 0

    if freeup_needed:
        freed_up_space = 0

        for _, name, size in sorted((date, name, size) for name, (date, size) in local_files.items()):
            delete_from_local.append(name)
            freed_up_space += size

            if freed_up_space >= freeup_needed:
                break
        else:
            logging.warning("Nem lehetséges elegendő tárhely felszabadítása. Kért mennyiség: %d, "
                            "teljesíthető: %d", freeup_needed, freed_up_space)

    with NamedTemporaryFile() as f:
        f.write("\n".join(copy_to_archive))
        subprocess.run(["rclone", "copy", "--files-from", f.name,
                       str(LOCAL_FOLDER), str(ARCHIVE_FOLDER)])
    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)
        missing = dir_path.joinpath("missing.txt")
        r = subprocess.run(["rclone", "check", str(LOCAL_FOLDER), str(
            REMOTE_FOLDER), "--missing-on-dst", missing], capture_output=True)

        if r.returncode:
            logging.error("A lokális fájlok szinkronizáltságának ellenőrzése sikertelen. "
                          "Fájlok törlése kihagyára kerül. (hibakód: %d, '%s', '%s')", r.returncode, r.stdout, r.stderr)

        with open(missing) as f:
            files = f.read()

            if files:
                logging.warning("Néhány fájl még nem került szinkronizálásra. Ezek törlése nem fog megtörténni.")
                data_logger.log(files)

                delete_from_local = list(set(delete_from_local) - set(files.splitlines()))

    if not extend_ignores(delete_from_local):
        logging.error("A törlendő fájlok figyelmen kívül hagyása sikertelen, "
                      "a törlések nem fognak megtörténni.")
    else:
        with NamedTemporaryFile() as f:
            f.write("\n".join(delete_from_local))
            subprocess.run(["rclone", "move", "--files-from", f.name,
                           str(LOCAL_FOLDER), str(ARCHIVE_FOLDER)])

    with NamedTemporaryFile() as f:
        f.write("\n".join(delete_from_archive))
        subprocess.run(["rclone", "move", "--files-from", f.name,
                       str(ARCHIVE_FOLDER), str(TRASH_FOLDER)])

    eject(archive_device)
