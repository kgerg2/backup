from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, Iterable, Tuple
from zoneinfo import ZoneInfo

from requests import JSONDecodeError

import data_logger
from util import extend_ignores, get_file_info, read_path_list, run_command, write_checkfile


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


def get_files(folder: Path, ignores: Iterable[str]) -> Dict[Path, Tuple[datetime, int]]:
    logging.debug("A %s mappában található fájlok adatainak beolvasása.", folder)
    files = {}

    for path in folder.glob("**/*"):
        stat = path.stat()
        relative_path = path.relative_to(folder)
        if not any(relative_path.match(pattern) for pattern in ignores):
            files[relative_path] = (datetime.fromtimestamp(stat.st_mtime)
                                            .astimezone(ZoneInfo("Europe/Budapest")),
                                    stat.st_size)

    return files


def eject(drive: str) -> None:
    logging.debug("%s eszköz kiadása.", drive)

    run_command(["sudo", "eject", drive],
                error_message=f"A külső merevlemez ({drive}) leválasztása sikertelen.")


def reconnect(drive: str) -> None:
    logging.debug("%s eszköz csatlakoztatása.", drive)

    r = run_command(["findmnt", drive, "-J"],
                    error_message=f"A merevlemez ({drive}) csatlakozottsági állapotának lekérése "
                    "sikertelen.", strict=False)
    if r.returncode == 0 and r.stdout:
        try:
            response = json.loads(r.stdout)
        except JSONDecodeError as e:
            logging.warning(f"A merevlemez ({drive}) csatlakozottsági állapotának lekérése "
                            "sikertelen. Az újracsatlakoztatás meg lesz kísérelve.")
        else:
            if response["filesystems"]:
                logging.warning("A külső merevlemez (%s) az már csatlakoztatva van.", drive)
                return

    run_command(["sudo", "eject", "-t", drive],
                error_message=f"A külső merevlemez ({drive}) csatlakoztatása sikertelen.")


def archive(freeup_needed: int = 0) -> None:
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

    try:
        sync_with_archive(freeup_needed)
    except Exception as e:
        logging.error("Hiba történt az archiválás során: %s", e)
    finally:
        eject(archive_device)

def sync_with_archive(freeup_needed: int = 0):
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

        run_command(["rclone", "check", checkfile, ARCHIVE_FOLDER,
                     "--checkcfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_archived_files_path,
                     "--missing-on-src", deleted_files_path],
                    error_message="Az archiválandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", strict=False,
                    expected_returncodes=(1,))

        copy_to_archive = read_path_list(differing_files_path, default=[])

        copy_to_archive += read_path_list(not_archived_files_path, default=[])

        delete_from_archive = read_path_list(deleted_files_path, default=[])

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

    # Copy files to archive

    with NamedTemporaryFile() as f:
        f.write("\n".join(copy_to_archive))
        run_command(["rclone", "copy", "--files-from", f.name, LOCAL_FOLDER, ARCHIVE_FOLDER],
                    error_message="hiba történt az archiválás során.", strict=False)

    # Delete archived and synced files

    if delete_from_local:
        with TemporaryDirectory() as tempdir:
            dir_path = Path(tempdir)
            missing = dir_path.joinpath("missing.txt")
            try:
                run_command(["rclone", "check", LOCAL_FOLDER, REMOTE_FOLDER,
                            "--missing-on-dst", missing],
                            error_message="A törlendő lokális fájlok szinkronizáltságának "
                            "ellenőrzése sikertelen, a fájlok törlése kihagyára kerül.",
                            expected_returncodes=(1,))

                files = read_path_list(missing)

                if files:
                    logging.warning("Néhány fájl még nem került szinkronizálásra. "
                                    "Ezek törlése nem fog megtörténni.")
                    data_logger.log(files)

                    delete_from_local = set(delete_from_local) - set(files)
            except (CalledProcessError, OSError, FileNotFoundError) as e:
                logging.warning("A %s hiba miatt a fájlok törlése nem fog megtörténni.",
                                e.__class__)
                delete_from_local = []

    if delete_from_local:
        if not extend_ignores(delete_from_local):
            logging.error("A törlendő fájlok figyelmen kívül hagyása sikertelen, "
                        "a törlések nem fognak megtörténni.")
        else:
            with NamedTemporaryFile() as f:
                f.write("\n".join(delete_from_local))
                run_command(["rclone", "move", "--files-from", f.name, LOCAL_FOLDER,
                            ARCHIVE_FOLDER], error_message="A fájlok archívumba történő "
                            "áthelyezése során hiba történt.", strict=False)

    # Delete removed files from archive

    if delete_from_archive:
        with NamedTemporaryFile() as f:
            f.write("\n".join(delete_from_archive))
            run_command(["rclone", "move", "--files-from", f.name, ARCHIVE_FOLDER, TRASH_FOLDER],
                        error_message="Hiba történt a törölt fájlok archívumból kukába helyezése "
                        "közben.", strict=False)
