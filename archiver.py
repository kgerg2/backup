import json
import logging
import re
import traceback
from datetime import datetime
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Dict, Iterable, List, Set, Tuple, Union
from zoneinfo import ZoneInfo

from requests.exceptions import JSONDecodeError

import data_logger
from config import (ALL_FILES, ARCHIVE_FOLDER, CONFIG_DATA, FOLDER_ID,
                    IGNORE_FILE, KEEP_AGE, LOCAL_FOLDER, REMOTE_FOLDER, TIMEZONE, TRASH_FOLDER)
from util import (extend_file_info, extend_ignores, get_file_details,
                  get_file_info, get_syncthing, is_same_file, read_path_list,
                  run_command, update_file_info, write_checkfile)


def get_files(folder: Path, ignores: Iterable[str]) -> Dict[Path, Tuple[datetime, int]]:
    logging.debug("A %s mappában található fájlok adatainak beolvasása.", folder)
    files = {}

    for path in folder.glob("**/*"):
        stat = path.stat()
        relative_path = path.relative_to(folder)
        if not any(relative_path.is_relative_to(pattern) for pattern in ignores):
            files[relative_path] = (datetime.fromtimestamp(stat.st_mtime)
                                            .astimezone(TIMEZONE),
                                    stat.st_size)

    return files

def validate_files(all_files: Dict[str, Tuple[str, datetime, int]], path: Path,
                   files: List[Dict[str, Union[str, int, dict, list]]], added: Set[str],
                   removed: Set[str], changed: Set[str]) -> None:
    for file in files:
        if file["type"] not in ("FILE_INFO_TYPE_FILE", "FILE_INFO_TYPE_DIRECTORY"):
            logging.warning("Ismeretlen fájltípus a Syncthing adatbázisában: %s", file)
            continue

        file_path = path.joinpath(file["name"])
        if file["type"] == "FILE_INFO_TYPE_DIRECTORY" and "children" in file:
            validate_files(all_files, file_path, file["children"], added, removed, changed)

        path_str = str(file_path)
        if path_str not in all_files:
            added.add(path_str)
            continue

        match = re.fullmatch(r"(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d{6})?\d*(\+\d\d:\d\d)", file["modTime"])

        if match is None:
            logging.error("Egy fájl módosítási ideje ismeretlen formátumú: %s (%s)",
                          file["modTime"], file["name"])
            removed.remove(path_str)
            continue
        
        date_time, ms, timezone = match.groups()
        ms = ms or ".000000"
        timezone = timezone or "+02:00"
        iso_time = "".join((date_time, ms, timezone))

        if not is_same_file((datetime.fromisoformat(iso_time), file["size"]), all_files[path_str]):
            changed.add(path_str)

        try:
            removed.remove(path_str)
        except KeyError:
            logging.warning("Egy elérési útvonal ('%s') többször is hozzáadásra vagy eltávolításra"
                            " került.", path_str)


def update_all_files(return_directories: bool = True) -> Dict[str, Tuple[str, datetime, int]]:
    try:
        known_files = get_file_info(ALL_FILES)
    except FileNotFoundError:
        logging.warning("Nem található az összes fájl adatatát tartalmazó dokumentum (%s).",
                        ALL_FILES)
        known_files = {}
    
    toplevel = get_syncthing("db/browse", {"folder": FOLDER_ID, "levels": 1})

    added = set()
    removed = set(known_files.keys())
    changed = set()
    path = Path()

    files = []

    for tl in toplevel:
        if tl["type"] == "FILE_INFO_TYPE_DIRECTORY":
            path = path.joinpath(tl["name"])
            files = get_syncthing("db/browse", {"folder": FOLDER_ID, "prefix": str(path)})
            validate_files(known_files, Path(tl["name"]), files, added, removed, changed)
        elif tl["type"] == "FILE_INFO_TYPE_FILE":
            validate_files(known_files, Path(), [tl], added, removed, changed)
        else:
            logging.warning("Ismeretlen fájltípus a Syncthing adatbázisában: %s", tl)
            continue

    exists = {file: get_file_details(LOCAL_FOLDER.joinpath(file)) for file in added | changed 
              if LOCAL_FOLDER.joinpath(file).exists()}

    known_files.update(exists)

    did_remove = False
    for file in removed:
        resp = get_syncthing("db/file", {"folder": FOLDER_ID, "file": file}, expected_errors=(404,))
        if isinstance(resp, str) and "No such object in the index" in resp \
            or resp["global"]["deleted"]:
            del known_files[file]
            did_remove = True
        else:
            logging.warning("A '%s' fájl nem szerepelt a globálisak között, mégse látszik "
                            "töröltnek.", file)
            data_logger.log(resp)

    if did_remove:
        update_file_info(ALL_FILES, known_files)
    elif exists:
        extend_file_info(ALL_FILES, exists)

    if not return_directories:
        known_files = {file: (hash, mod_time, size) for file, (hash, mod_time, size)
                       in known_files.items() if hash is not None}

    return known_files
        

def eject(drive: str) -> None:
    logging.debug("%s eszköz kiadása.", drive)

    if drive is None:
        logging.warning("A biztonsági mentés nem távoli esztközre történik.")
        return

    run_command(["sudo", "eject", drive],
                error_message=f"A külső merevlemez ({drive}) leválasztása sikertelen.")


def reconnect(drive: str) -> None:
    logging.debug("%s eszköz csatlakoztatása.", drive)

    if drive is None:
        logging.warning("A biztonsági mentés nem távoli esztközre történik.")
        return

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

    with open(CONFIG_DATA, encoding="utf-8") as f:
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
        logging.error("Hiba történt az archiválás során: %s", traceback.format_exc())
    finally:
        eject(archive_device)

def sync_with_archive(freeup_needed: int = 0):
    with open(IGNORE_FILE, encoding="utf-8") as f:
        ignore_patterns = f.read().splitlines()

    global_files = update_all_files(return_directories=False)

    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)

        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, global_files)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_archived_files_path = dir_path.joinpath("sync.txt")
        deleted_files_path = dir_path.joinpath("deleted.txt")

        r = run_command(["rclone", "check", checkfile, ARCHIVE_FOLDER,
                     "--checkfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_archived_files_path,
                     "--missing-on-src", deleted_files_path],
                    error_message="Az archiválandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", strict=False,
                    expected_returncodes=(1,))
        print(r)

        copy_to_archive = read_path_list(differing_files_path, default=[])

        copy_to_archive += read_path_list(not_archived_files_path, default=[])

        delete_from_archive = read_path_list(deleted_files_path, default=[])

    local_files = get_files(LOCAL_FOLDER, ignore_patterns)

    delete_from_local = []
    freed_up_space = 0

    if KEEP_AGE is not None:
        delete_from_local, freed_up_spaces = tuple(zip(*((file, size) for file, (time, size)
                                                   in local_files.items()
                                                   if datetime.now(TIMEZONE) - time > KEEP_AGE))) \
                                             or ([], [0])
        freed_up_space = sum(freed_up_spaces)


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
            
    data_logger.log(copy_to_archive=copy_to_archive, delete_from_archive=delete_from_archive, delete_from_local=delete_from_local)
    # Copy files to archive

    with NamedTemporaryFile(mode="w") as f:
        f.write("\n".join(copy_to_archive))
        f.flush()
        run_command(["rclone", "copy", "--files-from", f.name, LOCAL_FOLDER, ARCHIVE_FOLDER, "-vv"],
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
            with NamedTemporaryFile(mode="w") as f:
                f.write("\n".join(map(str, delete_from_local)))
                f.flush()
                run_command(["rclone", "move", "--files-from", f.name, LOCAL_FOLDER,
                            ARCHIVE_FOLDER], error_message="A fájlok archívumba történő "
                            "áthelyezése során hiba történt.", strict=False)

    # Delete removed files from archive

    if delete_from_archive:
        with NamedTemporaryFile(mode="w") as f:
            f.write("\n".join(delete_from_archive))
            f.flush()
            run_command(["rclone", "move", "--files-from", f.name, ARCHIVE_FOLDER, TRASH_FOLDER],
                        error_message="Hiba történt a törölt fájlok archívumból kukába helyezése "
                        "közben.", strict=False)
