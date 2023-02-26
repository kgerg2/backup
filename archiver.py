import json
import logging
import re
import traceback
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from subprocess import CalledProcessError
from tempfile import NamedTemporaryFile, TemporaryDirectory

from requests.exceptions import JSONDecodeError
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

import data_logger
from change_listener import SyncthingDbBrowseData
from config import AllFiles, ArchiveConfig, FolderConfig, GlobalConfig, NoHash, FolderProperties
from util import (discard_ignores, extend_ignores, get_file_details,
                  get_syncthing, is_same_file, read_path_list, run_command,
                  write_checkfile)


def get_files(config: FolderConfig, return_directories: bool = True) -> \
        dict[Path, tuple[datetime, int]]:
    """
    Gets the last modification time and the size of all the files (and possibly dictionaries) in a
    folder excluding those relative to `config.local_ignore_patterns`.

    :param FolderConfig config: The folder configuration
    :param bool return_directories: Whether to include directories, defaults to True
    :return dict[Path, tuple[datetime, int]]: The mod. date and size of all the files relative to
        the local folder.
    """

    logging.debug("A %s mappában található fájlok adatainak beolvasása.", config.local_folder)
    files = {}

    for path in config.local_folder.glob("**/*"):
        stat = path.stat()
        relative_path = path.relative_to(config.local_folder)

        if (return_directories or path.is_file()) and \
                not any(relative_path.is_relative_to(pattern)
                        for pattern in config.local_ignore_patterns):

            files[relative_path] = (datetime.fromtimestamp(stat.st_mtime)
                                            .astimezone(config.global_config.timezone),
                                    stat.st_size)

    return files


def validate_files(all_files: dict[str, tuple[str, datetime, int]], path: Path,
                   files: list[SyncthingDbBrowseData], added: set[str],
                   removed: set[str], changed: set[str], config: FolderConfig) -> None:
    """
    Looks for any changes in the local folder relative to the state of `all_files`.

    :param dict[str, tuple[str, datetime, int]] all_files: The currently known files (relative path
        mapped to hash, last modification time and size).
    :param Path path: The current folder path relative to the local folder.
    :param list[SyncthingDbBrowseData] files: The contents of this folder according to Syncthing.
    :param set[str] added: A set containing all the newly added files (previously not in
        `all_files`).
    :param set[str] removed: A set containing all the removed files. This gets emptied as all
        the files get checked.
    :param set[str] changed: A set containing all the changed files.
    :param FolderConfig config: The configuration for the current folder.
    """

    for file in files:
        if file["type"] not in ("FILE_INFO_TYPE_FILE", "FILE_INFO_TYPE_DIRECTORY"):
            logging.warning("Ismeretlen fájltípus a Syncthing adatbázisában: %s", file)
            continue

        file_path = path.joinpath(file["name"])
        if file["type"] == "FILE_INFO_TYPE_DIRECTORY" and "children" in file:
            validate_files(all_files, file_path, file["children"], added, removed, changed, config)

        path_str = str(file_path)
        if path_str not in all_files:
            added.add(path_str)
            continue

        match = re.fullmatch(r"(\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d*)?(\+\d\d:\d\d)",
                             file["modTime"])

        if match is None:
            logging.error("Egy fájl módosítási ideje ismeretlen formátumú: %s (%s)",
                          file["modTime"], file["name"])
            removed.remove(path_str)
            continue

        date_time, ms, timezone = match.groups()
        ms = ((ms or ".") + "0"*6)[:7]
        timezone = timezone or "+02:00"
        iso_time = "".join((date_time, ms, timezone))

        if file["type"] == "FILE_INFO_TYPE_FILE" and \
                not is_same_file((datetime.fromisoformat(iso_time), file["size"]),
                                 all_files[path_str], config):
            changed.add(path_str)

        try:
            removed.remove(path_str)
        except KeyError:
            logging.warning("Egy elérési útvonal ('%s') többször is hozzáadásra vagy eltávolításra"
                            " került.", path_str)


def update_all_files(config: FolderConfig, return_directories: bool = True) \
        -> dict[str, tuple[str, datetime, int]]:
    """
    Update the database with new / changed / removed files and return the current state.

    :param FolderConfig config: The folder configuration.
    :param bool return_directories: Whether to return directories, defaults to True
    :return dict[str, tuple[str, datetime, int]]: All the current files with hashsum, last
        modification date and size.
    """

    with Session(config.database) as session:
        select_stmt = select(AllFiles)
        logging.debug("SQL parancs futtatása: %s", select_stmt)
        result = session.execute(select_stmt)
        data = result.scalars().all()
        known_files: dict[str, tuple[NoHash | str, datetime, int]] = \
            {r.path: (r.hash, r.modified, r.size) for r in data}

    toplevel = get_syncthing("db/browse", config.global_config,
                             {"folder": config.folder_id, "levels": 0})

    added: set[str] = set()
    removed: set[str] = set(known_files.keys())
    changed: set[str] = set()

    for tl in toplevel:
        if tl["type"] == "FILE_INFO_TYPE_DIRECTORY":
            removed.discard(tl["name"])
            files = get_syncthing("db/browse", config.global_config,
                                  {"folder": config.folder_id, "prefix": tl["name"]})
            validate_files(known_files, Path(tl["name"]), files, added, removed, changed, config)
        elif tl["type"] == "FILE_INFO_TYPE_FILE":
            validate_files(known_files, Path(), [tl], added, removed, changed, config)
        else:
            logging.warning("Ismeretlen fájltípus a Syncthing adatbázisában: %s", tl)
            continue

    discard_ignores(added | removed | changed, config)

    exists = {file: get_file_details(Path(file), config) for file
              in added | changed if config.local_folder.joinpath(file).exists()}

    known_files.update(exists)

    paths_to_delete: list[str] = []
    for file in removed:
        resp = get_syncthing("db/file", config.global_config,
                             {"folder": config.folder_id, "file": file}, expected_errors=(404,))

        if isinstance(resp, str) and "No such object in the index" in resp \
                or (isinstance(resp, dict)
                    and (resp["global"]["deleted"] or resp["global"]["ignored"])):

            del known_files[file]
            paths_to_delete.append(file)

        else:
            logging.warning("A '%s' fájl nem szerepelt a globálisak között, mégse látszik "
                            "töröltnek.", file)
            data_logger.log(config.global_config, resp)

    if paths_to_delete or exists:
        with Session(config.database) as session:
            if paths_to_delete:
                delete_stmt = delete(AllFiles).where(AllFiles.path.in_(paths_to_delete))
                logging.debug("SQL parancs futtatása: %s", delete_stmt)
                session.execute(delete_stmt)

            if exists:
                session.add_all(AllFiles(path=path, size=s, hash=h, modified=m)
                                for path, (h, m, s) in exists.items())

            session.commit()

    if not return_directories:
        known_files = {file: (hash, mod_time, size) for file, (hash, mod_time, size)
                       in known_files.items() if hash is not None}

    return known_files


def eject(archive_config: ArchiveConfig, global_config: GlobalConfig) -> None:
    """
    Eject an external drive.

    :param ArchiveConfig archive_config: The archival configuration (contains the device).
    :param GlobalConfig global_config: The global configuration.
    """

    drive = archive_config.archive_device

    if drive is None:
        logging.warning("Az archiválás nem külső eszközre történik, de mégis annak leválasztása "
                        "volt kezdeményezve.")
        return

    logging.debug("%s eszköz kiadása.", drive)

    if drive is None:
        logging.warning("A biztonsági mentés nem távoli esztközre történik.")
        return

    run_command(["sudo", "eject", drive], global_config,
                error_message=f"A külső merevlemez ({drive}) leválasztása sikertelen.")


def reconnect(archive_config: ArchiveConfig, global_config: GlobalConfig) -> None:
    """
    Reconnect an external drive (if it is not already connected).

    :param ArchiveConfig archive_config: The archival configuration (contains the device).
    :param GlobalConfig global_config: The global configuration.
    """

    drive = archive_config.archive_device
    mount_folder = archive_config.mount_folder

    if drive is None or mount_folder is None:
        logging.warning("Az archiválás nem külső eszközre történik, de mégis annak csatlakoztatása"
                        " volt kezdeményezve.")
        return
    
    logging.debug("%s eszköz csatlakoztatása.", drive)

    r = run_command(["findmnt", drive, "-J"], global_config,
                    error_message=f"A merevlemez ({drive}) csatlakozottsági állapotának lekérése "
                    "sikertelen.", expected_returncodes=(1,), strict=False)
    if r.returncode == 0 and r.stdout:
        try:
            response = json.loads(r.stdout)
        except JSONDecodeError:
            logging.warning("A merevlemez (%s) csatlakozottsági állapotának lekérése "
                            "sikertelen. Az újracsatlakoztatás meg lesz kísérelve.", drive)
        else:
            if response["filesystems"]:
                logging.warning("A külső merevlemez (%s) az már csatlakoztatva van.", drive)
                return

    run_command(["sudo", "eject", "-t", drive], global_config,
                error_message=f"A külső merevlemez ({drive}) csatlakoztatása sikertelen.")
    if not Path(mount_folder).exists():
        run_command(["sudo", "mkdir", mount_folder], global_config,
                    error_message="A mount mappa létrehozása sikertelen.")
        # Path(mount_folder).mkdir(exist_ok=True)
    run_command(["sudo", "mount", drive, mount_folder], global_config,
                error_message="A mount művelet sikertelen.")


def archive(folder_properties: FolderProperties, freeup_needed: int = 0) -> None:
    """
    Archive a folder to an external deviccec.

    :param FolderProperties folder_properties: The configuration for the folder.
    :param int freeup_needed: The amount of space that needs to be freed by removing old files from
        the local storage, defaults to 0
    """

    config = folder_properties["config"]

    if config.archive_config is None:
        logging.warning("Archiválás volt kezdeményezve a %s mappára, de nincs eszköz megadva.",
                        config.folder_id)
        return

    logging.info("Archválás...")

    archive_config = config.archive_config

    if archive_config.archive_device is not None:
        reconnect(archive_config, config.global_config)

    try:
        sync_with_archive(config, freeup_needed)
    except:
        logging.error("Hiba történt az archiválás során: %s", traceback.format_exc())
    finally:
        if archive_config.archive_device is not None:
            eject(archive_config, config.global_config)


def sync_with_archive(config: FolderConfig, freeup_needed: int = 0):
    """
    Archive a folder. Should only be called by `archiver.archive`.

    :param FolderConfig config: The configuration for that folder.
    :param int freeup_needed: The amount of space that needs to be freed, defaults to 0
    """

    if config.archive_config is None:
        logging.warning("Archiválás volt kezdeményezve a %s mappára, de nincs megadva hozzá "
                        "konfiguráció.", config.folder_id)
        return

    archive_config = config.archive_config
    archive_config.archive_folder.mkdir(parents=True, exist_ok=True)

    global_files = update_all_files(config, return_directories=False)

    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)

        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, config)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_archived_files_path = dir_path.joinpath("sync.txt")
        deleted_files_path = dir_path.joinpath("deleted.txt")

        run_command(["rclone", "check", checkfile, archive_config.archive_folder,
                     "--checkfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_archived_files_path,
                     "--missing-on-src", deleted_files_path],
                    config.global_config,
                    error_message="Az archiválandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", strict=True,
                    expected_returncodes=(1,))

        copy_to_archive = read_path_list(differing_files_path, default=[])

        copy_to_archive += read_path_list(not_archived_files_path, default=[])

        delete_from_archive = read_path_list(deleted_files_path, default=[])

    local_files = get_files(config)

    not_matching_files = [file for file in copy_to_archive
                          if Path(file) not in local_files or file not in global_files
                          or not is_same_file(local_files[Path(file)], global_files[file], config)]

    try:
        discard_ignores(not_matching_files, config)
    except ChildProcessError:
        logging.error("Hiba történt a megváltozott fájlok újra figyelembevételekor.")

    delete_from_local = []
    freed_up_space = 0

    if config.local_keep_time is not None:
        t: tuple[Sequence[Path], Sequence[int]] = tuple(zip(*(
            (file, size) for file, (time, size) in local_files.items()
            if datetime.now(config.global_config.timezone) - time > config.local_keep_time
        ))) or ([], [0])  # type: ignore

        delete_from_local, freed_up_spaces = list(t[0]), t[1]
        freed_up_space = sum(freed_up_spaces)

    if freeup_needed < 0:
        logging.warning("A kért felszabadítandó tárhely mérete negatív (%d).", freeup_needed)
        freeup_needed = 0

    if freeup_needed:
        freed_up_space = 0

        for _, name, size in sorted((date, name, size)
                                    for name, (date, size) in local_files.items()):
            delete_from_local.append(name)
            freed_up_space += size

            if freed_up_space >= freeup_needed:
                break
        else:
            logging.warning("Nem lehetséges elegendő tárhely felszabadítása. Kért mennyiség: %d, "
                            "teljesíthető: %d", freeup_needed, freed_up_space)

    data_logger.log(config.global_config, copy_to_archive=copy_to_archive,
                    delete_from_archive=delete_from_archive, delete_from_local=delete_from_local)
    # Copy files to archive

    with NamedTemporaryFile(mode="w") as f:
        f.write("\n".join(copy_to_archive))
        f.flush()
        run_command(["rclone", "copy", "--files-from", f.name, config.local_folder,
                     archive_config.archive_folder], config.global_config,
                    error_message="hiba történt az archiválás során.", strict=False)

    # Delete archived and synced files

    if delete_from_local:
        with TemporaryDirectory() as tempdir:
            dir_path = Path(tempdir)
            missing = dir_path.joinpath("missing.txt")
            try:
                run_command(["rclone", "check", config.local_folder, config.remote_folder,
                            "--missing-on-dst", missing], config.global_config,
                            error_message="A törlendő lokális fájlok szinkronizáltságának "
                            "ellenőrzése sikertelen, a fájlok törlése kihagyára kerül.",
                            expected_returncodes=(1,))

                files = read_path_list(missing)

                if files:
                    logging.warning("Néhány fájl még nem került szinkronizálásra. "
                                    "Ezek törlése nem fog megtörténni.")
                    data_logger.log(config.global_config, files)

                    delete_from_local = set(map(str, delete_from_local)) - set(files)
            except (CalledProcessError, OSError, FileNotFoundError) as e:
                logging.warning("A %s hiba miatt a fájlok törlése nem fog megtörténni.",
                                e.__class__)
                delete_from_local = []

    if delete_from_local:
        try:
            extend_ignores(delete_from_local, config)
        except ChildProcessError:
            logging.error("A törlendő fájlok figyelmen kívül hagyása sikertelen, "
                          "a törlések nem fognak megtörténni.")
        else:
            with NamedTemporaryFile(mode="w") as f:
                f.write("\n".join(map(str, delete_from_local)))
                f.flush()
                run_command(["rclone", "move", "--files-from", f.name, config.local_folder,
                             archive_config.archive_folder], config.global_config,
                            error_message="A fájlok archívumba történő áthelyezése során hiba "
                            "történt.", strict=False)

    # Delete removed files from archive

    if delete_from_archive:
        with NamedTemporaryFile(mode="w") as f:
            f.write("\n".join(delete_from_archive))
            f.flush()
            run_command(["rclone", "move", "--files-from", f.name, archive_config.archive_folder,
                         config.trash_folder], config.global_config,
                        error_message="Hiba történt a törölt fájlok archívumból kukába helyezése "
                        "közben.", strict=False)
