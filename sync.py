import logging
from multiprocessing import Process
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile, TemporaryDirectory
import traceback
from archiver import update_all_files

import data_logger
from config import (ALL_FILES, CLOUD_ONLY_FILES, DOWNLOADED_FILES, LAST_SYNC_EVENT_FILE,
                    LOCAL_FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
from uploader import Uploader
from util import (discard_ignores, extend_file_info, get_file_details, get_file_info, get_remote_mod_times,
                  get_syncthing, read_path_list, run_command, update_file_info,
                  write_checkfile)


def get_change(last_event=0, timeout=60):
    return get_syncthing("events/disk", {"since": last_event, "timeout": timeout})


sync_process = None
last_sync_event = 0


def check_sync():
    global sync_process

    logging.debug("A szinkronizáló (%s) folyamat futásának ellenőrzése.", sync_process)

    if sync_process is None:
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()

    if not sync_process.is_alive():
        logging.warning("A Syncthing szinkronizálásért felelős folyamat nem fut, úrjaindításra kerül. "
                        "Leállás hibakódja: %d", sync_process.exitcode)
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()


def get_last_sync_event():
    try:
        with open(LAST_SYNC_EVENT_FILE, encoding="utf-8") as f:
            found = int(f.read())
    except:
        return 0
    
    if found <= 0:
        return 0

    last_changes = get_change(last_event=found - 1, timeout=5)

    if not last_changes:
        return 0

    return found


def sync_to_cloud():
    global last_sync_event

    logging.debug("A bejövő módosítások figyelése indul.")

    last_sync_event = get_last_sync_event()

    onedrive_uploader = Uploader(LOCAL_FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    while True:
        res = get_change(last_event=last_sync_event, timeout=3600)
        new_files = []
        deleted_files = []
        for change in res:
            logging.debug(pformat(change))
            last_sync_event = change["id"]
            if change["type"] == "RemoteChangeDetected":
                relpath = change["data"]["path"]
                if change["data"]["action"] == "deleted":
                    if change["data"]["type"] == "file":
                        onedrive_uploader.delete_file(relpath)
                    elif change["data"]["type"] in ("directory", "dir"):
                        onedrive_uploader.delete_folder(relpath)
                    else:
                        logging.warning("Ismeretlen típusú törölt elem: %s", change["data"]["type"])
                        data_logger.log(change)
                        onedrive_uploader.delete_folder(relpath)
                    deleted_files.append(relpath)
                else:
                    onedrive_uploader.upload(relpath)
                    new_files.append(relpath)
            if change["type"] == "file" and change["data"]["action"] == "modified":
                onedrive_uploader.upload(relpath)
                new_files.append(relpath)

        new_file_details = {str(path): get_file_details(LOCAL_FOLDER.joinpath(path))
                            for path in new_files if LOCAL_FOLDER.joinpath(path).exists()}

        if deleted_files:
            all_info = get_file_info(ALL_FILES)
            for deleted in deleted_files:
                try:
                    all_info.pop(deleted)
                except KeyError:
                    logging.warning("Egy fájl törölve lett, de nem szerepel az összes között: %s",
                                    deleted)
            all_info.update(new_file_details)
            update_file_info(ALL_FILES, all_info)
        else:
            extend_file_info(ALL_FILES, new_file_details)

        with open(LAST_SYNC_EVENT_FILE, "w", encoding="utf-8") as f:
            f.write(str(last_sync_event))


def sync_from_cloud():
    logging.debug("A felhőben végzett módosítások sinkronizálása.")
    onedrive_uploader = Uploader(LOCAL_FOLDER, REMOTE_FOLDER, UPLOADED_FILES)

    files = update_all_files(return_directories=False) | get_file_info(CLOUD_ONLY_FILES)
    logging.debug("Fájlok meghatározása sikeres.")
    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)
        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, files)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_uploaded_files_path = dir_path.joinpath("sync.txt")
        remotely_added_files = dir_path.joinpath("deleted.txt")

        run_command(["rclone", "check", checkfile, REMOTE_FOLDER,
                     "--checkfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_uploaded_files_path,
                     "--missing-on-src", remotely_added_files],
                    error_message="A felhővel szinfronizálandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", expected_returncodes=(1, 3))
        
        bisync_files = read_path_list(differing_files_path, default=[])

        upload_files = read_path_list(not_uploaded_files_path, default=[])

        download_files = set(read_path_list(remotely_added_files, default=[]))

    remote_times = get_remote_mod_times(bisync_files)
    for file in bisync_files:
        if remote_times[file][0] > files[file][1]:
            download_files.add(file)
        else:
            upload_files.append(file)

    if download_files:
        try:
            uploaded_files = set(read_path_list(UPLOADED_FILES, default=[]))
            downloaded_files = set(read_path_list(DOWNLOADED_FILES, default=[]))

            deletion_missed = download_files & (uploaded_files | downloaded_files)
            download_files = download_files - deletion_missed

            if deletion_missed:
                logging.warning("%d fájl törlése elmaradt, pótlás most.", len(deletion_missed))
                data_logger.log(deletion_missed)
                onedrive_uploader.delete_files(deletion_missed)

            with NamedTemporaryFile(mode="w") as f:
                f.write("\n".join(download_files))
                f.flush()
                r = run_command(["rclone", "copy", REMOTE_FOLDER, LOCAL_FOLDER, "--files-from", f.name],
                                error_message="Új fájlok letöltése sikertelen.", strict=False)

            if r.returncode == 0:
                with open(DOWNLOADED_FILES, "a+") as f:
                    f.writelines(f"{file}\n" for file in download_files)
        except (FileNotFoundError, OSError) as e:
            logging.error("Hiba történt a felhőben történt módosítások letöltése közben: %s", traceback.format_exc())

    data_logger.log(bisync_files=bisync_files, upload_files=upload_files, download_files=download_files)

    if upload_files:
        discard_ignores(upload_files)
        onedrive_uploader.upload(*upload_files)
