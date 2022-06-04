import logging
from multiprocessing import Process
from pathlib import Path
from pprint import pformat
from tempfile import NamedTemporaryFile, TemporaryDirectory

import data_logger
from uploader import Uploader
from util import (extend_file_info, get_file_details, get_file_info,
                  get_syncthing, read_path_list, run_command, update_file_info, write_checkfile)


API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
FOLDER_ID = "pfkxq-prxga"
FOLDER = Path("~/bmt").expanduser()
IGNORE_FILE = FOLDER.joinpath(".stignore")
BACKUP_FOLDER = Path("~/bmt-tavoli").expanduser()
REMOTE_FOLDER = Path("onedrive-kifu:bmt")

METADATA_FOLDER = FOLDER.joinpath(".backupdata")
BACKUP_FILE_LIST = METADATA_FOLDER.joinpath("backup-files.txt")
UPLOADED_FILES = METADATA_FOLDER.joinpath("onedrive-files.txt")

ALL_FILES = Path("~/bmt/")
CLOUD_ONLY_FILES = Path("~/bmt/")


def get_change(last_event=0, timeout=60):
    return get_syncthing("events/disk", {"since": last_event, "timeout": timeout})


sync_process = None
last_sync_event = 0


def check_sync():
    logging.debug("A szinkronizáló folyamat futásának ellenőrzése.")

    if sync_process is None:
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()

    if not sync_process.is_alive():
        logging.warning("A Syncthing szinkronizálásért felelős folyamat nem fut, úrjaindításra kerül. "
                        "Leállás hibakódja: %d", sync_process.exitcode)
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()


def sync_to_cloud():
    logging.debug("A bejövő módosítások figyelése indul.")

    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    while True:
        res = get_change(last_event=last_sync_event, timeout=3600)
        new_files = []
        deleted_files = []
        for change in res:
            logging.debug(pformat(change))
            last_sync_event = change["id"]
            if change["type"] == "RemoteChangeDetected":
                relpath = Path(change["data"]["path"]).relative_to(FOLDER)
                if change["data"]["action"] == "deleted":
                    if change["data"]["type"] == "file":
                        onedrive_uploader.delete_file(relpath)
                    elif change["data"]["type"] == "directory":
                        onedrive_uploader.delete_folder(relpath)
                    else:
                        logging.warning("Ismeretlen típusú törölt elem: %s", change["data"]["type"])
                        data_logger.log(change)
                        onedrive_uploader.delete_folder(relpath)
                    deleted_files.append(relpath)
                else:
                    onedrive_uploader.upload(relpath)
                    new_files.append(relpath)

        new_file_details = {str(path): get_file_details(FOLDER.joinpath(path))
                            for path in new_files}

        if deleted_files:
            all_info = get_file_info(ALL_FILES)
            for deleted in deleted_files:
                all_info.pop(deleted)
            all_info.update(new_file_details)
            update_file_info(ALL_FILES, all_info)
            with open(UPLOADED_FILES) as f:
                uploaded_files = set(f.read().splitlines())
            uploaded_files = uploaded_files - set(deleted_files)
            with open(UPLOADED_FILES) as f:
                for file in uploaded_files:
                    f.write(file)
                    f.write("\n")
        else:
            extend_file_info(ALL_FILES, new_file_details)


def sync_from_cloud():
    logging.debug("A felhőben végzett módosítások sinkronizálása.")
    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)

    files = get_file_info(ALL_FILES) | get_file_info(CLOUD_ONLY_FILES)
    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)
        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, files)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_uploaded_files_path = dir_path.joinpath("sync.txt")
        remotely_added_files = dir_path.joinpath("deleted.txt")

        run_command(["rclone", "check", checkfile, REMOTE_FOLDER,
                     "--checkcfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_uploaded_files_path,
                     "--missing-on-src", remotely_added_files],
                    error_message="A felhővel szinfronizálandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", expected_returncodes=(1,))
        
        bisync_files = read_path_list(differing_files_path, default=[])

        upload_files = read_path_list(not_uploaded_files_path, default=[])

        download_files = set(read_path_list(remotely_added_files, default=[]))

    if download_files:
        try:
            uploaded_files = set(read_path_list(UPLOADED_FILES))

            deletion_missed = download_files & uploaded_files
            download_files = download_files - deletion_missed

            if deletion_missed:
                logging.warning("%d fájl törlése elmaradt, pótlás most.", len(deletion_missed))
                data_logger.log(deletion_missed)
                for file in deletion_missed:
                    onedrive_uploader.delete_file(file)

            with NamedTemporaryFile() as f:
                f.write("\n".join(download_files))
                run_command(["rclone", "copy", REMOTE_FOLDER, FOLDER, "--files-from", f.name],
                            error_message="Új fájlok letöltése sikertelen.", strict=False)
        except (FileNotFoundError, OSError) as e:
            logging.error("Hiba történt a felhőben történt módosítások letöltése közben: %s", e)

    if upload_files:
        with NamedTemporaryFile() as f:
            f.write("\n".join(upload_files))
            run_command(["rclone", "copy", FOLDER, REMOTE_FOLDER, "--files-from", f.name],
                        error_message="Hiba történt az új fájlok feltöltése közben.", strict=False)

    if bisync_files:
        with NamedTemporaryFile() as f:
            f.write(f"+ {path}\n" for path in bisync_files)
            f.write("- *")
            run_command(["rclone", "bisync", FOLDER, REMOTE_FOLDER, "--filters-file", f.name,
                         "--resync"],
                        error_message="Hiba történt a különböző fájlok szinkronizálása közben.",
                        strict=False)
