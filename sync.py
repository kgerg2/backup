import logging
import traceback
from multiprocessing import Queue
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import NoReturn

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

import data_logger
from archiver import update_all_files
from change_listener import SyncthingChanges
from config import AllFiles, FolderConfig, FolderProperties, FolderUploaderQueue, UploaderAction
from util import (discard_ignores, extend_ignores, get_file_details, get_remote_mod_times,
                  read_path_list, retry_on_error, run_command, write_checkfile)


class UploadSyncer:
    """
    Class responsible for deciding which actions should be taken based on `SyncthingChanges` and
    also making the corresponding modifications in the `AllFiles` database.
    """

    def __init__(self, changes_queue: "Queue[SyncthingChanges]",
                 upload_queue: FolderUploaderQueue, config: FolderConfig) -> None:
        self.changes_queue = changes_queue
        self.upload_queue = upload_queue
        self.config = config
        retry_on_error(self.listen,
                       error_message="Hiba történt a Syncthing változtatások továbbítása során.")

    def listen(self) -> NoReturn:
        """
        Gets `SyncthingChanges` from `self.changes_queue` and processes them if they belong to the
        folder of this class by putting them into `self.upload_queue`:
          - remote file deletions
          - remote directory deletions
          - remote file modifications (includes addition)
          - local file modifications (includes addition)

        Local deletions are ignored, these paths are added to the Syncthing ignores.

        All changes get mirrored to the `AllFiles` database.

        :return NoReturn: function does not return
        """

        while True:
            changes = self.changes_queue.get()
            actions: dict[UploaderAction, list[str]] = {
                "copy": [],
                "delete_files": [],
                "delete_folders": []
            }

            for change in changes:
                if change["data"]["folder"] != self.config.folder_id:
                    continue

                match change:
                    case {"type": "RemoteChangeDetected",
                          "data": {"action": "deleted", "path": path, "type": "file"}} | \
                            {"type": "LocalChangeDetected",
                              "data": {"action": "deleted", "path": path, "type": "file"}}:

                        actions["delete_files"].append(path)

                    case {"type": "RemoteChangeDetected",
                          "data": {"action": "deleted", "path": path, "type": "dir"}} | \
                            {"type": "LocalChangeDetected",
                              "data": {"action": "deleted", "path": path, "type": "dir"}}:

                        actions["delete_folders"].append(path)

                    case {"type": "RemoteChangeDetected",
                          "data": {"action": "modified", "path": path}} | \
                            {"type": "LocalChangeDetected",
                             "data": {"action": "modified", "path": path}}:

                        actions["copy"].append(path)

                    case other:

                        logging.warning("Ismeretlen változás a Syncthing üzenetben: %s", other)

            if not any(actions.values()):
                continue

            for action, paths in actions.items():
                if not paths:
                    continue

                self.upload_queue.put((paths, action))

            with Session(self.config.database) as session:
                if actions["delete_files"] or actions["delete_folders"]:
                    delete_stmt = delete(AllFiles) \
                        .where((AllFiles.path.in_(actions["delete_files"]) |
                               AllFiles.is_relative_to_any(actions["delete_folders"]))  # pylint: disable=no-value-for-parameter
                               & ~AllFiles.cloud_only)
                    logging.debug("SQL parancs futtatása: %s", delete_stmt)
                    session.execute(delete_stmt)

                if actions["copy"]:
                    delete_stmt = delete(AllFiles).where(AllFiles.path.in_(actions["copy"]))
                    session.execute(delete_stmt)
                    session.add_all(AllFiles(path=path,
                                             **dict(zip(("hash", "modified", "size"),
                                                        get_file_details(Path(path), self.config))))
                                    for path in actions["copy"])

                session.commit()


def sync_from_cloud(folder_properties: FolderProperties):
    """
    Function responsible for making sure the the remote folder is in sync with the Syncthing global
    database by downloading, uploading and deleting the necessary files.

    :param FolderUploaderQueue uploader_queue: the uploader queue for the folder
    :param FolderConfig config: the config for the folder
    """

    config = folder_properties["config"]
    uploader_queue = folder_properties["uploader_queue"]

    logging.debug("A felhőben végzett módosítások sinkronizálása a %s mappában.", config.folder_id)

    files = update_all_files(config, return_directories=False)

    logging.debug("Fájlok meghatározása sikeres.")
    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)
        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, config)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_uploaded_files_path = dir_path.joinpath("sync.txt")
        remotely_added_files = dir_path.joinpath("deleted.txt")

        run_command(["rclone", "check", checkfile, config.remote_folder,
                     "--checkfile", "QuickXorHash",
                     "--differ", differing_files_path,
                     "--missing-on-dst", not_uploaded_files_path,
                     "--missing-on-src", remotely_added_files],
                    config.global_config,
                    error_message="A felhővel szinfronizálandó fájlok meghatározása nem sikerült, "
                    "az összehasonlítás meghiúsult.", expected_returncodes=(1, 3))

        bisync_files = read_path_list(differing_files_path, default=[])

        upload_files = read_path_list(not_uploaded_files_path, default=[])

        download_files = set(read_path_list(remotely_added_files, default=[]))

    remote_times = get_remote_mod_times(bisync_files, config)
    for file in bisync_files:
        if remote_times[file][0] > files[file][1]:
            download_files.add(file)
        else:
            upload_files.append(file)

    if download_files:
        try:
            with Session(config.database) as session:
                select_stmt = select(AllFiles.path).where(AllFiles.uploaded.is_not(None))
                logging.debug("SQL parancs futtatása: %s", select_stmt)
                uploaded_files = session.scalars(select_stmt)

            deletion_missed = download_files & set(uploaded_files)
            download_files = download_files - deletion_missed

            if deletion_missed:
                logging.warning("%d fájl törlése elmaradt, pótlás most.", len(deletion_missed))
                data_logger.log(config.global_config, deletion_missed)
                uploader_queue.put((deletion_missed, "delete_files"))

            with NamedTemporaryFile(mode="w") as f:
                f.write("\n".join(download_files))
                f.flush()
                r = run_command(["rclone", "copy", config.remote_folder, config.local_folder,
                                 "--files-from", f.name],
                                config.global_config,
                                error_message="Új fájlok letöltése sikertelen.",
                                strict=False)

            if r.returncode == 0:
                with Session(config.database) as session:
                    update_stmt = update(AllFiles) \
                        .where(AllFiles.path.in_(tuple(download_files))) \
                        .values(uploaded=AllFiles.modified)
                    logging.debug("SQL parancs futtatása: %s", update_stmt)
                    session.execute(update_stmt)
                    session.commit()

        except (FileNotFoundError, OSError):
            logging.error("Hiba történt a felhőben történt módosítások letöltése közben: %s",
                          traceback.format_exc())

    data_logger.log(config.global_config, bisync_files=bisync_files, upload_files=upload_files,
                    download_files=download_files)

    if upload_files:
        discard_ignores(upload_files, config)
        uploader_queue.put((upload_files, "copy"))
