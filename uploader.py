import logging
from collections.abc import Iterable
from multiprocessing import Queue  # pylint: disable=unused-import
from pathlib import Path
from queue import Empty
from tempfile import NamedTemporaryFile
from traceback import format_exc
from typing import NoReturn, Optional

from sqlalchemy import select, update
from sqlalchemy.orm import Session

import data_logger
from config import (AllFiles, FolderConfig, FolderUploaderQueue, GlobalConfig,
                    UploadAction, UploaderAction, UploaderQueue)
from util import retry_on_error, run_command


class Uploader:
    """
    Class responsible for performing file uploads. Only one should be used.
    """

    ACTIONS = ("copy", "move")

    def __init__(self, global_config: GlobalConfig, queue: UploaderQueue) -> None:
        self.queue = queue
        self.global_config = global_config

        logging.debug("Feltöltésért felelős osztály inicializálva.")
        retry_on_error(self.listen,
                       error_message="Hiba történt feltöltés közben és a folamat leállt.")

    def listen(self) -> NoReturn:
        """
        Gets a task from the queue and performs it.
        """

        while True:
            args = self.queue.get()
            try:
                self.upload(*args)
            except:
                logging.error("Feltöltés közben hiba történt. (Argumentumok: %s) %s", args,
                              format_exc())

    def upload(self, paths: Iterable[Path | str], action: UploadAction, local_folder: Path | str, remote_folder: Path | str) \
            -> None:
        """
        Runs the upload (copy or move) and sets the uploaded version date to the time of the
        last modification.
        """

        paths = [str(path) for path in paths]

        if not paths:
            logging.warning("Feltöltés volt kezdeményezve, de feltöltendő fájlok nem kerültek "
                            "megadásra.")
            return

        if len(paths) == 1:
            logging.debug("'%s' feltöltése elkezdődik", paths[0])
        else:
            logging.debug("%d fájl feltöltése elkezdődik ('%s' - '%s')",
                          len(paths), paths[0], paths[-1])

        with NamedTemporaryFile("w", suffix=".txt") as f:
            files_str = "\n".join(paths)
            f.write(files_str)
            f.flush()

            run_command(["rclone", action, "--files-from", f.name, local_folder,
                         remote_folder], self.global_config,
                        error_message="Hiba történt a fájlok feltöltése közben.")

        if len(paths) == 1:
            logging.debug("'%s' feltöltése sikeres", paths[0])
        else:
            logging.debug("%d fájl feltöltése sikeres ('%s' - '%s')",
                          len(paths), paths[0], paths[-1])




class FolderUploader:
    """
    Class responsible for collections all the uploads needed for a folder and forwarding them to
    the `Uploader` through the `uploader_queue`.
    """

    def __init__(self, config: FolderConfig,
                 queue: FolderUploaderQueue,
                 uploader_queue: UploaderQueue) -> None:
        self.config = config
        self.uploader_queue = uploader_queue
        self.queue = queue

        logging.debug("'%s' mappából '%s' mappába való feltöltésére felelős osztály "
                      "inicializálva.", config.local_folder, config.remote_folder)
        retry_on_error(self.listen,
                       error_message="Hiba történt egy mappa feltöltéseinek kezelése közben.")

    def listen(self) -> NoReturn:
        """
        Waits for a task to be placed into the queue and if it is in `Uploader.ACTIONS`, tries to
        collect more of them by waiting at most 10 seconds.
        Then performs the action with `self.perform_action`.

        :return NoReturn: does not return
        """

        collect_files: list[Path | str] = []
        collect_action: Optional[UploaderAction] = None

        while True:
            try:
                files, action = self.queue.get(block=True, timeout=10)

                if action == collect_action:
                    collect_files.extend(files)
                    continue

                if collect_action is not None:
                    self.perform_action(collect_action, collect_files)
            except Empty:
                if collect_action is not None:
                    self.perform_action(collect_action, collect_files)

                files, action = self.queue.get()

            if action in Uploader.ACTIONS:
                collect_action = action
                collect_files = list(files)
                continue

            self.perform_action(action, files)
            collect_action = None
            collect_files.clear()

    def perform_action(self, action: UploaderAction, files: Iterable[Path | str]) -> None:
        """
        If the action is a deletion, performs it directly. Otherwise forwards it with
        `self.upload`.

        :param UploaderAction action: the action to be performed (copy, move or deletion)
        :param Iterable[Path | str] files: the files or folders
        """

        match action:
            case "delete_files":
                try:
                    self.delete_files(files)
                except:
                    data_logger.log(self.config.global_config, failed_to_delete_files=files)
                    logging.error("Hiba történt fájlok törlése közben: %s", format_exc())

            case "delete_folders":
                try:
                    self.delete_folders(files)
                except:
                    data_logger.log(self.config.global_config, failed_to_delete_folders=files)
                    logging.error("Hiba történt mappák törlése közben: %s", format_exc())

            case _ if action in Uploader.ACTIONS:
                self.upload(files, action)

            case _:
                logging.error("Hibás kérés fájlok feltöltésére: %s", action)

    def upload(self, collect_files: Iterable[Path | str], action: UploadAction) -> None:
        """
        Filters out the unuploadable files with `self.check_file` and forwards the rest through
        the `self.uploader_queue`.

        :param Iterable[Path | str] collect_files: the files to be uploaded
        :param UploadAction action: the upload action
        """
        logging.debug("Feltöltendő fájlok szűrés előtt: %s", collect_files)
        filtered_files = [file for file in collect_files if self.check_file(file)]
        self.uploader_queue.put((filtered_files, action, self.config.local_folder,
                                 self.config.remote_folder))

        with Session(self.config.database) as session:
            update_stmt = update(AllFiles) \
                                  .where(AllFiles.path.in_([str(f) for f in filtered_files])) \
                                  .values(uploaded=AllFiles.modified)
            logging.debug("SQL parancs futtatása: %s", update_stmt)
            session.execute(update_stmt)
            session.commit()

    def delete_files(self, paths: Iterable[Path | str]) -> None:
        """
        Deletes files from the remote folder.

        :param Iterable[Path | str] paths: the files to be deleted
        """

        paths = [str(path) for path in paths]
        logging.debug("Fájlok törlése (%d db).", len(paths))
        data_logger.log(self.config.global_config, paths)

        with NamedTemporaryFile("w", suffix=".txt") as f:
            f.write("\n".join(paths))
            f.flush()
            run_command(["rclone", "delete", self.config.remote_folder, "--files-from", f.name],
                        self.config.global_config,
                        error_message="Hiba történt a fájlok törlése közben.")

        # with Session(self.config.database) as session:
        #     delete_stmt = delete(AllFiles).where(AllFiles.path.in_(paths))
        #     logging.debug("SQL parancs futtatása: %s", delete_stmt)
        #     session.execute(delete_stmt)
        #     session.commit()

    def delete_folders(self, paths: Iterable[Path | str]) -> None:
        """
        Deletes directories from the remote folder.

        :param Iterable[Path | str] paths: the paths of the directories
        """

        for path in paths:
            self.delete_folder(path)

    def delete_folder(self, path: Path | str) -> None:
        """
        Deletes a directory from the remote folder. The deletion will fail if any cloud only file
        is inside that directcory.

        :param Path | str path: the directory to be deleted
        """

        logging.debug("Mappa törlése: '%s'.", path)

        with Session(self.config.database) as session:
            exists_stmt = select(AllFiles).where(AllFiles.is_relative_to(str(path))
                                                 ).where(AllFiles.cloud_only).exists()
            logging.debug("SQL parancs futtatása: %s", exists_stmt)
            if session.query(exists_stmt).scalar():
                logging.warning("A mappa törlése nem lehetséges a csak felhőbeli fájlok miatt.")
                return

            run_command(["rclone", "purge", self.config.remote_folder.joinpath(path)],
                        self.config.global_config,
                        error_message=f"Hiba történt a '{path}' mappa törlése közben.",
                        strict=False)

            # delete_stmt = delete(AllFiles).where(AllFiles.is_relative_to(path))
            # logging.debug("SQL parancs futtatása: %s", delete_stmt)
            # session.execute(delete_stmt)
            # session.commit()

    @staticmethod
    def check_file(file: str | Path) -> bool:
        """
        Decides whether a file can be uploaded or not.

        :param str | Path file: the path of the file
        :return bool: whether the file can be uploaded
        """

        file_str = str(file)

        if "_files/" in file_str or file_str.endswith("_files"):
            return False

        return True
