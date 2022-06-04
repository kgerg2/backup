import logging
from pathlib import Path
from queue import Full, Queue
import subprocess
from tempfile import NamedTemporaryFile
from threading import Lock, Thread

import data_logger


class Uploader:

    def __init__(self, local_folder: Path, remote_folder: Path, uploaded_list_file: Path):
        self.upload_list_lock = Lock()
        self.upload_lock = Lock()
        self.upload_threads = Queue(2)
        self.files_to_upload = []
        self.local_folder = local_folder
        self.remote_folder = remote_folder
        self.upload_list_file = uploaded_list_file

        logging.debug("Feltöltésért felelős osztály inicializálva.")

    def get_command(self, command, path_to_list_of_files):
        return ["rclone", command, "--files-from", path_to_list_of_files, self.local_folder, self.remote_folder]

    def run_upload(self):
        logging.debug("Új feltöltés kezdeményezve. Várakozás az előző befejeztére...")
        self.upload_lock.acquire()

        with NamedTemporaryFile("w", suffix=".txt") as f:
            self.upload_list_lock.acquire()
            logging.debug("%d fájl feltöltése elkezdődik.", len(self.files_to_upload))
            files_str = "\n".join(self.files_to_upload)
            self.files_to_upload.clear()
            self.upload_list_lock.release()

            f.write(files_str)
            res = subprocess.run(self.get_command("copy", f.name), capture_output=True)

            if res.returncode != 0:
                logging.error("Fájlok feltöltése sikertelen volt.")
                data_logger.log(stdout=res.stdout, stderr=res.stderr, files=files_str)
            else:
                logging.info("Fájlok feltöltése sikeres (%d fájl, '%s' - '%s')",
                             files_str.count("\n") + 1,
                             files_str[:files_str.find("\n")],
                             files_str[files_str.rfind("\n"):])
                with open(self.upload_list_file, "a+") as f:
                    f.write(files_str)
                    f.write("\n")

        self.upload_lock.release()

    def run_move(self, files):
        logging.debug("Áthelyezés kezdeményezve a felhőtárhelyre. Várakozás...")
        self.upload_lock.acquire()

        with NamedTemporaryFile("w", suffix=".txt") as f:
            files_str = "\n".join(files)
            f.write(files_str)

            res = subprocess.run(self.get_command("move", f.name), capture_output=True)

            if res.returncode != 0:
                logging.error("Fájlok áthelyezése a felhőtárhelyre sikertelen.")
                data_logger.log(stdout=res.stdout, stderr=res.stderr, files=files_str)
            else:
                logging.info("Fájlok feltöltése sikeres (%d fájl, '%s' - '%s')",
                             len(files), files[0], files[-1])
                with open(self.upload_list_file, "a+") as f:
                    f.write(files_str)
                    f.write("\n")

        self.upload_lock.release()

    def delete_file(self, path):
        subprocess.run(["rclone", "deletefile", self.remote_folder.joinpath(path)])

    def delete_folder(self, path):
        subprocess.run(["rclone", "purge", self.remote_folder.joinpath(path)])

    def upload(self, file):
        self.upload_list_lock.acquire()
        self.files_to_upload.append(file)
        self.upload_list_lock.release()

        try:
            thread = Thread(target=self.run_upload)
            self.upload_threads.put_nowait(thread)
            thread.run()
        except Full:
            pass
