import logging
from multiprocessing import Process
from pathlib import Path
from queue import Empty, Full, Queue
from tempfile import NamedTemporaryFile
from threading import Lock

from util import run_command


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

        try:
            with NamedTemporaryFile("w", suffix=".txt") as f:
                self.upload_list_lock.acquire()
                logging.debug("%d fájl feltöltése elkezdődik.", len(self.files_to_upload))
                files_str = "\n".join(self.files_to_upload)
                self.files_to_upload.clear()
                self.upload_list_lock.release()

                f.write(files_str)
                f.flush()
                res = run_command(self.get_command("copy", f.name) + ["-vv"],
                                error_message="Hiba történt a fájlok feltöltése közben.",
                                strict=False)

            if res.returncode == 0:
                logging.debug("Fájlok feltöltése sikeres (%d fájl, '%s' - '%s')",
                                files_str.count("\n") + 1,
                                files_str[:files_str.find("\n")],
                                files_str[files_str.rfind("\n")+1:])
                with open(self.upload_list_file, "a+", encoding="utf-8") as f:
                    f.write(files_str)
                    f.write("\n")

            try:
                self.upload_threads.get_nowait()
            except Empty:
                logging.warning("A feltöltő folyamatokat tartalmazó sorban nem található elem.")

            if self.upload_threads.empty() and self.files_to_upload:
                logging.warning("Feltöltést végző folyamat nem várakozik, pedig vannak feltöltendő "
                                "fájlok. Elindítása most megtörténik.")
                try:
                    thread = Process(target=self.run_upload)
                    self.upload_threads.put_nowait(thread)
                    thread.start()
                except Full:
                    pass
        finally:
            self.upload_lock.release()

    def run_move(self, files):
        logging.debug("Áthelyezés kezdeményezve a felhőtárhelyre. Várakozás...")
        self.upload_lock.acquire()

        with NamedTemporaryFile("w", suffix=".txt") as f:
            files_str = "\n".join(files)
            f.write(files_str)
            f.flush()
            res = run_command(self.get_command("move", f.name),
                              error_message="Hiba történt a fájlok feltöltése, áthelyezése közben.",
                              strict=False)

        if res.returncode == 0:
            logging.debug("Fájlok feltöltése sikeres (%d fájl, '%s' - '%s')",
                            len(files), files[0], files[-1])
            with open(self.upload_list_file, "a+", encoding="utf-8") as f:
                f.write(files_str)
                f.write("\n")

        self.upload_lock.release()

    def delete_file(self, path):
        logging.debug("Fájl törlése (%s).", path)
        
        run_command(["rclone", "deletefile", self.remote_folder.joinpath(path)],
                    error_message=f"Hiba történt a '{path}' fájl törlése közben.", strict=False)

    def delete_folder(self, path):
        logging.debug("Mappa törlése (%s).", path)

        run_command(["rclone", "purge", self.remote_folder.joinpath(path)],
                    error_message=f"Hiba történt a '{path}' mappa törlése közben.", strict=False)

    def upload(self, file):
        logging.debug("Fájl feltöltésre sorbaállítása (%s).", file)

        self.upload_list_lock.acquire()
        self.files_to_upload.append(str(file))
        self.upload_list_lock.release()

        try:
            thread = Process(target=self.run_upload)
            self.upload_threads.put_nowait(thread)
            thread.start()
        except Full:
            pass
