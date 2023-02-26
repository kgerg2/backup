import logging
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import AuthenticationError, Process, Queue
from multiprocessing.connection import Connection, Listener
from time import sleep
from typing import Any, Iterable, Optional

import multiprocessing_logging
from dateutil.relativedelta import relativedelta

from archiver import archive
from change_listener import SyncthingChangeListener, SyncthingChanges
from config import FolderConfig, GlobalConfig, FolderProperties, FolderUploaderQueue, UploaderQueue
from sync import UploadSyncer, sync_from_cloud
from trashhandler import handle_trash
from uploader import FolderUploader, Uploader
from util import retry_on_error


class TimedTask:
    def __init__(self,
                 name: str,
                 task: Callable,
                 time: datetime,
                 time_fields: list[str],
                 time_diff: relativedelta | timedelta,
                 max_delay: timedelta,
                 retry_time: relativedelta | timedelta,
                 args: Sequence[Any] = (),
                 max_retry_count: int = 10,
                 enabled: bool = True,
                 skip_if_running: bool = False,
                 for_all_folders=False):
        self.name = name
        if for_all_folders:
            self.task = do_for_all_folders
            self.args = [task] + list(args)
        else:
            self.task = task
            self.args: list[Any] = list(args)
        self.time = time
        self.time_fields = time_fields
        self.time_diff = time_diff
        self.max_delay = max_delay
        self.retry_time = retry_time
        self.max_retry_count = max_retry_count
        self.enabled = enabled
        self.skip_if_running = skip_if_running
        self.for_all_folders = for_all_folders

        self.process: Optional[Process] = None
        self.next_time: datetime = datetime.min
        self.retry_count: int = 0

    def get_next_scheduled(self):
        time = datetime.now().replace(**{field: getattr(self.time, field)
                                         for field in self.time_fields})
        maxiter = 10
        while time < datetime.now() and maxiter > 0:
            maxiter -= 1
            time += self.time_diff

        if maxiter == 0:
            logging.warning("A %s feladathoz megfelelő időpont meghatározása sikertelen. (érték: "
                            "%s)", self.name, time)

        return time

    def get_next_retry(self, scheduled_time):
        return scheduled_time + self.retry_time

    def check_if_ok_now(self, scheduled_time):
        return timedelta(0) <= datetime.now() - scheduled_time < self.max_delay


def process_message(msg: Any, conn: Connection, config: GlobalConfig,
                    folders: list[FolderProperties], uploader_queue: UploaderQueue,
                    processes: dict[str, dict[str, Any]]):
    def get_process_summary(process: Optional[Process]):
        if process is None:
            return "does not exist"

        return {
            "alive": process.is_alive(),
            "exit_code": process.exitcode
        }

    commands = {
        "archive": 0,
        "check_processes": 1,
        "download": 2,
        "trash": 3
    }

    match msg:
        case ["get", "config"]:
            conn.send(asdict(config))

        case ["get", "folders"]:
            conn.send([{
                "config": folder["config"].get_summary(),
                "folder_changes_queue_size": folder["folder_changes_queue"].qsize(),
                "uploader_queue_size": folder["uploader_queue"].qsize(),
                "uploader_process": get_process_summary(folder["uploader_process"]),
                "upload_syncer_process": get_process_summary(folder["upload_syncer_process"])
            } for folder in folders])

        case ["get", "uploader"]:
            conn.send({
                "uploader_process": get_process_summary(processes["uploader"].get("process", None)),
                "uploader_queue_size": uploader_queue.qsize()
            })

        case ["get", process] if process in processes:  # pylint: disable=used-before-assignment
            conn.send(get_process_summary(processes[process].get("process", None)))

        case ["start", process] if process in processes:
            try:
                start_process(processes[process])
            except ValueError:
                conn.send(f"{process} is running. Use restart to kill and start a new one.")
            else:
                conn.send("OK")

        case ["stop", process] if process in processes:
            try:
                stop_process(processes[process])
            except ValueError as e:
                conn.send(f"The process could not be stopped: {e}")
            else:
                conn.send("OK")

        case ["restart", process] if process in processes:
            try:
                stop_process(processes[process])
            except ValueError as e:
                conn.send(f"The process could not be stopped: {e}")
                return

            try:
                retry_on_error(start_process, retry_delay=10,
                               max_retry_count=12, args=[processes[process]])
            except ValueError as e:
                conn.send(f"The process could not be started: {e}")
            else:
                conn.send("OK")

        case ["run", "check_processes"]:
            restarted = check_processes(processes)
            if restarted:
                conn.send(f"{len(restarted)} processes were restarted: {', '.join(restarted)}")
            else:
                conn.send("All processes are running.")

        case ["run", "archive", folder, *args] \
                if any(folder == config["config"].folder_id for config in folders):
            folder_properties = next(config for config in folders 
                                     if folder == config["config"].folder_id)
            task_process = Process(target=archive,
                                   args=[folder_properties, *args])
            task_process.start()


        case ["run", task, *args] if task in commands:  # pylint: disable=used-before-assignment
            task = TASKS[commands[task]]
            if task.task is check_processes:
                task.args = [processes]
            if task.for_all_folders and folders not in task.args:
                task.args.append(folders)

            task_process = Process(target=task.task,
                                   args=task.args + args)
            task_process.start()

        case _:
            conn.send(f"Unrecognized request: {msg}")


def check_processes(processes: dict[str, dict[str, Any]]):
    restarted: list[str] = []
    for name, spec in processes.items():
        if "process" not in spec or not spec["process"].is_alive():
            logging.warning("Egy folyamat nem fut, ezért újraindításra kerül: %s", name)
            restarted.append(name)
            start_process(spec)

    return restarted

def do_for_all_folders(task: Callable[[FolderProperties], None], folders: Iterable[FolderProperties], *args) -> None:
    for folder in folders:
        task(folder, *args)


TASKS = (
    TimedTask("archiválás",
              archive,
              datetime(2000, 1, 1, 0, 0, 0),
              ["day", "hour", "minute", "second"],
              relativedelta(months=1),
              timedelta(hours=4),
              timedelta(days=1),
              for_all_folders=True),
    TimedTask("folyamatok ellenőrzése",
              check_processes,
              datetime(2000, 1, 1, 1, 0, 0),
              ["hour", "minute", "second"],
              timedelta(days=1),
              timedelta(hours=4),
              timedelta(hours=1),
              skip_if_running=True),
    TimedTask("letöltés",
              sync_from_cloud,
              datetime(2000, 1, 1, 23, 0, 0),
              ["hour", "minute", "second"],
              timedelta(days=1),
              timedelta(hours=2),
              timedelta(hours=1),
              for_all_folders=True),
    TimedTask("lomtalanítás",
              handle_trash,
              datetime(2000, 1, 5, 10, 0, 0),
              ["day", "hour", "minute", "second"],
              relativedelta(months=1),
              timedelta(hours=24),
              timedelta(days=1),
              for_all_folders=True),
)


def start_main_loop(processes: dict[str, dict[str, Any]],
                    folders: list[FolderProperties]):
    """
    Calls the tasks at the appropriate times.
    """

    for task in TASKS:
        task.next_time = task.get_next_scheduled()
        task.process = None
        task.retry_count = 0
        if task.task is check_processes:
            task.args = [processes]
        if task.for_all_folders and folders not in task.args:
            task.args.append(folders)

    while True:
        task = min(TASKS, key=lambda x: x.next_time)

        if task.process is not None and task.process.exitcode:
            task.retry_count += 1
        else:
            task.retry_count = 0

        if task.retry_count > task.max_retry_count:
            logging.error("A(z) %s feladat futtatása során az újrapróbálkozások száma "
                          "meghaladta a megadott értéket ezért deaktiválásra került.", task.name)
            task.enabled = False

        if not task.enabled:
            logging.warning("A(z) %s feladat deaktiválva van ezért nem kerül futtatásra.",
                            task.name)
            continue

        seconds_until_task = (task.next_time - datetime.now()).total_seconds()
        if seconds_until_task > 0:
            sleep(seconds_until_task)

        if not task.check_if_ok_now(task.next_time):
            task.next_time = task.get_next_retry(task.next_time)
            logging.info("A(z) %s feladat a futtatása lekéste a megfelelő intervallumot. "
                         "Újrapróbálás ideje: %s", task.name, task.next_time.isoformat())
            task.retry_count += 1
            continue

        if task.process is not None and task.process.is_alive():
            if task.skip_if_running:
                task.next_time = task.get_next_scheduled()
                task.retry_count = 0
            else:
                task.next_time = task.get_next_retry(task.next_time)
                logging.info("A(z) %s feladat előző futtatása még nem fejeződött be, "
                             "ezért most nem indult el újra. Következő újrapróbálás ideje: %s",
                             task.name, task.next_time.isoformat())
                task.retry_count += 1
            continue

        if task.skip_if_running:
            logging.warning("A(z) %s feladat futása megszakadt. Újraindítás most...", task.name)

        try:
            task.process = Process(target=task.task, args=task.args)
            logging.debug("Feladat indítása: %s", task.name)
            task.process.start()
        except:
            logging.error("Hiba történt a feladat (%s) elindítása során.", task.name)
            task.next_time = task.get_next_retry(task.next_time)
            raise
        else:
            task.next_time = task.get_next_scheduled()


def start_process(spec):
    if "process" in spec and spec["process"].is_alive():
        raise ValueError("Cannot start process, it is already running.")

    spec.pop("process", None)
    spec["process"] = Process(**spec)
    spec["process"].start()


def stop_process(spec):
    if "process" not in spec:
        raise ValueError("The process does not exist.")

    if not spec["process"].is_alive():
        raise ValueError("The process is not running.")

    spec["process"].terminate()


def main():
    global_config = GlobalConfig.read_from_file("test-configs/global_config.json")

    global_config.logging_file.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        format="%(asctime)s|%(levelname)s|%(filename)s:%(funcName)s(%(lineno)d)|%(message)s",
        level=logging.DEBUG,
        handlers=(TimedRotatingFileHandler(global_config.logging_file, when="midnight"),))

    multiprocessing_logging.install_mp_handler()

    logging.debug("Program indul.")

    uploader_queue: UploaderQueue = Queue(maxsize=1000)  # type: ignore

    processes = {
        "uploader": {
            "target": Uploader,
            "args": [global_config, uploader_queue],
        }
    }

    start_process(processes["uploader"])

    folders: list[FolderProperties] = []
    for file in global_config.folder_configs.iterdir():
        config = FolderConfig.read_from_file(file, global_config)
        folder_uploader_queue: FolderUploaderQueue = Queue(maxsize=1000)  # type: ignore
        uploader_process = Process(target=FolderUploader,
                                   args=[config, folder_uploader_queue, uploader_queue])
        uploader_process.start()
        folder_changes_queue: "Queue[SyncthingChanges]" = Queue(maxsize=1000)
        upload_syncer_process = Process(target=UploadSyncer,
                                        args=[folder_changes_queue, folder_uploader_queue, config])
        upload_syncer_process.start()

        folders.append({
            "config": config,
            "uploader_queue": folder_uploader_queue,
            "uploader_process": uploader_process,
            "folder_changes_queue": folder_changes_queue,
            "upload_syncer_process": upload_syncer_process
        })

    processes["change_listener"] = {
        "target": SyncthingChangeListener,
        "args": [global_config, [f["folder_changes_queue"] for f in folders]]
    }
    start_process(processes["change_listener"])

    timed_tasks_process = Process(target=start_main_loop, args=[processes, folders])
    timed_tasks_process.start()

    return retry_on_error(run_message_server,
                          args=(global_config, uploader_queue, processes, folders),
                          error_message="Az üzenetfogadás során hiba történt.")

def run_message_server(global_config: GlobalConfig, uploader_queue: UploaderQueue,
                       processes: dict[str, dict[str, Any]], folders: list[FolderProperties]):
    logging.debug("Üzenetfogadás elindul.")

    listener = Listener(global_config.message_listener_address,
                        authkey=global_config.message_listener_auth_token)

    while True:
        try:
            try:
                conn = listener.accept()
            except AuthenticationError:
                logging.warning("Csatlakozási kísérlet elutasítva a hibás azonosító miatt.")
                continue

            logging.info("Csatlakozás az archiválóhoz: %s", listener.last_accepted)
            while True:
                try:
                    msg = conn.recv()
                except EOFError:
                    break

                logging.debug("Üzenet %s-tól: %s", listener.last_accepted, msg)

                if msg == 'close server':
                    conn.close()
                    listener.close()
                    return

                process_message(msg, conn, global_config, folders, uploader_queue, processes)
        except EOFError:
            logging.warning("Hibás fogadott üzenet.")

if __name__ == "__main__":
    main()
