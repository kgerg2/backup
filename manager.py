import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import Process
from multiprocessing.connection import Listener
from time import sleep
from typing import Callable, List, Optional, Union

from dateutil.relativedelta import relativedelta

from archiver import archive
from config import (FAILURE_EXPIRY_DAYS, LOGGING_FILE, MAX_FAILURES_PER_DAY,
                    MAX_FAILURES_PER_HOUR, MESSAGE_LISTENER_ADDRESS, MESSAGE_LISTENER_AUTH_TOKEN)
from sync import check_sync, sync_from_cloud, sync_to_cloud
from trashhandler import handle_trash


class TimedTask:
    def __init__(self,
                 name: str,
                 task: Callable,
                 time: datetime,
                 time_fields: List[str],
                 time_diff: Union[relativedelta, timedelta],
                 max_delay: timedelta,
                 retry_time: Union[relativedelta, timedelta],
                 max_retry_count: int = 10,
                 enabled: bool = True,
                 skip_if_running: bool = False):
        self.name = name
        self.task = task
        self.time = time
        self.time_fields = time_fields
        self.time_diff = time_diff
        self.max_delay = max_delay
        self.retry_time = retry_time
        self.max_retry_count = max_retry_count
        self.enabled = enabled
        self.skip_if_running = skip_if_running

        self.process: Optional[Process] = None

    #     self.next_time = None
    #     self.process = None
    #     self.retry_count = 0

    # def reset_variables(self):
    #     self.next_time = None
    #     self.process = None
    #     self.retry_count = 0

    def get_next_scheduled(self):
        time = datetime.now().replace(**{field: getattr(self.time, field) for field in self.time_fields})
        maxiter = 10
        while time < datetime.now() and maxiter > 0:
            maxiter -= 1
            time += self.time_diff

        if maxiter == 0:
            logging.warning("A %s feladathoz megfelelő időpont meghatározása sikertelen. (érték: %s)", self.name, time)

        return time

    def get_next_retry(self, scheduled_time):
        return scheduled_time + self.retry_time

    def check_if_ok_now(self, scheduled_time):
        return timedelta(0) <= datetime.now() - scheduled_time < self.max_delay


def message_server():
    logging.debug("Üzenetfogadásért felelős folyamat elindul.")
    
    commands = {
        "archive": 0,
        "check_upload": 1,
        "upload": sync_to_cloud,
        "download": 2,
        "trash": 3
    }
    listener = Listener(MESSAGE_LISTENER_ADDRESS, authkey=MESSAGE_LISTENER_AUTH_TOKEN)

    while True:
        conn = listener.accept()
        logging.info("Csatlakozás az archiválóhoz: %s", listener.last_accepted)
        while True:
            try:
                msg = conn.recv()
            except EOFError:
                break

            logging.debug("Üzenet %s-tól: %s", listener.last_accepted, msg)
            if msg in commands:
                if isinstance(commands[msg], int):
                    task = TASKS[commands[msg]]
                    if task.process is None:
                        logging.debug("Feladat indítása először: %s", task.name)
                        task.process = Process(target=task.task)
                        task.process.start()

                    elif not task.process.is_alive():
                        logging.debug("Feladat indítása: %s", task.name)
                        task.process = Process(target=task.task)
                        task.process.start()

                        if task.skip_if_running:
                            logging.warning("A %s futtatása megszakadt, most újraindításra "
                                            "került.", task.name)
                else:
                    logging.debug("Feladat futtatása: %s", msg)
                    Process(target=commands[msg]).start()
            # if msg == 'close connection':
            #     conn.close()
            #     break
            if msg == 'close server':
                conn.close()
                listener.close()
                return

    listener.close()


TASKS = (
    TimedTask("archiválás",
              archive,
              datetime(2000, 1, 1, 0, 0, 0),
              ["day", "hour", "minute", "second"],
              relativedelta(months=1),
              timedelta(hours=4),
              timedelta(days=1)),
    TimedTask("feltöltés ellenőrzése",
              sync_to_cloud,
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
              timedelta(hours=1)),
    TimedTask("lomtalanítás",
              handle_trash,
              datetime(2000, 1, 5, 10, 0, 0),
              ["day", "hour", "minute", "second"],
              relativedelta(months=1),
              timedelta(hours=24),
              timedelta(days=1)),
    TimedTask("üzenetfogadás ellenőrzése",
              message_server,
              datetime(2000, 1, 1, 1, 0, 0),
              ["hour", "minute", "second"],
              timedelta(days=1),
              timedelta(hours=4),
              timedelta(hours=1),
              skip_if_running=True)
)


def start_main_loop() -> bool:
    """
    Calls the tasks at the appropriate times.

    :return bool: the program should continue
    """

    for task in TASKS:
        task.next_time = task.get_next_scheduled()
        task.process = None
        task.retry_count = 0

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
            task.process = Process(target=task.task)
            logging.debug("Feladat indítása: %s", task.name)
            task.process.start()
        except:
            logging.error("Hiba történt a feladat (%s) elindítása során.", task.name)
            task.next_time = task.get_next_retry(task.next_time)
            raise
        else:
            task.next_time = task.get_next_scheduled()


def main():
    global message_server_process

    logging.basicConfig(format="%(asctime)s|%(levelname)s|%(filename)s:%(funcName)s(%(lineno)d)|%(message)s",
                        level=logging.DEBUG,
                        handlers=(TimedRotatingFileHandler(LOGGING_FILE, atTime="midnight"),))
    
    logging.debug("Program indul.")

    message_server_process = Process(target=message_server)
    message_server_process.start()

    end = False
    failures = []
    while not end:
        try:
            end = not start_main_loop()
        except Exception as e:
            logging.error("Hiba történt a program futása során: %s", e)
            current_time = datetime.now()

            failures = [time for time in failures
                        if current_time - time <= timedelta(days=FAILURE_EXPIRY_DAYS)]

            failures.append(current_time)

            if sum(1 for time in failures if current_time - time < timedelta(hours=1)) \
                > MAX_FAILURES_PER_HOUR:

                logging.error("Az utóbbi órában több, mint %d hiba történt, ezért az alkalmazás "
                              "leáll.", MAX_FAILURES_PER_HOUR)
                break

            if sum(1 for time in failures if current_time - time < timedelta(days=1)) \
                > MAX_FAILURES_PER_DAY:

                logging.error("Az utóbbi 24 órában több, mint %d hiba történt, ezért az "
                              "alkalmazás leáll.", MAX_FAILURES_PER_DAY)
                break


if __name__ == "__main__":
    main()
