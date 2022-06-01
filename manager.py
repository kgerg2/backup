from datetime import datetime, time, timedelta
import logging
from operator import itemgetter
from queue import Queue
from multiprocessing import Process
from time import sleep
from typing import Callable, List, Optional, Union
from dateutil import relativedelta, rrule, utils, tz

from sync import archive

FAILURE_EXPIRY_DAYS = 30
MAX_FAILURES_PER_HOUR = 5
MAX_FAILURES_PER_DAY = 20

class TimedTask:
    def __init__(self,
                 name: str,
                 task: Callable,
                 time: datetime,
                 time_fields: List[str],
                 time_diff: Union[relativedelta.relativedelta, timedelta],
                 max_delay: timedelta,
                 retry_time: Union[relativedelta.relativedelta, timedelta],
                 max_retry_count: int = 10,
                 enabled: bool = True):
        self.name = name
        self.task = task
        self.time = time
        self.time_fields = time_fields
        self.time_diff = time_diff
        self.max_delay = max_delay
        self.retry_time = retry_time
        self.max_retry_count = max_retry_count
        self.enabled = enabled

    #     self.next_time = None
    #     self.process = None
    #     self.retry_count = 0

    # def reset_variables(self):
    #     self.next_time = None
    #     self.process = None
    #     self.retry_count = 0
        
    def get_next_scheduled(self):
        time = datetime.now()
        for field in self.time_fields:
            time.replace(**{field: getattr(self.time, field)})
        time += self.time_diff
        return time

    def get_next_retry(self, scheduled_time):
        return scheduled_time + self.retry_time

    def check_if_ok_now(self, scheduled_time):
        return 0 <= datetime.now() - scheduled_time < self.max_delay



TASKS = (
    TimedTask("archiválás", archive, datetime(2000, 1, 1, 0, 0, 0), ["day", "hour", "minute", "second"], relativedelta(month=1), timedelta(hours=4), timedelta(days=1)),
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
        task = min(TASKS, lambda x: x.next_time)

        if task.process is not None and task.process.exitcode:
            task.retry_count += 1
        else:
            task.retry_count = 0

        if task.retry_count > task.max_retry_count:
            logging.error(f"A(z) {task.name} feladat futtatása során az újrapróbálkozások száma "
                          "meghaladta a megadott értéket ezért deaktiválásra került.")
            task.enabled = False

        if not task.enabled:
            logging.warning(f"A(z) {task.name} feladat deaktiválva van ezért nem kerül futtatásra.")
            continue

        seconds_until_task = (task.next_time - datetime.now()).total_seconds()
        if seconds_until_task > 0:
            sleep(seconds_until_task)

        if not task.check_if_ok_now(task.next_time):
            task.next_time = task.get_next_retry(task.next_time)
            logging.info(f"A(z) {task.name} feladat a futtatása lekéste a "
                         f"megfelelő intervallumot. Újrapróbálás ideje: {task.next_time.isoformat()}")
            task.retry_count += 1
            continue

        if task.process is not None and task.process.is_alive():
            task.next_time = task.get_next_retry(task.next_time)
            logging.info(f"A(z) {task.name} feladat előző futtatása még nem fejeződött be, "
                         f"ezért most nem indult el újra. Következő újrapróbálás ideje: "
                         f"{task.next_time.isoformat()}")
            task.retry_count += 1
            continue
            

        try:
            task.process = Process(target=task.task)
            task.process.start()
        except SystemExit as e:
            logging.warning(f"A futtatott feladat ({task.name}) kilépést kért a programból: {e}")
            return False
        except:
            logging.error(f"Hiba történt a feladat ({task.name}) futtatása során.")
            task.next_time = task.get_next_retry(task.next_time)
            raise
        else:
            task.next_time = task.get_next_scheduled()



def main():
    # logging.basicConfig()
    end = False
    failures = []
    while not end:
        try:
            end = not start_main_loop()
        except Exception as e:
            logging.error(f"Hiba történt a program futása során: {e}")
            current_time = datetime.now()

            failures = [time for time in failures if current_time - time <= timedelta(days=FAILURE_EXPIRY_DAYS)]

            failures.append(current_time)
            
            if sum(1 for time in failures if current_time - time < timedelta(hours=1)) > MAX_FAILURES_PER_HOUR:
                logging.error(f"Az utóbbi órában több, mint {MAX_FAILURES_PER_HOUR} hiba történt, ezért az alkalmazás leáll.")
                break

            if sum(1 for time in failures if current_time - time < timedelta(days=1)) > MAX_FAILURES_PER_DAY:
                logging.error(f"Az utóbbi 24 órában több, mint {MAX_FAILURES_PER_DAY} hiba történt, ezért az alkalmazás leáll.")
                break


if __name__ == "__main__":
    main()