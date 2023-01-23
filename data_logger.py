import logging
from datetime import datetime
from multiprocessing import Process
from threading import Lock
from typing import Optional

from config import GlobalConfig

file_creation_lock = Lock()


def get_time() -> str:
    """
    Returns the current time in a way that can be used in file names.

    :return str: The current time.
    """

    return datetime.now().strftime("%Y-%m-%d_%H.%M.%S,%f")


def log(config: GlobalConfig, /, *args, **kwargs) -> None:
    """
    Logs potentially lagre amount of data in separate files. If kwargs are specified, args are
    ignored.
    Arguments should be strings.

    :param GlobalConfig config: The global configuration.
    """

    if kwargs:
        Process(target=log_dir, args=[config], kwargs=kwargs).start()
    else:
        Process(target=log_files, args=[config] + list(args)).start()


def log_dir(config: GlobalConfig, /, **kwargs):
    """
    Log into multiple files inside aa directory.

    :param GlobalConfig config: The global configuration.
    """

    time = get_time()
    folder_name = time

    file_creation_lock.acquire()
    if count := sum(1 for f in config.logging_folder.glob(f"{time}*") if f.is_dir()):
        folder_name = f"{time}-{count}"
    config.logging_folder.joinpath(folder_name).mkdir()
    file_creation_lock.release()

    folder = config.logging_folder.joinpath(folder_name)
    for k, v in kwargs.items():
        with open(folder.joinpath(f"{k}.log"), "a+", encoding="utf-8") as f:
            if isinstance(v, bytes):
                f.write(v.decode())
            elif isinstance(v, str):
                f.write(v)
            else:
                try:
                    f.write("\n".join(v))
                except TypeError:
                    f.write(str(v))


def log_files(config: GlobalConfig, /, *args):
    """
    Log into multiple files with the same timestamp.

    :param GlobalConfig config: The global configuration.
    """

    time = get_time()

    for data in args:
        log_file(config, data, time)


def log_file(config: GlobalConfig, /, data: str, time: Optional[str] = None):
    """
    Log into a file.

    :param GlobalConfig config: The global configuration.
    :param str data: The data to be logged.
    :param Optional[str] time: The timestamp to be used in the filename, defaults to None
    """

    if time is None:
        time = get_time()
    filename = f"{time}.log"

    if not file_creation_lock.acquire(timeout=30):
        logging.debug("A lock megszerzése sikertelen.")
    if count := sum(1 for _ in config.logging_folder.glob(f"{time}*")):
        filename = f"{time}-{count}.log"
    logging.debug("Napló írása %s fájlba.", config.logging_folder.joinpath(filename))
    with open(config.logging_folder.joinpath(filename), "a+", encoding="utf-8") as f:
        file_creation_lock.release()
        if isinstance(data, bytes):
            f.write(data.decode())
        else:
            f.write(str(data))
