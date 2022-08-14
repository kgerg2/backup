from datetime import datetime
import logging
from multiprocessing import Process
from pathlib import Path
from threading import Lock, Thread

log_folder = Path("./logs")
file_creation_lock = Lock()

def configure(log_path: Path):
    global log_folder
    log_folder = log_path

def get_time():
    return datetime.now().strftime("%Y-%m-%d_%H.%M.%S,%f")

def log(*args, **kwargs):
    """
    Logs potentially lagre amount of data in separate files. If kwargs are specified, args are ignored.
    Arguments should be strings.
    """
    if kwargs:
        Process(target=log_dir, kwargs=kwargs).start()
    else:
        Process(target=log_files, args=args).start()
    
def log_dir(**kwargs):
    time = get_time()
    folder_name = time

    file_creation_lock.acquire()
    if count := sum(1 for f in log_folder.glob(f"{time}*") if f.is_dir()):
        folder_name = f"{time}-{count}"
    log_folder.joinpath(folder_name).mkdir()
    file_creation_lock.release()

    folder = log_folder.joinpath(folder_name)
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

def log_files(*args):
    # logging.debug("Fájlok írása: %s", args)
    
    time = get_time()

    for data in args:
        log_file(data, time)

def log_file(data: str, time: str=None):
    if time is None:
        time = get_time()
    filename = f"{time}.log"

    if not file_creation_lock.acquire(timeout=30):
        logging.debug("A lock megszerzése sikertelen.")
    if count := sum(1 for _ in log_folder.glob(f"{time}*")):
        filename = f"{time}-{count}.log"
    logging.debug("Napló írása %s fájlba.", log_folder.joinpath(filename))
    with open(log_folder.joinpath(filename), "a+", encoding="utf-8") as f:
        file_creation_lock.release()
        if isinstance(data, bytes):
            f.write(data.decode())
        else:
            f.write(str(data))

