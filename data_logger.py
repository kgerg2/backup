from datetime import datetime
from pathlib import Path
from threading import Lock, Thread

log_folder = Path(".")
file_creation_lock = Lock()

def configure(log_path: Path):
    global log_folder
    log_folder = log_path

def get_time():
    return datetime.now().strftime("%Y-%m-%d_%H.%M.%s,%f")

def log(*args, **kwargs):
    """
    Logs potentially lagre amount of data in separate files. If kwargs are specified, args are ignored.
    Arguments should be strings.
    """
    if kwargs:
        Thread(target=log_dir, kwargs=kwargs).start()
    else:
        Thread(target=log_files, args=args).start()
    
def log_dir(**kwargs):
    time = get_time()
    folder_name = time

    file_creation_lock.acquire()
    if count := len(f for f in log_folder.glob(f"{time}*") if f.is_dir()):
        folder_name = f"{time}-{count}"
    log_folder.mkdir(folder_name)
    file_creation_lock.release()

    folder = log_folder.joinpath(folder_name)
    for k, v in kwargs:
        with open(folder.joinpath(f"{k}.log"), "a+") as f:
            f.write(v)

def log_files(time=None, *args):
    if time is None:
        time = get_time()

    for data in args:
        log_file(data, time)

def log_file(data: str, time: str=None):
    if time is None:
        time = get_time()
    filename = f"{time}.log"

    file_creation_lock.acquire()
    if count := len(log_folder.glob(f"{time}*")):
        filename = f"{time}-{count}.log"
    with open(log_folder.joinpath(filename), "a+") as f:
        file_creation_lock.release()

        f.write(data)

