import subprocess
import csv
from datetime import datetime, timedelta
import logging
from pathlib import Path
from time import sleep
from typing import Dict, Iterable, Tuple, Union

import requests

import data_logger


API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
FOLDER_ID = "pfkxq-prxga"

RETRY_COUNT = 5
RETRY_DELAY = 120  # seconds

TIME_FORMAT = "%Y-%m-%d_%H.%M.%s,%f"


class CSVDialect(csv.Dialect):
    delimiter: str = ":"
    doublequote: bool = False
    escapechar: str | None = "\\"
    lineterminator: str = "\n"
    quotechar: str | None = "\""
    quoting: csv._QuotingType = csv.QUOTE_MINIMAL
    skipinitialspace: bool = True
    strict: bool = False


def freeze(obj):
    if isinstance(obj, (list, tuple)):
        return tuple(map(freeze, obj))

    return obj


def read_csv(path):
    with open(path, newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            yield line


def get_file_info(file) -> Dict[str, Tuple[str, datetime, int]]:
    return {name: (hash, datetime.strptime(time, TIME_FORMAT), int(size)) for name, hash, time, size in read_csv(file)}


def write_csv(path, data: Iterable[Iterable]):
    with open(path, newline="") as f:
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def union_csv(path, data: Iterable[Iterable]):
    data = set(freeze(data))
    with open(path, "a+", newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            data.discard(tuple(line))
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def update_file_info(file, data: Dict[str, Tuple[str, datetime, int]]):
    write_csv(file, ((name, hash, time.strftime(TIME_FORMAT), size)
                     for name, (hash, time, size) in data.items()))


def extend_file_info(file, data: Dict[str, Tuple[str, datetime, int]]):
    union_csv(file, ((name, hash, time.strftime(TIME_FORMAT), str(size))
                     for name, (hash, time, size) in data.items()))


def get_file_details(path: Path) -> Tuple[str, datetime, int]:
    r = subprocess.run(["rclone", "hashsum", "quickxor", str(path)], capture_output=True)
    if r.returncode:
        logging.error("Nem sikerült a fájl hashjének meghatározása. (hibakód: %d, '%s', '%s')",
                      r.returncode, r.stdout, r.stderr)
        hashsum = ""
    else:
        hashsum = r.stdout.split()[0].decode("utf-8")

    stat = path.stat()
    return (hashsum, datetime.fromtimestamp(stat.st_mtime), stat.st_size)


def delete_from_file_info(file, data: Dict[str, Tuple[str, datetime, int]]):
    union_csv(file, ((name, hash, time.strftime(TIME_FORMAT), str(size))
                     for name, (hash, time, size) in data.items()))


def is_same_file(file1: Tuple[str, datetime, int], file2: Tuple[str, datetime, int]) -> bool:
    hash1, date1, size1 = file1
    hash2, date2, size2 = file2
    if hash1 != hash2:
        return False

    if size1 != size2:
        logging.warning(
            f"Két fájl azonos hash-sel de eltérő mérettel rendelkezett. ({hash1}, {size1} != {size2})")
        return False

    if abs(date1 - date2) < timedelta(microseconds=10):
        logging.warning("Két fájl azonos hash-sel és mérettel rendelkezett, de módosítási idejük eltérő. "
                        f"({hash1}, {size1}, {date1.isoformat()} != {date2.isoformat()})")
    return True


def write_checkfile(path, data):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(f"{hash}  {name}\n" for name, (hash, _, _) in data.items())


def get_syncthing(req, params={}):
    r = requests.get(f"http://localhost:8384/rest/{req}",
                     params=params, headers={"X-API-Key": API_KEY})
    r.raise_for_status()
    try:
        return r.json()
    except requests.JSONDecodeError:
        return r.text


def post_syncthing(req, data, params={}):
    r = requests.post(f"http://localhost:8384/rest/{req}",
                      json=data, params=params, headers={"X-API-Key": API_KEY})
    r.raise_for_status()
    try:
        return r.json()
    except requests.JSONDecodeError:
        return r.text


def extend_ignores(new: Iterable[Union[Path, str]]) -> bool:
    for _ in range(RETRY_COUNT):
        try:
            res = get_syncthing("db/ignores", {"folder": FOLDER_ID})
        except requests.RequestException as e:
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva. %s", RETRY_DELAY, e)
            sleep(RETRY_DELAY)
        else:
            if "ignore" in res:
                break
            res_text = ""
            if len(str(res)) < 50:
                res_text = f" {res}"
            else:
                data_logger.log(res)
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva.%s", RETRY_DELAY, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt, "
                      "az új elemek hozzáadása sikertelen.")
        return False

    new = {f"/{path}" if not (pathstr := str(path)).startswith("/") else pathstr for path in new}
    ignores = set(res["ignore"]) | new

    for _ in range(RETRY_COUNT):
        try:
            res = post_syncthing("db/ignores", {"ignore": list(ignores)}, {"folder": FOLDER_ID})
        except requests.RequestException as e:
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva. %s", RETRY_DELAY, e)
            sleep(RETRY_DELAY)
        else:
            if "ignore" in res:
                break
            res_text = ""
            if len(str(res)) < 50:
                res_text = f" {res}"
            else:
                data_logger.log(res)
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva.%s", RETRY_DELAY, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt, "
                      "az új elemek hozzáadása sikertelen.")
        return False

    res = post_syncthing("db/ignores", {"ignore": list(ignores)}, {"folder": FOLDER_ID})
    print(res)
    if len(res) == len(ignores) and not new - set(res):
        logging.info(
            "A nem szinkronizálandó fájlok adatbázisának frissítése megtörtént (összesen %d fájl).", len(res))
    else:
        logging.error(
            "A nem szinkronizálandó fájlok adatbázisának frissítése sikertelen. Válasz: %s", res)
    return True
