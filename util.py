import subprocess
import csv
from datetime import datetime, timedelta
import logging
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, Iterator, List, Tuple, Union

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


def freeze(obj: Any) -> Any:
    if isinstance(obj, (list, tuple)):
        return tuple(map(freeze, obj))

    return obj


def read_csv(path: Union[Path, str]) -> Iterator[List]:
    with open(path, newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            yield line


def read_path_list(file: Union[Path, str], *, default: Any = None, strict: bool = True) \
                   -> List[str]:
    try:
        with open(file) as f:
            return f.read().splitlines()
    except (FileNotFoundError, OSError):
        if default is not None or not strict:
            logging.warning("A kért fájl (%s) beolvasása sikertelen. Alapértelmezett érték "
                            "használata: %s", file, default)
            return default
        
        logging.error("A kért fájl (%s) beolvasása sikertelen.", file)
        raise


def run_command(command: List[Union[str, Path]], error_message: str = "", strict: bool = True,
                expected_returncodes: Iterable[int] = (), **kwargs) -> subprocess.CompletedProcess[str]:
                
    logging.debug("Parancs futtatása: %s", command)

    r = subprocess.run(command, capture_output=True, **kwargs)

    if not error_message:
        error_message = f"A parancs ({command}) futtatása meghiúsult."


    if r.returncode and r.returncode not in expected_returncodes:
        if len(r.stdout) + len(r.stderr) > 200:
            logging.error("%s (hibakód: %d)", error_message, r.returncode)
            data_logger.log(stdout=r.stdout, stderr=r.stderr)
        else:
            logging.error("%s (hibakód: %d, '%s', '%s')", error_message, r.returncode, r.stdout,
                          r.stderr)

    if strict and r.returncode not in expected_returncodes:
        r.check_returncode()

    return r


def get_file_info(file: Union[Path, str]) -> Dict[str, Tuple[str, datetime, int]]:
    return {name: (hash, datetime.strptime(time, TIME_FORMAT), int(size))
            for name, hash, time, size in read_csv(file)}


def write_csv(path: Union[Path, str], data: Iterable[Iterable]) -> None:
    with open(path, newline="") as f:
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def union_csv(path: Union[Path, str], data: Iterable[Iterable]) -> None:
    data = set(freeze(data))
    with open(path, "a+", newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            data.discard(tuple(line))
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def update_file_info(file: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    write_csv(file, ((name, hash, time.strftime(TIME_FORMAT), size)
                     for name, (hash, time, size) in data.items()))


def extend_file_info(file: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    union_csv(file, ((name, hash, time.strftime(TIME_FORMAT), str(size))
                     for name, (hash, time, size) in data.items()))


def get_file_details(path: Path) -> Tuple[str, datetime, int]:
    r = run_command(["rclone", "hashsum", "quickxor", str(path)],
                    error_message=f"Nem sikerült a fájl ({path}) hashjének meghatározása.",
                    strict=False)
    if r.returncode:
        hashsum = ""
    else:
        hashsum = r.stdout.split()[0].decode("utf-8")

    stat = path.stat()
    return (hashsum, datetime.fromtimestamp(stat.st_mtime), stat.st_size)


def delete_from_file_info(file: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    union_csv(file, ((name, hash, time.strftime(TIME_FORMAT), str(size))
                     for name, (hash, time, size) in data.items()))


def is_same_file(file1: Tuple[str, datetime, int], file2: Tuple[str, datetime, int]) -> bool:
    hash1, date1, size1 = file1
    hash2, date2, size2 = file2
    if hash1 != hash2:
        return False

    if size1 != size2:
        logging.warning("Két fájl azonos hash-sel de eltérő mérettel rendelkezett. (%s, %s != %s)",
                        hash1, size1, size2)
        return False

    if abs(date1 - date2) < timedelta(microseconds=10):
        logging.warning("Két fájl azonos hash-sel és mérettel rendelkezett, de módosítási idejük "
                        "eltérő. (%s, %d, %s != %s)", hash1, size1, date1.strftime(TIME_FORMAT),
                        date2.strftime(TIME_FORMAT))
    return True


def write_checkfile(path: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(f"{hash}  {name}\n" for name, (hash, _, _) in data.items())


def get_syncthing(req: str, params: Dict[str, Any] = {}) -> Any:
    r = requests.get(f"http://localhost:8384/rest/{req}", params=params,
                     headers={"X-API-Key": API_KEY})
    r.raise_for_status()
    try:
        return r.json()
    except requests.JSONDecodeError:
        return r.text


def post_syncthing(req: str, data: Any, params: Dict[str, Any] = {}) -> Any:
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

    if len(res) == len(ignores) and not new - set(res):
        logging.debug("A nem szinkronizálandó fájlok adatbázisának frissítése megtörtént "
                      "(összesen %d fájl).", len(res))
    else:
        logging.error("A nem szinkronizálandó fájlok adatbázisának frissítése sikertelen. "
                      "Válasz: %s", res)
    return True
