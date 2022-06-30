import csv
import logging
import shlex
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import sleep
import traceback
from typing import Any, Callable, Dict, Iterable, Iterator, List, Tuple, Union

import requests

import data_logger
from config import API_KEY, DEFAULT_HASHSUM, FOLDER_ID, REMOTE_FOLDER, SYNCTHING_RETRY_COUNT, SYNCTHING_RETRY_DELAY, TIME_FORMAT


class CSVDialect(csv.Dialect):
    delimiter: str = ":"
    doublequote: bool = False
    escapechar: Union[str, None] = "\\"
    lineterminator: str = "\n"
    quotechar: Union[str, None] = "\""
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace: bool = True
    strict: bool = False


def freeze(obj: Any) -> Any:
    if isinstance(obj, (list, tuple)):
        return tuple(map(freeze, obj))

    return obj


def read_csv(path: Union[Path, str]) -> Iterator[List]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            yield line


def read_path_list(file: Union[Path, str], *, default: Any = None, strict: bool = True) \
                   -> List[str]:
    try:
        with open(file, encoding="utf-8") as f:
            return f.read().splitlines()
    except (FileNotFoundError, OSError):
        if default is not None or not strict:
            logging.warning("A kért fájl (%s) beolvasása sikertelen. Alapértelmezett érték "
                            "használata: %s", file, default)
            return default
        
        logging.error("A kért fájl (%s) beolvasása sikertelen.", file)
        raise


def run_command(command: List[Union[str, Path]], error_message: str = "", strict: bool = True,
                expected_returncodes: Iterable[int] = (), **kwargs) -> subprocess.CompletedProcess[bytes]:
                
    logging.debug("Parancs futtatása: %s", shlex.join(map(str, command)))

    r = subprocess.run(list(map(str, command)), capture_output=True, **kwargs)

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
    logging.debug("Adatfájl beolvasása: %s", file)
    return {name: (hash if hash else None, datetime.strptime(time, TIME_FORMAT), int(size))
            for name, hash, time, size in read_csv(file)}


def write_csv(path: Union[Path, str], data: Iterable[Iterable]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def union_csv(path: Union[Path, str], data: Iterable[Iterable]) -> None:
    data = set(freeze(data))
    with open(path, "a+", encoding="utf-8", newline="") as f:
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
    if path.is_dir():
        hashsum = None
    else:
        r = run_command(["rclone", "hashsum", "quickxor", str(path)],
                        error_message=f"Nem sikerült a fájl ({path}) hashjének meghatározása.",
                        strict=False)
        if r.returncode:
            hashsum = DEFAULT_HASHSUM
        else:
            try:
                hashsum = r.stdout.split()[0].decode("utf-8")
            except IndexError:
                logging.warning("Egy fájlnak nem sikerült a hash-jéjt meghatározni: '%s'", path)
                hashsum = DEFAULT_HASHSUM

    stat = path.stat()
        
    return (hashsum, datetime.fromtimestamp(stat.st_mtime), stat.st_size)


def delete_from_file_info(file: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    union_csv(file, ((name, hash, time.strftime(TIME_FORMAT), str(size))
                     for name, (hash, time, size) in data.items()))


def is_same_file(file1: Union[Tuple[str, datetime, int], Tuple[datetime, int]],
                 file2: Union[Tuple[str, datetime, int], Tuple[datetime, int]]) -> bool:
    if len(file1) == 3:
        hash1, date1, size1 = file1
        hash2, date2, size2 = file2
        if hash1 != hash2:
            return False

        if size1 != size2:
            logging.warning("Két fájl azonos hash-sel de eltérő mérettel rendelkezett. (%s, %s != %s)",
                            hash1, size1, size2)
            return False

        timezone = date1.tzinfo or date2.tzinfo
        if abs(date1.astimezone(timezone) - date2.astimezone(timezone)) < timedelta(microseconds=10):
            logging.warning("Két fájl azonos hash-sel és mérettel rendelkezett, de módosítási idejük "
                            "eltérő. (%s, %d, %s != %s)", hash1, size1, date1.strftime(TIME_FORMAT),
                            date2.strftime(TIME_FORMAT))
        return True

    else:
        date1, size1 = file1[-2:]
        date2, size2 = file2[-2:]

        if size1 != size2:
            return False

        timezone = date1.tzinfo or date2.tzinfo
        date1 = date1.astimezone(timezone)
        date2 = date2.astimezone(timezone)
        if abs(date1 - date2) < timedelta(microseconds=10):
            return True

        if abs(date1 - date2) < timedelta(milliseconds=1):
            logging.warning("Két fájlnak azonos a mérete, de a módosítási idejeik különbsége %s, "
                            "ezért különbözőnek lesznek tekintve.", abs(date1 - date2))

        return False

def write_checkfile(path: Union[Path, str], data: Dict[str, Tuple[str, datetime, int]]) -> None:
    logging.debug("Rclone ellenőrzőfájl írása.")
    data_logger.log("".join(f"{hash}  {name}\n" for name, (hash, _, _) in data.items()))
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(f"{hash}  {name}\n" for name, (hash, _, _) in data.items())


def get_remote_mod_times(paths: Iterable[Union[Path, str]]) -> Dict[str, Tuple[datetime, int]]:
    with NamedTemporaryFile("w", encoding="utf-8") as f:
        f.writelines(f"{path}\n" for path in paths)
        f.flush()
        r = run_command(["rclone", "lsl", REMOTE_FOLDER, "--files-from", f.name],
                        error_message="Távoli fájlok módosítási idejeinek lekérése sikertelen.",
                        strict=False)

        return {line[40:]: (datetime.strptime(line[10:36], "%Y-%m-%d %H:%M:%S.%f"), int(line[:9]))
                for line in r.stdout.decode().splitlines()}
        


def request_syncthing(method: Callable[..., requests.Response], req: str,
                      expected_errors: Tuple[int] = (), **kwargs) -> Any:
    last_exception = None
    for _ in range(SYNCTHING_RETRY_COUNT):
        try:
            r = method(f"http://localhost:8384/rest/{req}", headers={"X-API-Key": API_KEY}, **kwargs)
            if r.status_code in expected_errors:
                break
            r.raise_for_status()
        except requests.RequestException as e:
            last_exception = e
            logging.warning("A Syncthinggel való kommunikáció sikertelen (%s). Újrapróbálás "
                            "%d másodperc múlva.", req, SYNCTHING_RETRY_DELAY)
            sleep(SYNCTHING_RETRY_DELAY)
        else:
            break
    else:
        logging.error("A Syncthinggel való kommunikáció meghiúsult. %s",
                      traceback.format_exception(last_exception))
        raise last_exception

    try:
        return r.json()
    except requests.JSONDecodeError:
        return r.text


def get_syncthing(req: str, params: Dict[str, Any] = {}, expected_errors: Tuple[int] = ()) -> Any:
    return request_syncthing(requests.get, req, params=params, expected_errors=expected_errors)


def post_syncthing(req: str, data: Any, params: Dict[str, Any] = {},
                   expected_errors: Tuple[int] = ()) -> Any:
    return request_syncthing(requests.post, req, json=data, params=params,
                             expected_errors=expected_errors)


def extend_ignores(new: Iterable[Union[Path, str]]) -> bool:
    for _ in range(SYNCTHING_RETRY_COUNT):
        try:
            res = get_syncthing("db/ignores", {"folder": FOLDER_ID})
        except requests.RequestException:
            return False
        else:
            if "ignore" in res:
                break
            res_text = ""
            if len(str(res)) < 50:
                res_text = f" {res}"
            else:
                data_logger.log(res)
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva.%s", SYNCTHING_RETRY_DELAY, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt, "
                      "az új elemek hozzáadása sikertelen.")
        return False

    new = {f"/{path}" if not (pathstr := str(path)).startswith("/") else pathstr for path in new}
    ignores = set(res["ignore"]) | new

    for _ in range(SYNCTHING_RETRY_COUNT):
        try:
            res = post_syncthing("db/ignores", {"ignore": list(ignores)}, {"folder": FOLDER_ID})
        except requests.RequestException:
            return False
        else:
            if "ignore" in res:
                break
            res_text = ""
            if len(str(res)) < 50:
                res_text = f" {res}"
            else:
                data_logger.log(res)
            logging.warning("A figyelmen kívül hagyott fájlok lekérdezése sikertelen, "
                            "újrapróbálás %d másodperc múlva.%s", SYNCTHING_RETRY_DELAY, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt, "
                      "az új elemek hozzáadása sikertelen.")
        return False

    if len(res["ignore"]) == len(ignores) and not new - set(res["ignore"]):
        logging.debug("A nem szinkronizálandó fájlok adatbázisának frissítése megtörtént "
                      "(összesen %d fájl).", len(res["ignore"]))
    else:
        logging.error("A nem szinkronizálandó fájlok adatbázisának frissítése sikertelen. "
                      "Válasz: %s", res)
    return True
