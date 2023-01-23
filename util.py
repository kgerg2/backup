import csv
import logging
import re
import shlex
import subprocess
import traceback
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import sleep, time
from typing import Any, Optional, TypeVar

import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

import data_logger
from config import AllFiles, FolderConfig, GlobalConfig, NoHash


class CSVDialect(csv.Dialect):
    delimiter: str = ":"
    doublequote: bool = False
    escapechar: Optional[str] = "\\"
    lineterminator: str = "\n"
    quotechar: Optional[str] = "\""
    quoting = csv.QUOTE_MINIMAL
    skipinitialspace: bool = True
    strict: bool = False


def freeze(obj: Any) -> Any:
    if isinstance(obj, (list, tuple)):
        return tuple(map(freeze, obj))

    return obj


def read_csv(path: Path | str) -> Iterator[list[str]]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            yield line


def read_path_list(file: Path | str, *, default: Any = None, strict: bool = True) \
        -> list[str]:
    try:
        logging.debug("Adatfájl beolvasása: %s", file)
        with open(file, encoding="utf-8") as f:
            return f.read().splitlines()
    except (FileNotFoundError, OSError):
        if default is not None or not strict:
            logging.warning("A kért fájl (%s) beolvasása sikertelen. Alapértelmezett érték "
                            "használata: %s", file, default)
            return default

        logging.error("A kért fájl (%s) beolvasása sikertelen.", file)
        raise


def run_command(command: list[Any], config: GlobalConfig, error_message: str = "",
                strict: bool = True, expected_returncodes: Iterable[int] = (),
                **kwargs) -> subprocess.CompletedProcess[bytes]:
    logging.debug("Parancs futtatása: %s", shlex.join(map(str, command)))

    r: subprocess.CompletedProcess[bytes] = subprocess.run(list(map(str, command)),
                                                           capture_output=True, check=False,
                                                           encoding=None, **kwargs)  # type: ignore

    if not error_message:
        error_message = f"A parancs ({command}) futtatása meghiúsult."

    if r.returncode and r.returncode not in expected_returncodes:
        if len(r.stdout) + len(r.stderr) > 200:
            logging.error("%s (hibakód: %d)", error_message, r.returncode)
            data_logger.log(config, stdout=r.stdout, stderr=r.stderr)
        else:
            logging.error("%s (hibakód: %d, '%s', '%s')", error_message, r.returncode, r.stdout,
                          r.stderr)

    if strict and r.returncode not in expected_returncodes:
        r.check_returncode()

    return r


def get_file_info(file: Path | str, config: FolderConfig) -> \
        dict[str, tuple[Optional[str], datetime, int]]:
    logging.debug("Adatfájl beolvasása: %s", file)
    return {name: (hash if hash else None,
                   datetime.strptime(time, config.global_config.time_format),
                   int(size))
            for name, hash, time, size in read_csv(file)}


def write_csv(path: Path | str, data: Iterable[Iterable]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def union_csv(path: Path | str, data: Iterable[Iterable]) -> None:
    data = set(freeze(data))
    with open(path, "a+", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, dialect=CSVDialect)
        for line in reader:
            data.discard(tuple(line))
        writer = csv.writer(f, dialect=CSVDialect)
        writer.writerows(data)


def update_file_info(file: Path | str, data: dict[str, tuple[str, datetime, int]],
                     config: FolderConfig) -> None:
    write_csv(file, ((name, hash, time.strftime(config.global_config.time_format), size)
                     for name, (hash, time, size) in data.items()))


def extend_file_info(file: Path | str, data: dict[str, tuple[str, datetime, int]],
                     config: FolderConfig) -> None:
    union_csv(file, ((name, hash, time.strftime(config.global_config.time_format), str(size))
                     for name, (hash, time, size) in data.items()))


def get_file_details(path: Path, config: FolderConfig) -> tuple[NoHash | str, datetime, int]:
    if path.is_dir():
        hashsum = config.global_config.default_hashsum
    else:
        r = run_command(["rclone", "hashsum", "quickxor", str(path)], config.global_config,
                        error_message=f"Nem sikerült a fájl ({path}) hashjének meghatározása.",
                        strict=False)
        if r.returncode:
            hashsum = config.global_config.default_hashsum
        else:
            try:
                hashsum = r.stdout.split()[0].decode("utf-8")
            except IndexError:
                logging.warning("Egy fájlnak nem sikerült a hash-jéjt meghatározni: '%s'", path)
                hashsum = config.global_config.default_hashsum

    stat = path.stat()

    return (hashsum, datetime.fromtimestamp(stat.st_mtime), stat.st_size)


def delete_from_file_info(file: Path | str, data: dict[str, tuple[str, datetime, int]],
                          config: FolderConfig) -> None:
    union_csv(file, ((name, hash, time.strftime(config.global_config.time_format), str(size))
                     for name, (hash, time, size) in data.items()))


def is_same_file(file1: tuple[str, datetime, int] | tuple[datetime, int],
                 file2: tuple[str, datetime, int] | tuple[datetime, int],
                 config: FolderConfig) -> bool:
    if len(file1) == 3 and len(file2) == 3:
        hash1, date1, size1 = file1
        hash2, date2, size2 = file2
        if hash1 != hash2:
            return False

        if size1 != size2:
            logging.warning("Két fájl azonos hash-sel de eltérő mérettel rendelkezett. "
                            "(%s, %s != %s)", hash1, size1, size2)
            return False

        timezone = date1.tzinfo or date2.tzinfo
        if abs(date1.astimezone(timezone) - date2.astimezone(timezone)) < \
                timedelta(microseconds=10):
            logging.warning("Két fájl azonos hash-sel és mérettel rendelkezett, de módosítási "
                            "idejük eltérő. (%s, %d, %s != %s)", hash1, size1,
                            date1.strftime(config.global_config.time_format),
                            date2.strftime(config.global_config.time_format))
        return True

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
        logging.info("Két fájlnak azonos a mérete, de a módosítási idejeik különbsége %s, "
                     "ezért különbözőnek lesznek tekintve.", abs(date1 - date2))

    return False


def write_checkfile(path: Path | str, config: FolderConfig) -> None:
    logging.debug("Rclone ellenőrzőfájl írása.")
    # data_logger.log("".join(f"{hash}  {name}\n" for name, (hash, _, _) in data.items()))
    with Session(config.database) as session:
        select_stmt = select(AllFiles.path, AllFiles.hash).where(AllFiles.hash.is_not(None))
        logging.debug("SQL parancs futtatása: %s", select_stmt)
        data = session.scalars(select_stmt)

        with open(path, "w", encoding="utf-8") as f:
            f.writelines(f"{hash}  {name}\n" for name, hash in data)


def get_remote_file_info(paths: Iterable[Path | str], config: FolderConfig) \
        -> dict[str, tuple[str | NoHash, datetime, int]]:
    logging.debug("Távoli fájlok adatainak összegyűjtése.")

    result = get_remote_mod_times(paths, config)

    with NamedTemporaryFile("w", encoding="utf-8") as f:
        f.writelines(f"{path}\n" for path in paths)
        f.flush()
        r = run_command(["rclone", "hashsum", "quickxor", config.remote_folder, "--files-from",
                         f.name], config.global_config,
                        error_message="Nem sikerült a fájlok hashjének meghatározása.")

    hashsums = {line[42:]: line[:40] for line in r.stdout.decode().splitlines()}
    result = {file: (hashsums.get(file, config.global_config.default_hashsum), date, size)
              for file, (date, size) in result.items()}

    return result


def get_remote_mod_times(paths: Iterable[Path | str],
                         config: FolderConfig) -> dict[str, tuple[datetime, int]]:
    with NamedTemporaryFile("w", encoding="utf-8") as f:
        f.writelines(f"{path}\n" for path in paths)
        f.flush()
        r = run_command(["rclone", "lsl", config.remote_folder, "--files-from", f.name],
                        config.global_config,
                        error_message="Távoli fájlok módosítási idejeinek lekérése sikertelen.",
                        strict=False)

    def get_tuple(line):
        match = re.fullmatch(r" *(\d+) (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) (.*)", line)

        if not match:
            raise ValueError("Cannot get modification time. Time format is not as expected.")

        size, date, file = match.groups()
        date = (date + "0"*6)[:26]
        return file, datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f"), int(size)

    return {file: (date, size) for file, date, size
            in map(get_tuple, r.stdout.decode().splitlines())}


def request_syncthing(method: Callable[..., requests.Response], req: str, config: GlobalConfig,
                      expected_errors: Sequence[int] = (), **kwargs) -> Any:
    last_exception: Optional[requests.RequestException] = None
    for _ in range(config.syncthing_retry_count):
        try:
            r = method(f"http://localhost:8384/rest/{req}",
                       headers={"X-API-Key": config.api_key}, **kwargs)
            if r.status_code in expected_errors:
                break
            r.raise_for_status()
        except requests.RequestException as e:
            last_exception = e
            logging.warning("A Syncthinggel való kommunikáció sikertelen (%s). Újrapróbálás "
                            "%d másodperc múlva.", req, config.syncthing_retry_delay)
            sleep(config.syncthing_retry_delay)
        else:
            break
    else:
        logging.error("A Syncthinggel való kommunikáció meghiúsult. %s",
                      traceback.format_exception(last_exception))

        if last_exception:
            raise last_exception
        raise ValueError("Failed to communicate with Syncthing. Last exception is not stored, "
                         "SYNCTHING_RETRY_COUNT may be 0.")

    try:
        return r.json()
    except requests.JSONDecodeError:
        return r.text


def get_syncthing(req: str, config: GlobalConfig, params: Optional[dict[str, Any]] = None,
                  expected_errors: Sequence[int] = ()) -> Any:
    return request_syncthing(requests.get, req, config, params=params or {},
                             expected_errors=expected_errors)


def post_syncthing(req: str, data: Any, config: GlobalConfig,
                   params: Optional[dict[str, Any]] = None,
                   expected_errors: Sequence[int] = ()) -> Any:
    return request_syncthing(requests.post, req, config, json=data, params=params or {},
                             expected_errors=expected_errors)


def modify_ignores(modify: Callable[[Iterable[str]], Iterable[str]], config: FolderConfig) -> bool:
    for _ in range(config.global_config.syncthing_retry_count):
        try:
            res = get_syncthing("db/ignores", config.global_config, {"folder": config.folder_id})
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
                            "újrapróbálás %d másodperc múlva.%s",
                            config.global_config.syncthing_retry_delay, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt.")
        return False

    ignores = modify(res["ignore"])

    for _ in range(config.global_config.syncthing_retry_count):
        try:
            res = post_syncthing("db/ignores", {"ignore": list(ignores)},
                                 config.global_config, {"folder": config.folder_id})
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
                            "újrapróbálás %d másodperc múlva.%s",
                            config.global_config.syncthing_retry_delay, res_text)
    else:
        logging.error("A figyelmen kívül hagyott fájlok lekérdezése során túl sok hiba történt.")
        return False

    if set(res["ignore"]) == set(ignores):
        logging.debug("A nem szinkronizálandó fájlok adatbázisának frissítése megtörtént "
                      "(összesen %d fájl).", len(res["ignore"]))
    else:
        logging.error("A nem szinkronizálandó fájlok adatbázisának frissítése sikertelen. "
                      "Válasz: %s", res)
    return True


def remove_parents_from_ignores(config: FolderConfig) -> None:
    def filter_leafs(paths):
        paths = sorted(paths)
        filtered = [p1 for p1, p2 in zip(paths, paths[1:] + [""]) if not p2.startswith(p1)]
        return filtered

    if not modify_ignores(filter_leafs, config):
        raise ChildProcessError()


def extend_ignores(new: Iterable[Path | str], config: FolderConfig) -> None:
    new = {f"/{path}" if not (pathstr := str(path)).startswith("/") else pathstr for path in new}

    if not modify_ignores(lambda ignores: set(ignores) | new, config):
        raise ChildProcessError()


def discard_ignores(files: Iterable[Path | str], config: FolderConfig) -> None:
    files = {f"/{path}" if not (pathstr := str(path)).startswith("/") else pathstr
             for path in files}

    if not modify_ignores(lambda ignores: set(ignores) - files, config):
        raise ChildProcessError()


T = TypeVar("T")


def retry_on_error(function: Callable[..., T], max_retry_count: int = 10, retry_expiry: int = 3600,
                   retry_delay: int = 10, error_message: Optional[str] = None, args: Sequence = (), **kwargs) -> T:

    if max_retry_count < 0 or retry_expiry < 0 or retry_delay < 0:
        logging.error("Negatív érték nem megengedett. (max_retry_count=%s, retry_expiry=%s, "
                      "retry_delay=%s,)", max_retry_count, retry_expiry, retry_delay)
        raise ValueError("Negative values are not allowed")

    runs = []

    while True:
        runs.append(time())
        try:
            return function(*args, **kwargs)
        except:
            if error_message:
                logging.error("%s Hibaüzenet: %s", error_message, traceback.format_exc())

            sleep(retry_delay)

            curr_time = time()
            while runs and runs[0] + retry_expiry < curr_time:
                runs.pop(0)

            runs.append(curr_time)

            if len(runs) > max_retry_count:
                logging.critical("A %s függvény futása során túl sok hiba történt, nem lesz "
                                 "újraindítva.", function)
                raise
