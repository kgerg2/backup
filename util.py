import csv
import itertools
import json
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
from sqlalchemy import Engine, select
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



def run_rclone(command: str, args: list[Any], config: GlobalConfig, method: str = "core/command",
               run_async: bool = True, use_web_ui: bool = True, **kwargs
               ) -> subprocess.CompletedProcess[bytes]:
    rclone_config = config.rclone_gui

    if not rclone_config or not use_web_ui:
        return run_command(["rclone", command, *args], config, **kwargs)

    if command not in rclone_config.special_commands:
        return wait_for_rclone(
            run_command(["rclone", "rc", "--url", f"{rclone_config.host}:{rclone_config.port}",
                         "--user", rclone_config.user, "--pass", rclone_config.password,
                         method, f"command={command}",
                         *itertools.chain.from_iterable(zip(itertools.repeat("-a"), args)),
                         f"_async={str(run_async).lower()}"],
                        config,
                        **kwargs),
            run_async,
            config
        )

    def camelize(s: str) -> str:
        return "".join(word.capitalize() for word in s.split("-"))

    filters = {}
    key = None
    pos_args = []
    for arg in args:
        if arg in rclone_config.filter_params:
            key = arg
        elif key is not None:
            if key in rclone_config.list_filter_params and not isinstance(arg, list):
                arg = [arg]
            filters[camelize(key)] = arg
            key = None
        elif isinstance(arg, str) and arg.startswith("-"):
            logging.warning("A '%s' paraméter nem támogatott a '%s' parancsnál, és ezért el lett "
                            "dobva.", arg, command)
        else:
            pos_args.append(arg)

    if command == "purge" and len(pos_args) == 1:
        pos_args.append("/")

    return wait_for_rclone(
        run_command(["rclone", "rc", "--url", f"{rclone_config.host}:{rclone_config.port}",
                     "--user", rclone_config.user, "--pass", rclone_config.password,
                     rclone_config.special_commands[command][0],
                     *(f"{name}={value}" for name, value
                       in zip(rclone_config.special_commands[command][1], pos_args)),
                     f"_filter={json.dumps(filters)}", f"_async={str(run_async).lower()}"],
                    config,
                    **kwargs),
        run_async,
        config
    )

def wait_for_rclone(res: subprocess.CompletedProcess[bytes], runs_async: bool, config: GlobalConfig) -> Any:
    if not runs_async:
        return res
    
    rclone_config = config.rclone_gui

    if not rclone_config:
        logging.error("Rclone GUI nincs beállítva, mégis aszinkron rclone parancs lett "
                      "kezdeményezve.")
        return res

    if res.returncode:
        logging.error("Az rclone parancs aszinkron elindítása meghiúsult (hibakód: %d, '%s', '%s')",
                      res.returncode, res.stdout, res.stderr)
        return res

    output = json.loads(res.stdout)

    if "jobid" not in output:
        logging.error("A rclone parancs aszinkron elindítása meghiúsult (nincs 'jobid' mező)")
        return res
    
    jobid = output["jobid"]

    wait_time = 1
    while True:
        sleep(wait_time)
        wait_time = min(wait_time * 2, rclone_config.max_async_poll_interval)
        res = run_command(["rclone", "rc", "--url", f"{rclone_config.host}:{rclone_config.port}",
                           "--user", rclone_config.user, "--pass", rclone_config.password,
                           "job/status", f"jobid={jobid}"],
                          config)
        output = json.loads(res.stdout)
        if output["finished"] or output["error"] == "job not found":
            break

    return output["output"]

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
    stat = config.local_folder.joinpath(path).stat()
    time = datetime.fromtimestamp(stat.st_mtime)
    size = stat.st_size

    if config.local_folder.joinpath(path).is_dir():
        return config.global_config.default_hashsum, time, size

    r = run_rclone("hashsum", ["quickxor", config.local_folder.joinpath(path)],
                    config.global_config,
                    run_async=False,
                    error_message=f"Nem sikerült a fájl ({path}) hashjének meghatározása.",
                    strict=False)
    if r.returncode:
        return config.global_config.default_hashsum, time, size

    try:
        output = json.loads(r.stdout)

        if "error" in output and output["error"]:
            logging.warning("Egy fájlnak nem sikerült a hash-jéjt meghatározni: '%s'", path)
            return config.global_config.default_hashsum, time, size

        result = output["result"]
    except (json.JSONDecodeError, KeyError):
        result = r.stdout.decode()

    try:
        hashsum = result.split()[0]
    except IndexError:
        logging.warning("Egy fájlnak nem sikerült a hash-jéjt meghatározni: '%s'", path)
        hashsum = config.global_config.default_hashsum

    return (hashsum, datetime.fromtimestamp(stat.st_mtime), stat.st_size)


def delete_from_file_info(file: Path | str, data: dict[str, tuple[str, datetime, int]],
                          config: FolderConfig) -> None:
    union_csv(file, ((name, hash, time.strftime(config.global_config.time_format), str(size))
                     for name, (hash, time, size) in data.items()))


def is_same_file(file1: tuple[str | NoHash, datetime | None, int | None]
                        | tuple[datetime | None, int | None],
                 file2: tuple[str | NoHash, datetime | None, int | None]
                        | tuple[datetime | None, int | None],
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
        
        if date1 is None or date2 is None:
            return date1 == date2

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
    
    if date1 is None or date2 is None:
        return date1 == date2

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
    with Session(config.database) as session:
        select_stmt = select(AllFiles).where(AllFiles.hash.is_not(None))
        logging.debug("SQL parancs futtatása: %s", select_stmt)
        data = session.scalars(select_stmt)
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(f"{r.hash}  {r.path}\n" for r in data)


def get_remote_file_info(paths: Iterable[Path | str], config: FolderConfig) \
        -> dict[str, tuple[str | NoHash, datetime, int]]:
    logging.debug("Távoli fájlok adatainak összegyűjtése.")

    result = get_remote_mod_times(paths, config)

    with NamedTemporaryFile("w", encoding="utf-8") as f:
        f.writelines(f"{path}\n" for path in paths)
        f.flush()
        r = run_rclone("hashsum", ["quickxor", config.remote_folder, "--files-from",
                         f.name], config.global_config, run_async=False,
                        error_message="Nem sikerült a fájlok hashjének meghatározása.")

    try:
        output = json.loads(r.stdout)

        if "error" in output and output["error"]:
            logging.error("Nem sikerült a fájlok hashjének meghatározása: %s", output)

        hashsum_result = output["result"].splitlines()
    except (json.JSONDecodeError, KeyError):
        hashsum_result = r.stdout.decode().splitlines()

    hashsums = {line[42:]: line[:40] for line in hashsum_result}
    result = {file: (hashsums.get(file, config.global_config.default_hashsum), date, size)
              for file, (date, size) in result.items()}

    return result


def get_remote_mod_times(paths: Iterable[Path | str],
                         config: FolderConfig) -> dict[str, tuple[datetime, int]]:
    with NamedTemporaryFile("w", encoding="utf-8") as f:
        f.writelines(f"{path}\n" for path in paths)
        f.flush()
        r = run_rclone("lsl", [config.remote_folder, "--files-from", f.name],
                        config.global_config, run_async=False,
                        error_message="Távoli fájlok módosítási idejeinek lekérése sikertelen.",
                        strict=False)

    def get_tuple(line):
        match = re.fullmatch(r" *(\d+) (\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) (.*)", line)

        if not match:
            raise ValueError("Cannot get modification time. Time format is not as expected.")

        size, date, file = match.groups()
        date = (date + "0"*6)[:26]
        return file, datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f"), int(size)

    try:
        output = json.loads(r.stdout)

        if "error" in output and output["error"]:
            logging.error("A távoli fájlok módosítási idejeinek lekérése sikertelen: %s", output)
            raise ValueError("Cannot get modification time. " + output)

        result = output["result"].splitlines()
    except (json.JSONDecodeError, KeyError):
        result = r.stdout.decode().splitlines()

    return {file: (date, size) for file, date, size
            in map(get_tuple, result)}


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

    if res["ignore"] is None:
        res["ignore"] = []

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

    if res["ignore"] is None:
        res["ignore"] = []

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


def file_exisis_in_database(database: Engine, regex: str) -> bool:
    with Session(database) as session:
        select_stmt = select(AllFiles).where(AllFiles.path.regexp_match(regex))
        logging.debug("SQL parancs futtatása: %s", select_stmt)
        return session.execute(select_stmt).first() is not None
