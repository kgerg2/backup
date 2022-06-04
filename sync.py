from datetime import datetime, timedelta
import json
import logging
from multiprocessing import Process
from pathlib import Path
from pprint import pprint, pformat
from shutil import copy2
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from threading import Thread
from typing import Dict, Tuple
from zoneinfo import ZoneInfo
import requests

from util import extend_file_info, get_file_details, get_file_info, update_file_info, write_checkfile
from uploader import Uploader
import data_logger

API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
FOLDER_ID = "pfkxq-prxga"
FOLDER = Path("~/bmt").expanduser()
IGNORE_FILE = FOLDER.joinpath(".stignore")
BACKUP_FOLDER = Path("~/bmt-tavoli").expanduser()
REMOTE_FOLDER = Path("onedrive-kifu:bmt")

METADATA_FOLDER = FOLDER.joinpath(".backupdata")
BACKUP_FILE_LIST = METADATA_FOLDER.joinpath("backup-files.txt")
UPLOADED_FILES = METADATA_FOLDER.joinpath("onedrive-files.txt")

ALL_FILES = Path("~/bmt/")
CLOUD_ONLY_FILES = Path("~/bmt/")

# def make_request(type, req, params)
#     r = type(f"http://localhost:8384/rest/{req}", params=params, headers={"X-API-Key": API_KEY})
#     try:
#         return r.json()
#     except:
#         return r.text


def get(req, params={}):
    r = requests.get(f"http://localhost:8384/rest/{req}",
                     params=params, headers={"X-API-Key": API_KEY})
    try:
        return r.json()
    except:
        return r.text


def post(req, data, params={}):
    # print(req, data, params)
    r = requests.post(f"http://localhost:8384/rest/{req}",
                      json=data, params=params, headers={"X-API-Key": API_KEY})
    # print(r.text)
    try:
        return r.json()
    except:
        return r.text


def write_json(obj, path):
    with open(path, "w") as f:
        json.dump(obj, f)

    print(f"JSON written to {path}")


def read_json(path):
    with open(path) as f:
        return json.load(f)


def get_change(last_event=0, timeout=60):
    return get("events", {"events": "RemoteChangeDetected,ItemFinished", "since": last_event, "timeout": timeout})


def get_ignores():
    return get("db/ignores", {"folder": FOLDER_ID})["ignore"]


def update_ignores(ignores):
    # print(f"Ignores: {ignores}")
    res = post("db/ignores", {"ignore": list(ignores)}, {"folder": FOLDER_ID})
    print(res)
    if len(res) == len(ignores):
        logging.info(
            "A nem szinkronizálandó fájlok adatbázisának frissítése megtörtént (összesen %d fájl).", len(res))
    else:
        logging.error(
            "A nem szinkronizálandó fájlok adatbázisának frissítése sikertelen. Válasz: %s", res)
    return res


with open(FILE_VERSIONS) as f:
    latest_versions = json.load(f)


def main():
    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    ignores = set(get_ignores())

    if ignores is None:
        ignores = set()
    last_event = 0

    print(f"{ignores=}")

    print(get("db/browse", {"folder": FOLDER_ID}))

    while True:
        changes = get_change(last_event)
        if changes:
            print(changes)
            last_event = max(c["id"] for c in changes)

            ignores_changed = False

            for c in changes:
                if c["type"] == "RemoteChangeDetected":
                    latest_versions.pop(c["data"]["path"], None)

                if c["type"] == "ItemFinished" and c["data"]["error"] is None:
                    stat = FOLDER.joinpath(c["data"]["item"]).stat()
                    latest_versions[c["data"]["item"]] = {
                        "time": stat.st_mtime_ns, "size": stat.st_size}
                    ignores.add("/" + c["data"]["item"])
                    ignores_changed = True
                    write_json(latest_versions, FILE_VERSIONS)

            if ignores_changed:
                Thread(target=update_ignores, args=[ignores])


def is_same(date1, size1, date2, size2):
    return abs(date1 - date2) < timedelta(microseconds=10) and size1 == size2


def archive():
    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    archived_versions = read_json(FILE_VERSIONS)
    global_versions = get("db/browse", {"folder": FOLDER_ID})
    local_versions = []

    # print(global_versions)
    archived_files = {k: (v["time"], v["size"]) for k, v in archived_versions.items()}
    global_files: Dict[Path, Tuple[datetime, int]] = {}
    local_files: Dict[Path, Tuple[datetime, int]] = {}

    for path in global_versions:
        global_files[path["name"]] = (datetime.fromisoformat(
            path["modTime"][:26] + path["modTime"][-6:]), path["size"])
        # pprint({
        #     "name": path["name"],
        #     "modTime": datetime.fromisoformat(path["modTime"][:26] + path["modTime"][-6:]),
        #     "size": path["size"]
        # })

    for path in FOLDER.glob("**/*"):
        stat = path.stat()
        local_files[str(path.relative_to(FOLDER))] = (datetime.fromtimestamp(
            stat.st_mtime).astimezone(ZoneInfo("Europe/Budapest")), stat.st_size)
        # pprint({
        #     "name": str(path.relative_to(FOLDER)),
        #     "modTime": (stat.st_mtime, datetime.fromtimestamp(stat.st_mtime).astimezone(ZoneInfo("Europe/Budapest")).isoformat()),
        #     "size": stat.st_size
        # })

    # for file in global_files.keys() & local_files.keys():
    #     glt, gls = global_files[file]
    #     lt, ls = local_files[file]

    #     if not is_same(glt, gls, lt, ls):
    #         continue

    #     copy2(FOLDER.joinpath(file), REMOTE_FOLDER.joinpath(file))

    #     archived_versions[file] = (glt.isoformat(), gls)
    #     print(f"{file}: méretkülönbség {abs(gls - ls)}, időkülönbség {abs(glt - lt) < timedelta(microseconds=10)}")

    to_archive = [f for f, data in local_files.items()
                  if f in global_files and (f not in archived_files or not is_same(*data, *archived_files[f]))]

    for file in to_archive:
        copy2(FOLDER.joinpath(file), BACKUP_FOLDER.joinpath(file))

        glt, gls = global_files[file]
        archived_versions[file] = {"time": glt.isoformat(), "size": gls}

    write_json(archived_versions, BACKUP_FILE_LIST)

    ignores = get_ignores()

    ignores.extend(map(lambda x: f"/{x}", to_archive))

    update_ignores(ignores)

    onedrive_uploader.run_move(list(local_files.keys() & global_files.keys()))

    # A többi lokális fájl törölhető?

    # for file in to_archive:
    #     if file.is_file():
    #         file.unlink()

    #     if file.is_dir():
    #         file.unlink()

    #     print(f"Unexpected thing: {file} not file and not dir. Might have been deleted.")

# rclone copy Képek onedrive-kifu:Képek --exclude /.st**


sync_process = None
last_sync_event = 0


def check_sync():
    if sync_process is None:
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()

    if not sync_process.is_alive():
        logging.warning("A Syncthing szinkronizálásért felelős folyamat nem fut, úrjaindításra kerül. "
                        "Leállás hibakódja: %d", sync_process.exitcode)
        sync_process = Process(target=sync_to_cloud)
        sync_process.start()


def sync_to_cloud():
    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    while True:
        res = get_change(last_event=last_sync_event, timeout=3600)
        new_files = []
        deleted_files = []
        for change in res:
            logging.debug(pformat(change))
            last_sync_event = change["id"]
            if change["type"] == "RemoteChangeDetected":
                relpath = Path(change["data"]["path"]).relative_to(FOLDER)
                if change["data"]["action"] == "deleted":
                    if change["data"]["type"] == "file":
                        onedrive_uploader.delete_file(relpath)
                    elif change["data"]["type"] == "directory":
                        onedrive_uploader.delete_folder(relpath)
                    else:
                        logging.warning("Ismeretlen típusú törölt elem: %s", change["data"]["type"])
                        data_logger.log(change)
                        onedrive_uploader.delete_folder(relpath)
                    deleted_files.append(relpath)
                else:
                    onedrive_uploader.upload(relpath)
                    new_files.append(relpath)

        new_file_details = {str(path): get_file_details(FOLDER.joinpath(path)) for path in new_files}

        if deleted_files:
            all_info = get_file_info(ALL_FILES)
            for deleted in deleted_files:
                all_info.pop(deleted)
            all_info.update(new_file_details)
            update_file_info(ALL_FILES, all_info)
            with open(UPLOADED_FILES) as f:
                uploaded_files = set(f.read().splitlines())
            uploaded_files = uploaded_files - set(deleted_files)
            with open(UPLOADED_FILES) as f:
                for file in uploaded_files:
                    f.write(file)
                    f.write("\n")
        else:
            extend_file_info(ALL_FILES, new_file_details)

def sync_from_cloud():
    onedrive_uploader = Uploader(FOLDER, REMOTE_FOLDER, UPLOADED_FILES)
    
    files = get_file_info(ALL_FILES) | get_file_info(CLOUD_ONLY_FILES)
    with TemporaryDirectory() as tempdir:
        dir_path = Path(tempdir)
        checkfile = dir_path.joinpath("checkfile.txt")
        write_checkfile(checkfile, files)

        differing_files_path = dir_path.joinpath("differ.txt")
        not_uploaded_files_path = dir_path.joinpath("sync.txt")
        remotely_added_files = dir_path.joinpath("deleted.txt")

        r = subprocess.run(["rclone", "check", checkfile, REMOTE_FOLDER,
                            "--checkcfile", "QuickXorHash",
                            "--differ", str(differing_files_path),
                            "--missing-on-dst", str(not_uploaded_files_path),
                            "--missing-on-src", str(remotely_added_files)], capture_output=True)

        if r.returncode:
            logging.error("A felhővel szinfronizálandó fájlok meghatározása nem sikerült, "
                          "az összehasonlítás meghiúsult. (hibakód: %d, '%s', '%s')",
                          r.returncode, r.stdout, r.stderr)

        with open(differing_files_path) as f:
            bisync_files = f.read().splitlines()

        with open(not_uploaded_files_path) as f:
            upload_files = f.read().splitlines()

        with open(remotely_added_files) as f:
            download_files = set(f.read().splitlines())

    if download_files:
        with open(UPLOADED_FILES) as f:
            uploaded_files = set(f.read().splitlines())
        
        deletion_missed = download_files & uploaded_files
        download_files = download_files - deletion_missed

        if deletion_missed:
            logging.warning("%d fájl törlése elmaradt, pótlás most.", len(deletion_missed))
            data_logger.log(deletion_missed)
            for file in deletion_missed:
                onedrive_uploader.delete_file(file)

        with NamedTemporaryFile() as f:
            f.write("\n".join(download_files))
            r = subprocess.run(["rclone", "copy", REMOTE_FOLDER, FOLDER, "--files-from", f.name], capture_output=True)
            if r.returncode:
                logging.error("Új fájlok letöltése sikertelen. (hibakód: %d, '%s', '%s'),", r.returncode, r.stdout, r.stderr)

    if upload_files:
        with NamedTemporaryFile() as f:
            f.write("\n".join(upload_files))
            r = subprocess.run(["rclone", "copy", FOLDER, REMOTE_FOLDER, "--files-from", f.name], capture_output=True)
            if r.returncode:
                logging.error("Új fájlok feltöltése sikertelen. (hibakód: %d, '%s', '%s'),", r.returncode, r.stdout, r.stderr)
        
    if bisync_files:
        with NamedTemporaryFile() as f:
            f.write(f"+ {path}\n" for path in bisync_files)
            f.write("- *")
            r = subprocess.run(["rclone", "bisync", FOLDER, REMOTE_FOLDER, "--filters-file", f.name, "--resync"], capture_output=True)
            if r.returncode:
                logging.error("Új fájlok feltöltése sikertelen. (hibakód: %d, '%s', '%s'),", r.returncode, r.stdout, r.stderr)
        
