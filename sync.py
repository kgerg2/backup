from datetime import datetime, timedelta
import json
from pathlib import Path
from pprint import pprint
from shutil import copy2
from typing import Dict, Tuple
from zoneinfo import ZoneInfo
import requests
import asyncio

API_KEY = "KaK7CFasJCLAoSCsHtZjvEoC7LZwAvqi"
FOLDER_ID = "pfkxq-prxga"
FOLDER = Path("~/bmt").expanduser()
IGNORE_FILE = FOLDER.joinpath(".stignore")
REMOTE_FOLDER = Path("~/bmt-tavoli").expanduser()
FILE_VERSIONS = REMOTE_FOLDER.joinpath(".fileversions")

# def make_request(type, req, params)
#     r = type(f"http://localhost:8384/rest/{req}", params=params, headers={"X-API-Key": API_KEY})
#     try:
#         return r.json()
#     except:
#         return r.text

async def get(req, params={}):
    r = requests.get(f"http://localhost:8384/rest/{req}", params=params, headers={"X-API-Key": API_KEY})
    try:
        return r.json()
    except:
        return r.text

async def post(req, data, params={}):
    # print(req, data, params)
    r = requests.post(f"http://localhost:8384/rest/{req}", json=data, params=params, headers={"X-API-Key": API_KEY})
    # print(r.text)
    try:
        return r.json()
    except:
        return r.text

async def write_json(obj, path):
    with open(path, "w") as f:
        json.dump(obj, f)

    print(f"JSON written to {path}")

async def read_json(path):
    with open(path) as f:
        return json.load(f)

async def get_change(last_event=0, timeout=60):
    return await get("events", {"events": "RemoteChangeDetected,ItemFinished", "since": last_event, "timeout": timeout})


async def get_ignores():
    return (await get("db/ignores", {"folder": FOLDER_ID}))["ignore"]

async def update_ignores(ignores):
    # print(f"Ignores: {ignores}")
    res = await post("db/ignores", {"ignore": list(ignores)}, {"folder": FOLDER_ID})
    print(f"Ignores updated: {res}")
    return res


with open(FILE_VERSIONS) as f:
    latest_versions = json.load(f)

async def main():
    ignores = set(await get_ignores())

    if ignores is None:
        ignores = set()
    last_event = 0

    print(f"{ignores=}")

    print(await get("db/browse", {"folder": FOLDER_ID}))

    loop = asyncio.get_event_loop()

    while True:
        changes = await get_change(last_event)
        if changes:
            print(changes)
            last_event = max(c["id"] for c in changes)

            ignores_changed = False

            for c in changes:
                if c["type"] == "RemoteChangeDetected":
                    latest_versions.pop(c["data"]["path"], None)

                if c["type"] == "ItemFinished" and c["data"]["error"] is None:
                    stat = FOLDER.joinpath(c["data"]["item"]).stat()
                    latest_versions[c["data"]["item"]] = {"time": stat.st_mtime_ns, "size": stat.st_size}
                    ignores.add("/" + c["data"]["item"])
                    ignores_changed = True
                    loop.create_task(write_json(latest_versions, FILE_VERSIONS))

            if ignores_changed:
                loop.create_task(update_ignores(ignores))

        await asyncio.sleep(1)

def is_same(date1, size1, date2, size2):
    return abs(date1 - date2) < timedelta(microseconds=10) and size1 == size2

async def archive():
    archived_versions = await read_json(FILE_VERSIONS)
    global_versions = await get("db/browse", {"folder": FOLDER_ID})
    local_versions = []

    # print(global_versions)
    archived_files = {k: (v["time"], v["size"]) for k, v in archived_versions.items()}
    global_files: Dict[Path, Tuple[datetime, int]] = {}
    local_files: Dict[Path, Tuple[datetime, int]] = {}

    for path in global_versions:
        global_files[path["name"]] = (datetime.fromisoformat(path["modTime"][:26] + path["modTime"][-6:]), path["size"])
        # pprint({
        #     "name": path["name"],
        #     "modTime": datetime.fromisoformat(path["modTime"][:26] + path["modTime"][-6:]),
        #     "size": path["size"]
        # })

    for path in FOLDER.glob("**/*"):
        stat = path.stat()
        local_files[str(path.relative_to(FOLDER))] = (datetime.fromtimestamp(stat.st_mtime).astimezone(ZoneInfo("Europe/Budapest")), stat.st_size)
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
        copy2(FOLDER.joinpath(file), REMOTE_FOLDER.joinpath(file))

        glt, gls = global_files[file]
        archived_versions[file] = {"time": glt.isoformat(), "size": gls}
        

    await write_json(archived_versions, FILE_VERSIONS)

    ignores = await get_ignores()

    ignores.extend(map(lambda x: f"/{x}", to_archive))

    await update_ignores(ignores)

    # Wait for syncthing to be idle

    for file in to_archive:
        if file.is_file():
            file.unlink()

        if file.is_dir():
            file.unlink()
    
        print(f"Unexpected thing: {file} not file and not dir. Might have been deleted.")

# rclone copy Képek onedrive-kifu:Képek --exclude /.st**

asyncio.run(archive())