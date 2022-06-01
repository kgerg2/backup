import logging
from pathlib import Path
import subprocess

TRASH_FOLDER = Path("~/bmt/")
TRASH_KEEP_DAYS = 60

def handle_trash():
    r = subprocess.run(["rclone", "delete", str(TRASH_FOLDER), "--min-age", TRASH_KEEP_DAYS, "--rmdirs"], capture_output=True)

    if r.exit_code != 0:
        logging.error("A lomtárban lévő régi fájlok ürítése sikertelen. (hibakód: %d, '%s', '%s')", r.exit_code, r.stdout, r.stderr)