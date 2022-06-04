from pathlib import Path

from util import run_command

TRASH_FOLDER = Path("~/bmt/")
TRASH_KEEP_DAYS = 60

def handle_trash():
    run_command(["rclone", "delete", TRASH_FOLDER, "--min-age", TRASH_KEEP_DAYS, "--rmdirs"],
                error_message="A lomtárban lévő régi fájlok ürítése sikertelen.")