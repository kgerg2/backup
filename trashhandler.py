from config import TRASH_FOLDER, TRASH_KEEP_DAYS
from util import run_command


def handle_trash():
    run_command(["rclone", "delete", TRASH_FOLDER, "--min-age", str(TRASH_KEEP_DAYS), "--rmdirs"],
                error_message="A lomtárban lévő régi fájlok ürítése sikertelen.")
