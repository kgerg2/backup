from config import FolderProperties
from util import run_command


def handle_trash(folder_properties: FolderProperties) -> None:
    config = folder_properties["config"]
    run_command(["rclone", "delete", config.trash_folder, "--min-age", config.trash_keep_time,
                "--rmdirs"], config.global_config,
                error_message="A lomtárban lévő régi fájlok ürítése sikertelen.")
