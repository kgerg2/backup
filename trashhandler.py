from config import FolderProperties
from util import run_rclone


def handle_trash(folder_properties: FolderProperties) -> None:
    config = folder_properties["config"]
    run_rclone("delete", [config.trash_folder, "--min-age", config.trash_keep_time.total_seconds(),
                "--rmdirs"], config.global_config, use_web_ui=False, strict=False,
                error_message="A lomtárban lévő régi fájlok ürítése sikertelen.")
