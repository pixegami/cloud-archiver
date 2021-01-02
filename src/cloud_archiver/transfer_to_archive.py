import os
import shutil
import stat
from datetime import datetime
from typing import Dict
from rich.console import Console

from .archive_path import ArchivePath


def transfer_to_archive(paths: Dict[str, ArchivePath], root_dir: str, archive_dir: str):
    console = Console()

    # Ensure that the archive folder exists.
    archive_path = os.path.join(root_dir, archive_dir)
    os.makedirs(archive_path, exist_ok=True)
    console.print(f"Archive directory created at [green]{archive_path}[/green].")
    archive_items = []
    n = 0

    for path in paths.values():
        if not path.should_archive or path.is_dir:
            continue

        archive_key = _create_archive_key(path.key)
        archive_key_path = os.path.join(archive_key, path.key)
        archive_file_path = os.path.join(archive_path, archive_key_path)

        archive_file_dir = os.path.dirname(archive_file_path)
        os.makedirs(archive_file_dir, exist_ok=True)

        console.print(
            f"Moving [yellow]{path.key}[/yellow] to [blue]{archive_file_path}[/blue]."
        )
        shutil.move(path.key, archive_file_path)
        archive_items.append((archive_key_path, archive_file_path))
        n += 1

    console.print(f"Transferred {n} files to [green]{archive_path}[/green].")
    return archive_items


def _create_archive_key(path: str):
    file_stats_result = os.stat(path)
    access_time = file_stats_result[stat.ST_ATIME]
    access_date = datetime.fromtimestamp(access_time)
    key = os.path.join(str(access_date.year), str(access_date.month).zfill(2))
    return key