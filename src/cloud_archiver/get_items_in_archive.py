import os
from typing import List

from .archive_item import ArchiveItem


def get_items_in_archive(root_dir: str, archive_folder: str) -> List[ArchiveItem]:

    # Get a list of all items in the archive so we can be ready to transfer it to S3.
    archive_path = os.path.join(root_dir, archive_folder)
    items = []

    # No items to archive.
    if not os.path.exists(archive_path):
        return items

    root_length = len(archive_path) + 1
    for walk_root, walk_dirs, files in os.walk(archive_path):
        root_head = walk_root[root_length:]

        # For each file, also get the path and restore the key.
        for walk_file in files:
            key = os.path.join(root_head, walk_file)
            file_path = os.path.join(walk_root, walk_file)
            items.append(ArchiveItem(key, file_path))

    return items
