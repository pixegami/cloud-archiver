import math
import os
import stat
import time
from typing import Dict, List

from .archive_path import ArchivePath


def analyze_directory(root_path: str, ignore_paths: List[str], threshold_days: int = 1) -> Dict[str, ArchivePath]:
    # Shortlist paths to archive.
    paths = {}

    # Scan within this directory.
    for partial_path in os.listdir(root_path):
        key = os.path.join(root_path, partial_path)
        days_since_last_access = _days_since_last_access(key)
        should_archive = days_since_last_access is not None and days_since_last_access >= threshold_days
        should_ignore = partial_path in ignore_paths

        # Add sub paths.
        if should_archive and not should_ignore:
            for sub_key in _paths_in(key):
                if key != sub_key:  # Only add sub-paths, and not actual path.
                    paths[sub_key] = ArchivePath(
                        sub_key, days_since_last_access, should_archive=True,
                        is_root=False, is_dir=False, is_ignored=False)

        # Add this root directory.
        paths[key] = ArchivePath(
            key, days_since_last_access, should_archive=should_archive,
            is_root=True, is_dir=os.path.isdir(key), is_ignored=should_ignore)

    return paths


def _paths_in(path: str):
    # Get all file paths within this path.
    arr = []
    if os.path.isdir(path):
        for child in os.listdir(path):
            sub_path = os.path.join(path, child)
            arr += _paths_in(sub_path)
    else:
        arr.append(path)
    return arr


def _days_since_last_access(path: str):
    # If this is a directory, the day of last access is the LATEST access date of all files in here.
    if os.path.isdir(path):
        latest_date = None
        for child in os.listdir(path):
            sub_path = os.path.join(path, child)
            sub_path_last_access = _days_since_last_access(sub_path)

            if sub_path_last_access is None:
                continue

            if latest_date is None or sub_path_last_access < latest_date:
                latest_date = sub_path_last_access

        # Return the latest access date, or 0 if no files were found.
        return latest_date if latest_date is not None else 0
    else:
        file_stats_result = os.stat(path)
        access_time = file_stats_result[stat.ST_ATIME]
        access_delta_seconds = time.time() - access_time
        access_delta_days = _convert_seconds_to_days(access_delta_seconds)
        return access_delta_days


def _convert_seconds_to_days(seconds: float):
    return math.floor(seconds / 86400)
