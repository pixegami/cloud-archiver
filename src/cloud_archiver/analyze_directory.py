import math
import os
import stat
import time
from typing import Dict, List

from .archive_path import ArchivePath


def analyze_directory(
    root_path: str, ignore_paths: List[str], threshold_days: int = 1
) -> Dict[str, ArchivePath]:
    # Shortlist paths to archive.
    paths = {}

    # Scan within this directory.
    for partial_path in os.listdir(root_path):

        key = os.path.join(root_path, partial_path)
        days_since_last_access = _days_stale(key)
        should_ignore = partial_path in ignore_paths
        should_archive = days_since_last_access >= threshold_days and not should_ignore

        # Add this root directory.
        paths[key] = ArchivePath(
            key,
            days_since_last_access,
            should_archive=should_archive,
            is_root=True,
            is_dir=os.path.isdir(key),
            is_ignored=should_ignore,
        )

        # Add sub paths.
        if should_archive:
            for file in _all_files_in_dir(key):
                paths[file] = ArchivePath(
                    file,
                    days_since_last_access,
                    should_archive=True,
                    is_root=False,
                    is_dir=False,
                    is_ignored=False,
                )

    return paths


def _days_stale(path: str):
    if os.path.isfile(path):
        return _days_stale_of_file(path)
    else:
        return _days_stale_of_directory(path)


def _all_files_in_dir(path: str):
    files = []
    for r, _, f in os.walk(path):
        for file in f:
            sub_path = os.path.join(r, file)
            files.append(sub_path)
    return files


def _days_stale_of_file(file_path: str):
    file_stats_result = os.stat(file_path)
    access_time = file_stats_result[stat.ST_ATIME]
    access_delta_seconds = time.time() - access_time
    days_stale = _seconds_to_days(access_delta_seconds)
    return days_stale


def _days_stale_of_directory(directory_path: str):
    latest_days = None
    for file in _all_files_in_dir(directory_path):
        days = _days_stale_of_file(file)
        if latest_days is None or days < latest_days:
            latest_days = days

    return latest_days if latest_days is not None else 0


def _seconds_to_days(seconds: float):
    return math.floor(seconds / 86400)
