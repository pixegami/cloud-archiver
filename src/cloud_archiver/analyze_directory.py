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
        days_since_last_access = _recursive_days_since_last_access(key)
        should_archive = days_since_last_access >= threshold_days
        should_ignore = partial_path in ignore_paths

        # Add sub paths.
        if should_archive and not should_ignore and os.path.isdir(key):
            for r, _, f in os.walk(key):
                for file in f:
                    sub_key = os.path.join(r, file)
                    paths[sub_key] = ArchivePath(
                        sub_key,
                        days_since_last_access,
                        should_archive=True,
                        is_root=False,
                        is_dir=False,
                        is_ignored=False,
                    )

        # Add this root directory.
        paths[key] = ArchivePath(
            key,
            days_since_last_access,
            should_archive=should_archive,
            is_root=True,
            is_dir=os.path.isdir(key),
            is_ignored=should_ignore,
        )

    return paths


def _recursive_days_since_last_access(path: str):
    # If this is a directory, the day of last access is the LATEST access date of all files in here.
    if os.path.isfile(path):
        return _days_since_last_access(path)
    else:
        latest_days = None
        for r, _, f in os.walk(path):
            for file in f:
                sub_path = os.path.join(r, file)
                days = _days_since_last_access(sub_path)
                print(sub_path, days)
                if latest_days is None or days < latest_days:
                    latest_days = days

        return latest_days if latest_days is not None else 0


def _days_since_last_access(file_path: str):
    file_stats_result = os.stat(file_path)
    access_time = file_stats_result[stat.ST_ATIME]
    access_delta_seconds = time.time() - access_time
    access_delta_days = _convert_seconds_to_days(access_delta_seconds)
    return access_delta_days


def _convert_seconds_to_days(seconds: float):
    return math.floor(seconds / 86400)
