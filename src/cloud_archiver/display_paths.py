import os
from pathlib import Path
from typing import Dict, List

from rich import box

from .archive_path import ArchivePath
from rich.console import Console
from rich.table import Table


def display_paths(root_path: str, paths: Dict[str, ArchivePath]):
    # Print in terminal the files we're about to archive.
    console = Console()
    abs_path = os.path.abspath(root_path)
    table = Table(show_header=True, header_style="bold", box=box.HEAVY_EDGE)
    table.add_column(f"Path (from {abs_path})")
    table.add_column("Days Idle", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Archive", justify="right")
    root_path_len = len(root_path) + 1

    sorted_paths: List[ArchivePath] = sorted(
        paths.values(),
        key=lambda x: (not x.ignored, x.should_archive, x.days_since_access),
        reverse=True)

    for item in sorted_paths:
        if not item.is_root:
            continue

        sub_path = item.key[root_path_len:]

        color = "default"
        if item.ignored:
            color = "dim"
        if item.should_archive:
            color = "yellow"

        will_archive = "YES" if item.should_archive else "NO"

        path = Path(item.key)
        size = sum(f.stat().st_size for f in path.glob('**/*') if f.is_file())
        if path.is_file():
            size += path.stat().st_size

        table.add_row(
            _with_color(sub_path, color),
            _with_color(str(item.days_since_access), color),
            _with_color(_human_readable_bytes(size), color),
            _with_color(will_archive, color)
        )

    console.print(table)


def _with_color(x: str, color: str):
    return f"[{color}]{x}[/{color}]"


def _human_readable_bytes(num: int, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)