import os
from pathlib import Path
from typing import List

from rich import box
from rich.padding import Padding

from .archive_item import ArchiveItem
from rich.console import Console
from rich.table import Table

S3_PRICING_DOLLAR_PER_GB_MONTH = 0.023


def display_archive_items(items: List[ArchiveItem], estimate_cost: bool = False):
    # Given a list of ArchiveItems, display them in a table.

    console = Console()
    table = Table(show_header=True, header_style="bold", box=box.HEAVY_EDGE)
    table.add_column(f"Path")
    table.add_column("Size", justify="right")

    # This is the most items we'll display.
    max_display = 24
    truncated_items = 0

    sorted_items: List[ArchiveItem] = sorted(
        items,
        key=lambda x: x.key,
        reverse=True)

    total_size = sum([_size_of(item.path) for item in items])

    if len(sorted_items) > max_display:
        truncated_items = len(sorted_items) - max_display
        sorted_items = sorted_items[:max_display]

    for item in sorted_items:
        sub_key = "/".join(item.key.split("/")[2:])
        size = _size_of(item.path)

        table.add_row(
            sub_key,
            _human_readable_bytes(size)
        )

    if truncated_items > 0:
        table.add_row(
            f"[green]+{truncated_items} more item(s)...[/green]",
            ""
        )

    console.print(table)

    # Summary Calculation
    if estimate_cost:
        storage_gb = total_size / (1024 * 1024)  # Convert to GB
        estimated_storage_cost = storage_gb * S3_PRICING_DOLLAR_PER_GB_MONTH
        console.print(Padding(f"Total Size: [blue]{_human_readable_bytes(total_size)}[/blue]\n"
                              f"[default]Est. Monthly Storage Cost: [blue]${estimated_storage_cost:.6f}", 1))


def _size_of(path: str):
    path = Path(path)
    if path.is_file():
        return path.stat().st_size
    return 0


def _human_readable_bytes(num: int, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
