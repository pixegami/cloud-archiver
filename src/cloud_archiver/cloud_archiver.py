import argparse
import os
from typing import List, Dict

import boto3

from .file_generator import generate_test_set
from .archive_item import ArchiveItem
from .archive_path import ArchivePath
from .get_items_in_archive import get_items_in_archive
from .analyze_directory import analyze_directory
from .transfer_to_achive import transfer_to_archive
from .upload_archive import upload
from .load_config import load_config
from .display_paths import display_paths
from rich.console import Console
from rich.padding import Padding
from rich.prompt import Confirm

ARCHIVE_FOLDER = ".archive"
CONFIG_FILE = ".archive_config.json"
CONSOLE = Console()


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--generate",
                        action="store_true",
                        help="Generates some random text files in this directory.")

    parser.add_argument("--config",
                        action="store_true",
                        help="Shows (or creates) the current config.")

    args = parser.parse_args()
    if args.generate:
        generate_test_files()
    elif args.config:
        configure()
    else:
        archive_app()


def archive_app():
    root_path = "."

    _console_section(
        "Configuration",
        f"This is the current configuration for px-archiver at this directory {os.getcwd()}. "
        f"You can edit this configuration at {CONFIG_FILE}.")
    bucket, days = load_config(CONFIG_FILE)

    # Analyze which files to archive.
    archive_paths = archiver_analyze(root_path, days)

    # File transfer.
    archiver_transfer(root_path, archive_paths)

    # Get items still in archive.
    archived_items = get_items_in_archive(root_path, ARCHIVE_FOLDER)

    # File upload.
    upload_success = archiver_upload(bucket, archived_items)

    # File deletion.
    if upload_success:
        archiver_clean(archived_items)


def generate_test_files():
    _console_print(f"Generating test files in {os.getcwd()}!")
    generate_test_set(os.getcwd())


def configure():
    load_config(CONFIG_FILE)


def archiver_analyze(root_path: str, days: int) -> Dict[str, ArchivePath]:
    _console_section(
        "Directory Analysis",
        f"Scanning for files and folders in this directory which haven't been accessed for over {days} days."
    )
    archive_paths = analyze_directory(root_path, [ARCHIVE_FOLDER, CONFIG_FILE], days)
    display_paths(root_path, archive_paths)
    return archive_paths


def archiver_transfer(root_path: str, archive_paths: Dict[str, ArchivePath]):
    n_archive_files = sum([1 for x in archive_paths.values() if x.should_archive and not x.is_dir])
    if n_archive_files == 0:
        _console_print("No new files require archiving.")
    else:
        should_archive = Confirm.ask(f"Do you want to move {n_archive_files} "
                                     f"files to archive ({os.path.abspath(ARCHIVE_FOLDER)})?")
        if should_archive:
            transfer_to_archive(archive_paths, root_path, ARCHIVE_FOLDER)
        else:
            _console_print("No files moved.")


def archiver_upload(bucket: str, archived_items: List[ArchiveItem]) -> bool:
    _console_section("Uploading")
    upload_success = False
    if len(archived_items) == 0:
        _console_print(f"No archived files in {os.path.abspath(ARCHIVE_FOLDER)} to upload.")
    else:
        _console_print(f"There's currently {len(archived_items)} files in the archive.")
        should_upload = Confirm.ask(
            f"Do you want to upload them to S3 bucket [green]{bucket}?")
        if not should_upload:
            _console_print(f"No files uploaded. {len(archived_items)} files will remain in local archive.")
            upload_success = True
        else:
            client = boto3.client('s3')
            upload_success = upload(client, bucket, archived_items)

    return upload_success


def archiver_clean(archived_items: List[ArchiveItem]):
    _console_section("Deletion")
    if len(archived_items) > 0:
        _console_print(f"There's currently {len(archived_items)} files in the archive.")
        should_delete = Confirm.ask(
            f"Do you want to [red]permanently delete[/red] these {len(archived_items)} files locally?")
        if should_delete:
            for item in archived_items:
                os.remove(item.path)
            _console_print(f"{len(archived_items)} files deleted from local archive.")
        else:
            _console_print(f"No files deleted.")
    else:
        _console_print(f"No files to be deleted.")


def _console_section(title: str, description: str = None):
    CONSOLE.rule(f"[bold]{title}")
    if description is not None:
        _console_print(f"[dim]{description}")


def _console_print(text: str):
    CONSOLE.print(Padding(text, 1))


if __name__ == "__main__":
    main()
