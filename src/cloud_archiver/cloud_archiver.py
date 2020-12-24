import json
import os
import time
import stat
import math
import uuid
from datetime import datetime
from typing import Dict
import shutil
import boto3

from .display_paths import display_paths
from .archive_path import ArchivePath
from rich import box
from rich.console import Console
from rich.padding import Padding
from rich.progress import Progress
from rich.prompt import Confirm, Prompt
from rich.table import Table


ARCHIVE_DIRECTORY = ".archive"
CONFIG_PATH = ".archive_config.json"


class CloudArchiver:

    SECONDS_PER_DAY = 86400

    def __init__(self):
        self.console = Console()
        pass

    def traverse(self, root_path: str, threshold_days: int = 1) -> Dict[str, ArchivePath]:

        # Shortlist paths to archive.
        paths = {}

        # Scan within this directory.
        for partial_path in os.listdir(root_path):
            key = os.path.join(root_path, partial_path)
            days_since_last_access = self._days_since_last_access(key)
            should_archive = days_since_last_access is not None and days_since_last_access >= threshold_days
            should_ignore = partial_path == ARCHIVE_DIRECTORY or partial_path == CONFIG_PATH

            # Add sub paths.
            if should_archive and not should_ignore:
                for sub_key in self._paths_in(key):
                    if key != sub_key:  # Only add sub-paths, and not actual path.
                        paths[sub_key] = ArchivePath(
                            sub_key, days_since_last_access, should_archive=True,
                            is_root=False, is_dir=False, is_ignored=False)

            # Add this root directory.
            paths[key] = ArchivePath(
                key, days_since_last_access, should_archive=should_archive,
                is_root=True, is_dir=os.path.isdir(key), is_ignored=should_ignore)

        return paths

    def _paths_in(self, path: str):
        # Get all file paths within this path.
        arr = []
        if os.path.isdir(path):
            for child in os.listdir(path):
                sub_path = os.path.join(path, child)
                arr += self._paths_in(sub_path)
        else:
            arr.append(path)
        return arr

    def _days_since_last_access(self, path: str):
        # If this is a directory, the day of last access is the LATEST access date of all files in here.
        if os.path.isdir(path):
            latest_date = None
            for child in os.listdir(path):
                sub_path = os.path.join(path, child)
                sub_path_last_access = self._days_since_last_access(sub_path)

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
            access_delta_days = self._convert_seconds_to_days(access_delta_seconds)
            return access_delta_days

    def _convert_seconds_to_days(self, seconds: float):
        return math.floor(seconds / self.SECONDS_PER_DAY)

    def transfer_to_archive(self, paths: Dict[str, ArchivePath], archive_dir: str):
        # Ensure that the archive folder exists.
        archive_path = os.path.join(archive_dir, ARCHIVE_DIRECTORY)
        os.makedirs(archive_path, exist_ok=True)
        self.console.print(f"Archive directory created at [green]{archive_path}[/green].")
        archive_items = []
        n = 0

        for path in paths.values():
            if not path.should_archive or path.is_dir:
                continue

            archive_key = self._create_archive_key(path.key)
            archive_key_path = os.path.join(archive_key, path.key)
            archive_file_path = os.path.join(archive_path, archive_key_path)

            archive_file_dir = os.path.dirname(archive_file_path)
            os.makedirs(archive_file_dir, exist_ok=True)

            self.console.print(f"Moving [yellow]{path.key}[/yellow] to [blue]{archive_file_path}[/blue].")
            shutil.move(path.key, archive_file_path)
            archive_items.append((archive_key_path, archive_file_path))
            n += 1

        self.console.print(f"Transferred {n} files to [green]{archive_path}[/green].")
        return archive_items

    @staticmethod
    def get_items_in_archive(archive_dir: str):

        # Get a list of all items in the archive so we can be ready to transfer it to S3.
        archive_path = os.path.join(archive_dir, ARCHIVE_DIRECTORY)
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
                items.append((key, file_path))

        return items

    @staticmethod
    def _create_archive_key(path: str):
        file_stats_result = os.stat(path)
        access_time = file_stats_result[stat.ST_ATIME]
        access_date = datetime.fromtimestamp(access_time)
        key = os.path.join(str(access_date.year), str(access_date.month).zfill(2))
        return key

    def upload(self, bucket_name: str, archive_items: list):
        # Upload everything under archive_path to S3.

        s3_client = self.get_s3_client()
        s3_client.create_bucket(Bucket=bucket_name)

        try:
            with Progress() as progress:
                task = progress.add_task("[green]Upload", total=len(archive_items))
                for key, path in archive_items:
                    s3_client.upload_file(path, bucket_name, key)
                    progress.update(task, advance=1)
            self.console.print(f"Uploaded {len(archive_items)} files to [green]{bucket_name}[/green].")
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False

    @staticmethod
    def get_s3_client():
        return boto3.client('s3')

    def load_config(self):
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                config = json.load(f)
        else:
            self.console.print(f"Config file not found at [green]{os.path.join(os.getcwd(), CONFIG_PATH)}[/green].")
            unique_id = uuid.uuid4().hex[:12]
            default_bucket_name = os.path.basename(os.getcwd())
            default_bucket_name = default_bucket_name.strip().strip("/").strip(".").lower()
            default_bucket_name = f"px-archive.{unique_id}.{default_bucket_name}"
            bucket_name = Prompt.ask("Enter bucket to use", default=default_bucket_name)

            config = {
                "bucket": bucket_name,
                "days": 90
            }

            with open(CONFIG_PATH, "w") as f:
                json.dump(config, f, indent=2)

        my_session = boto3.session.Session()
        region = my_session.region_name
        profile = my_session.profile_name

        table = Table(show_header=False, box=box.MINIMAL)
        table.add_row("[green]Bucket", config["bucket"])
        table.add_row("[green]Days", str(config["days"]))
        table.add_row("[green]AWS Profile", str(profile))
        table.add_row("[green]AWS Region", str(region))
        self.console.print(table)

        return config["bucket"], config["days"]

    def section(self, title: str, description: str=None):
        self.console.rule(f"[bold]{title}")
        if description is not None:
            self.print(f"[dim]{description}")

    def print(self, text: str):
        self.console.print(Padding(text, 1))


def configure():
    archiver = CloudArchiver()
    archiver.load_config()


def main():
    archiver = CloudArchiver()
    root_path = "."

    archiver.section(
        "Configuration",
        f"This is the current configuration for px-archiver at this directory {os.getcwd()}. "
        f"You can edit this configuration at {CONFIG_PATH}.")
    bucket, days = archiver.load_config()

    # File traversing.
    archiver.section(
        "Directory Analysis",
        f"Scanning for files and folders in this directory which haven't been accessed for over {days} days."
    )
    paths = archiver.traverse(root_path, days)
    display_paths(root_path, paths)
    n_archive_files = sum([1 for x in paths.values() if x.should_archive and not x.is_dir])
    archive_path = os.path.abspath(ARCHIVE_DIRECTORY)

    # File archiving.

    if n_archive_files == 0:
        archiver.print("No new files require archiving.")
    else:
        should_archive = Confirm.ask(f"Do you want to move {n_archive_files} files to archive ({archive_path})?")
        if should_archive:
            archiver.transfer_to_archive(paths, root_path)
        else:
            archiver.print("No files moved.")

    # File upload.
    archiver.section("Uploading")
    archived_items = archiver.get_items_in_archive(root_path)
    upload_failure = True
    if len(archived_items) == 0:
        archiver.print(f"No archived files in {archive_path} to upload.")
    else:
        archiver.print(f"There's currently {len(archived_items)} files in the archive.")
        should_upload = Confirm.ask(
            f"Do you want to upload them to S3 bucket [green]{bucket}?")
        if not should_upload:
            archiver.print(f"No files uploaded. {len(archived_items)} files will remain in local archive.")
            upload_failure = False
        else:
            upload_success = archiver.upload(bucket, archived_items)
            upload_failure = not upload_success

    # File deletion.
    archiver.section("Deletion")
    if len(archived_items) > 0 and not upload_failure:
        archiver.print(f"There's currently {len(archived_items)} files in the archive.")
        should_delete = Confirm.ask(
            f"Do you want to [red]permanently delete[/red] these {len(archived_items)} files locally?")
        if should_delete:
            for key, path in archived_items:
                os.remove(path)
            archiver.print(f"{len(archived_items)} files delete from local archive.")
        else:
            archiver.print(f"No files deleted.")
    else:
        archiver.print(f"No files to be deleted.")


if __name__ == "__main__":
    configure()
