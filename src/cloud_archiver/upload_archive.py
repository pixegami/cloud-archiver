from typing import List

from botocore.client import BaseClient
from rich.progress import Progress
from rich.console import Console

from.archive_item import ArchiveItem


def upload(s3_client: BaseClient, bucket_name: str, archive_items: List[ArchiveItem]):

    # Upload everything under archive_path to S3.
    s3_client.create_bucket(Bucket=bucket_name)
    console = Console()

    try:
        with Progress() as progress:
            task = progress.add_task("[green]Upload", total=len(archive_items))
            for item in archive_items:
                s3_client.upload_file(item.path, bucket_name, item.key)
                progress.update(task, advance=1)
        console.print(f"Uploaded {len(archive_items)} files to [green]{bucket_name}[/green].")
        return True
    except Exception as e:
        console.log(f"ERROR: Failed uploading to S3: {e}")
        return False
