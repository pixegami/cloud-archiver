import json
import os
import uuid

import boto3
from rich import box
from rich.prompt import Prompt
from rich.console import Console
from rich.table import Table


def load_config(config_path: str) -> (str, int):
    console = Console()

    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config = json.load(f)
    else:
        console.print(f"Config file not found at [green]{os.path.join(os.getcwd(), config_path)}[/green].")
        unique_id = uuid.uuid4().hex[:12]
        default_bucket_name = os.path.basename(os.getcwd())
        default_bucket_name = ''.join(ch for ch in default_bucket_name if ch.isalnum())
        default_bucket_name = default_bucket_name.strip().lower()
        default_bucket_name = f"px-archive.{unique_id}.{default_bucket_name}"
        bucket_name = Prompt.ask("Enter bucket to use", default=default_bucket_name)

        config = {
            "bucket": bucket_name,
            "days": 90
        }

        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

    my_session = boto3.session.Session()
    region = my_session.region_name
    profile = my_session.profile_name

    table = Table(show_header=False, box=box.MINIMAL)
    table.add_row("[green]Bucket", config["bucket"])
    table.add_row("[green]Days", str(config["days"]))
    table.add_row("[green]AWS Profile", str(profile))
    table.add_row("[green]AWS Region", str(region))
    console.print(table)

    return config["bucket"], config["days"]
