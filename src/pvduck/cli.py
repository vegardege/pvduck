import sys
import time
from datetime import datetime
from pathlib import Path

import typer
from rich import print

from pvduck.config import list_config_files, read_config, remove_config, write_config
from pvduck.db import (
    compact_db,
    create_db,
    read_log_timestamps,
    update_from_parquet,
    update_log,
)
from pvduck.stream import parquet_from_url
from pvduck.timeseries import timeseries
from pvduck.wikimedia import url_from_timestamp

app = typer.Typer()


@app.command()
def create(project_name: str) -> None:
    """Create a new project."""
    try:
        config = write_config(project_name, allow_replace=False)
        create_db(config.database_path)
        print(f"Project '{project_name}' created")
    except FileExistsError:
        print(f"[bold red]Error:[/bold red] Project {project_name} already exists")
        sys.exit(2)
    except Exception as e:
        print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@app.command()
def ls() -> None:
    """List all projects."""
    for project_name in list_config_files():
        print(f"- {project_name}")


@app.command()
def edit(project_name: str) -> None:
    """Edit project config."""
    try:
        write_config(project_name, allow_replace=True)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)
    except Exception as e:
        print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@app.command()
def rm(project_name: str) -> None:
    """Delete a project, config and database."""
    try:
        # Ask if the user is sure
        if not typer.confirm(
            f"Are you sure you want to delete the project '{project_name}'?\n"
            "This action cannot be undone."
        ):
            print("Aborted")
            return

        delete_database = typer.confirm(f"Do you want to delete the database?")

        remove_config(project_name, delete_database=delete_database)
        print(f"Project '{project_name}' deleted")
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)
    except Exception as e:
        print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)


@app.command()
def compact(project_name: str) -> None:
    """Compact the database."""
    try:
        config = read_config(project_name)
        compact_db(config.database_path)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)
    except Exception as e:
        print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    print(f"Project '{project_name}' compacted")


@app.command()
def sync(project_name: str) -> None:
    """Download, parse, and sync pageviews files."""
    try:
        config = read_config(project_name)

        seen = read_log_timestamps(config.database_path)
        ts = timeseries(config.start_date, config.end_date, config.sample_rate)

        for timestamp in ts:
            if timestamp in seen:
                continue  # Already processed

            print(f"Processing '{timestamp}'")
            url = url_from_timestamp(timestamp=timestamp)

            print(f"Downloading from '{url}'")
            with parquet_from_url(url) as parquet:
                print("Updating database")
                update_from_parquet(
                    config.database_path,
                    parquet,
                )

            update_log(
                config.database_path,
                timestamp,
                success=True,
            )
            print(f"Updated from {parquet}")

            time.sleep(config.sleep_time)

    except Exception as e:
        update_log(
            config.database_path,
            timestamp,
            success=False,
            error=str(e),
        )
        print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    print(f"Project '{project_name}' synced")
