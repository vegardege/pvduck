import sys
import time
from typing import Annotated, Optional

import typer
from rich import print

from pvduck.config import read_config, write_config
from pvduck.db import (
    compact_db,
    create_db,
    read_log_timestamps,
    update_from_parquet,
    update_log,
)
from pvduck.project import list_projects, open_database, remove_project
from pvduck.stream import parquet_from_url
from pvduck.timeseries import timeseries
from pvduck.wikimedia import url_from_timestamp

app = typer.Typer()


@app.command()
def create(project_name: str) -> None:
    """Create a new project."""
    try:
        config = write_config(project_name, replace_existing=False)
        create_db(config.database_path)
        print(f"Project '{project_name}' created")
    except FileExistsError:
        print(f"[bold red]Error:[/bold red] Project {project_name} already exists")
        sys.exit(2)


@app.command()
def edit(project_name: str) -> None:
    """Edit project config."""
    try:
        write_config(project_name, replace_existing=True)
        print(f"Project '{project_name}' updated")
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)


@app.command()
def open(project_name: str) -> None:
    """Open the database in duckdb."""
    try:
        open_database(project_name)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)


@app.command()
def rm(project_name: str) -> None:
    """Delete a project, config and database."""
    try:
        remove_project(project_name, delete_database=True)
        print(f"Project '{project_name}' deleted")
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)


@app.command()
def sync(
    project_name: str,
    max_files: Annotated[
        Optional[int],
        typer.Argument(help="The maximum number of files to process"),
    ] = None,
) -> None:
    """Download, parse, and sync pageviews files."""
    try:
        config = read_config(project_name)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)

    seen = read_log_timestamps(config.database_path)
    ts = timeseries(config.start_date, config.end_date, config.sample_rate)

    file_count = 0
    for timestamp in ts:
        try:
            if timestamp in seen:
                continue  # Already processed

            if max_files and file_count >= max_files:
                print(f"[bold yellow]Max files reached:[/bold yellow] {max_files}")
                break

            file_count += 1

            print(f"Processing '{timestamp}'")
            url = url_from_timestamp(config.base_url, timestamp)

            print(f"Downloading from '{url}'")
            with parquet_from_url(
                url,
                line_regex=config.line_regex,
                domain_codes=config.domain_codes,
                page_title=config.page_title,
                min_views=config.min_views,
                max_views=config.max_views,
                languages=config.languages,
                domains=config.domains,
                mobile=config.mobile,
            ) as parquet:
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

            print(f"Sleeping for {config.sleep_time} seconds")
            time.sleep(config.sleep_time)

        except Exception as e:
            update_log(
                config.database_path,
                timestamp,
                success=False,
                error=str(e),
            )
            print(f"[bold red]Error:[/bold red] {e}")

    print(f"Project '{project_name}' synced")


@app.command()
def status(project_name: str) -> None:
    """See a status overview of the project."""
    try:
        config = read_config(project_name)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)

    seen = read_log_timestamps(config.database_path)
    ts = timeseries(config.start_date, config.end_date, config.sample_rate)
    errors = read_log_timestamps(config.database_path, success=False)

    print(f"- Progress: {len(seen)}/{len(ts)} ({len(seen) / len(ts) * 100:.2f}%)")
    print(f"- Errors:   {len(errors)}")


@app.command()
def compact(project_name: str) -> None:
    """Compact the database."""
    try:
        config = read_config(project_name)
        compact_db(config.database_path)
    except FileNotFoundError:
        print(f"[bold red]Error:[/bold red] Project {project_name} does not exist")
        sys.exit(2)

    print(f"Project '{project_name}' compacted")


@app.command()
def ls() -> None:
    """List all projects."""
    for project_name in list_projects():
        print(f"- {project_name}")
