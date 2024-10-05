import subprocess
from dataclasses import dataclass

from pvduck import config


@dataclass
class ProjectStatus:
    """Status of a project."""


def open_database(project_name: str) -> None:
    """Open the database in DuckDB.

    Args:
        project_name (str): Name of the project.
    """
    db_path = config.database_path(project_name)

    if not db_path.is_file():
        raise FileNotFoundError(f"Database does not exist at {db_path}")

    subprocess.run(
        ["duckdb", str(db_path)],
        check=True,
    )


def remove_project(project_name: str, delete_database: bool = False) -> None:
    """Delete the config file for a project.

    Args:
        project_name (str): Name of the project.
        delete_database (bool): If True, delete the associated database file.
            It can't be updated without the config file, but could still be
            useful to keep as a read-only data source.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = config.config_path(project_name)
    database_path = config.database_path(project_name)

    if not config_path.is_file() or not database_path.is_file():
        raise FileNotFoundError(f"Project {project_name} does not exist")

    if config_path.is_file():
        config_path.unlink()
    if database_path.is_file() and delete_database:
        database_path.unlink()


def list_projects() -> list[str]:
    """List all config files in the XDG base directory.

    Returns:
        list[str]: List of project names.
    """
    return [f.stem for f in config.CONFIG_ROOT.glob("*.yml")]
