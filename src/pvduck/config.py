import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Optional

import yaml
from pydantic import BaseModel, BeforeValidator, HttpUrl

from pvduck.validators import mandatory_datetime, optional_datetime

XDG_CONFIG = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
XDG_DATA = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

ASSETS_ROOT = Path(__file__).parent.parent.parent / "assets"
CONFIG_ROOT = XDG_CONFIG / "pvduck"
DATA_ROOT = XDG_DATA / "pvduck"

CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
DATA_ROOT.mkdir(parents=True, exist_ok=True)

USER_EDITOR = os.getenv("EDITOR", "nano")


class Config(BaseModel):
    """Configuration for a single sync project.

    Attributes:
        database_path (Path): Path to the DuckDB database file.
    """

    database_path: Path

    base_url: HttpUrl
    sleep_time: int

    start_date: Annotated[datetime, BeforeValidator(mandatory_datetime)]
    end_date: Annotated[Optional[datetime], BeforeValidator(optional_datetime)]
    sample_rate: float


def read_config(project_name: str) -> Config:
    """Read the configuration for a project from the XDG base directory.

    Args:
        project_name (str): Name of the project.

    Returns:
        Config: Configuration object containing the database path.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = CONFIG_ROOT / f"{project_name}.yml"
    data_path = DATA_ROOT / f"{project_name}.duckdb"

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "rb") as f:
        config_data: dict[str, Any] = yaml.safe_load(f)
        config_data["database_path"] = data_path

    return Config.model_validate(config_data, strict=True)


def write_config(
    project_name: str, editor: Optional[str] = None, allow_replace: bool = False
) -> Config:
    """Open an editor to let the user modify the config file.

    Args:
        project_name (str): Name of the project.
        editor (Optional[str]): Path to the editor executable.
            Defaults to the EDITOR environment variable if not set.
        allow_replace (bool): If True, allow replacing the config file
            with a new one. Defaults to False.

    Returns:
        Config: Configuration object containing the database path.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    default_config_path = ASSETS_ROOT / "config.yml"
    project_config_path = CONFIG_ROOT / f"{project_name}.yml"

    if project_config_path.is_file() and not allow_replace:
        raise FileExistsError(f"Config file already exists at {project_config_path}")

    # Allow the user to edit a copy of the file in a temporary directory.
    # If the saved file validates, copy it back to the proper location.
    with tempfile.TemporaryDirectory() as tmpdir:
        src_path = (
            project_config_path
            if project_config_path.is_file()
            else default_config_path
        )
        tmp_path = Path(tmpdir) / f"{project_name}.yml"
        shutil.copy(src_path, tmp_path)

        subprocess.run([editor or USER_EDITOR, str(tmp_path)], check=True)

        with open(tmp_path, "rb") as f:
            config_data: dict[str, Any] = yaml.safe_load(f)
            config_data["database_path"] = DATA_ROOT / f"{project_name}.duckdb"
            Config.model_validate(config_data, strict=True)

        shutil.copy(tmp_path, project_config_path)

    return read_config(project_name)


def list_config_files() -> list[str]:
    """List all config files in the XDG base directory.

    Returns:
        list[str]: List of project names.
    """
    return [f.stem for f in CONFIG_ROOT.glob("*.yml")]


def remove_config(project_name: str, delete_database: bool = False) -> None:
    """Delete the config file for a project.

    Args:
        project_name (str): Name of the project.
        delete_database (bool): If True, delete the associated database file.
            It can't be updated without the config file, but could still be
            useful to keep as a read-only data source.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = CONFIG_ROOT / f"{project_name}.yml"
    data_path = DATA_ROOT / f"{project_name}.duckdb"

    if config_path.is_file():
        config_path.unlink()
    if data_path.is_file() and delete_database:
        data_path.unlink()
