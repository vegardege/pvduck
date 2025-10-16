import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Optional

import yaml
from pydantic import BaseModel, BeforeValidator

from pvduck.validators import mandatory_datetime, optional_datetime

XDG_CONFIG = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
XDG_DATA = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

ASSETS_ROOT = Path(__file__).parent.parent.parent / "assets"
CONFIG_ROOT = XDG_CONFIG / "pvduck"
DATA_ROOT = XDG_DATA / "pvduck"

CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
DATA_ROOT.mkdir(parents=True, exist_ok=True)


class Config(BaseModel):
    """Configuration for a single sync project."""

    database_path: Path

    base_url: str
    sleep_time: int

    start_date: Annotated[datetime, BeforeValidator(mandatory_datetime)]
    end_date: Annotated[Optional[datetime], BeforeValidator(optional_datetime)]
    sample_rate: float

    line_regex: Optional[str]
    domain_codes: Optional[list[str]]
    page_title: Optional[str]
    min_views: Optional[int]
    max_views: Optional[int]
    languages: Optional[list[str]]
    domains: Optional[list[str]]
    mobile: Optional[bool]


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

    with open(config_path, "rb") as f:
        config_data: dict[str, Any] = yaml.safe_load(f)
        config_data["database_path"] = data_path

    return Config.model_validate(config_data, strict=True)


def write_config(project_name: str, replace_existing: bool = False) -> Config:
    """Open an editor to let the user modify the config file.

    Args:
        project_name (str): Name of the project.
        replace_existing (bool): If True, allow replacing the config file
            with a new one. Defaults to False.

    Returns:
        Config: Configuration object containing the database path.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    default_config_path = ASSETS_ROOT / "default_config.yml"
    project_config_path = CONFIG_ROOT / f"{project_name}.yml"

    if project_config_path.is_file() and not replace_existing:
        raise FileExistsError(
            f"Config file already exists at {project_config_path}"
        )
    elif not project_config_path.is_file() and replace_existing:
        raise FileNotFoundError(
            f"Config file does not exist at {project_config_path}"
        )

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

        subprocess.run(
            [os.getenv("EDITOR", "nano"), str(tmp_path)],
            check=True,
        )

        with open(tmp_path, "rb") as f:
            config_data: dict[str, Any] = yaml.safe_load(f)
            config_data["database_path"] = DATA_ROOT / f"{project_name}.duckdb"
            Config.model_validate(config_data, strict=True)

        shutil.copy(tmp_path, project_config_path)

    return read_config(project_name)


def config_path(project_name: str) -> Path:
    """Get the path to the config file for a project.

    Args:
        project_name (str): Name of the project.

    Returns:
        Path: Path to the config file.
    """
    return CONFIG_ROOT / f"{project_name}.yml"


def database_path(project_name: str) -> Path:
    """Get the path to the database file for a project.

    Args:
        project_name (str): Name of the project.

    Returns:
        Path: Path to the database file.
    """
    return DATA_ROOT / f"{project_name}.duckdb"
