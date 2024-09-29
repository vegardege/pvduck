import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

XDG_CONFIG = Path(os.getenv("XDG_CONFIG_HOME", Path.home() / ".config"))
XDG_DATA = Path(os.getenv("XDG_DATA_HOME", Path.home() / ".local" / "share"))

ASSETS_ROOT = Path(__file__).parent.parent.parent / "assets"
CONFIG_ROOT = XDG_CONFIG / "pvduck"
DATA_ROOT = XDG_DATA / "pvduck"

CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
DATA_ROOT.mkdir(parents=True, exist_ok=True)

USER_EDITOR = os.getenv("EDITOR", "nano")


@dataclass
class Config:
    """Configuration for a single sync project.

    Attributes:
        database_path (Path): Path to the DuckDB database file.
    """

    database_path: Path


def read_config(project_name: str) -> Config:
    """Read the configuration for a project from the XDG base directory.

    Args:
        project_name (str): Name of the project.

    Returns:
        Config: Configuration object containing the database path.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = CONFIG_ROOT / f"{project_name}.toml"
    data_path = DATA_ROOT / f"{project_name}.duckdb"

    if not config_path.is_file():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    return Config(
        database_path=data_path,
    )


def update_config(
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
    config_path = CONFIG_ROOT / f"{project_name}.toml"

    if config_path.is_file() and not allow_replace:
        raise FileExistsError(f"Config file already exists at {config_path}")
    elif not config_path.is_file():
        _create_default_config(config_path)

    subprocess.run([editor or USER_EDITOR, str(config_path)], check=True)

    return read_config(project_name)


def _create_default_config(target_path: Path) -> None:
    """Copy the default config file to a project specific location.

    Args:
        target_path (Path): Path to the target config file.
    """
    src_path = ASSETS_ROOT / "default_config.toml"
    shutil.copy(src_path, target_path)
