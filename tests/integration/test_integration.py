import io
import random
import string
import sys
from typing import Callable, Optional
from unittest.mock import patch

import duckdb
import pytest
from _pytest.monkeypatch import MonkeyPatch

from pvduck.cli import compact, create, edit, ls, rm, sync
from pvduck.config import CONFIG_ROOT, DATA_ROOT


@pytest.mark.integration
def test_full_run(monkeypatch: MonkeyPatch) -> None:
    """Test a full run of the cli tool.

    This test downloads a big file from a remote server. Do not call it more
    often than necessary.
    """
    project_name = "".join(random.choices(string.ascii_lowercase, k=10))

    # Temporarily set the EDITOR to "true" to auto-save the config file
    monkeypatch.setenv("EDITOR", "true")

    # Create the project and ensure the files exist
    create(project_name)
    assert _project_exists(project_name)

    # Make sure we can't re-create an existing project
    with pytest.raises(SystemExit):
        create(project_name)

    # Edit the project and ensure the config file is updated
    edit(project_name)
    assert _project_exists(project_name)

    # Before syncing, the database should be empty
    assert _pageviews_count(project_name) == 0
    assert _main_page_views(project_name) is None
    assert _log_count(project_name) == 0

    # Run a full sync
    sync(project_name, max_files=1)
    pv_count_first_sync = _pageviews_count(project_name)
    main_page_first_sync = _main_page_views(project_name)

    assert pv_count_first_sync > 0
    assert main_page_first_sync is not None
    assert main_page_first_sync > 0
    assert _log_count(project_name) == 1
    assert _log_count(project_name, success_only=True) == 1

    # Run a second sync and ensure the database is updated
    sync(project_name, max_files=2)
    pv_count_second_sync = _pageviews_count(project_name)
    main_page_count_second_sync = _main_page_views(project_name)

    assert pv_count_second_sync > pv_count_first_sync
    assert main_page_count_second_sync is not None
    assert main_page_count_second_sync > main_page_first_sync
    assert _log_count(project_name) == 3
    assert _log_count(project_name, success_only=True) == 3

    # Try to compact the database
    compact(project_name)
    assert _project_exists(project_name)

    # Delete the project and ensure everything is removed
    rm(project_name)
    assert not _project_exists(project_name)


@pytest.mark.integration
def test_exception_in_file(monkeypatch: MonkeyPatch) -> None:
    """Test exception handling when we fail to parse a file."""
    project_name = "".join(random.choices(string.ascii_lowercase, k=10))

    # Temporarily set the EDITOR to "true" to auto-save the config file
    monkeypatch.setenv("EDITOR", "true")

    # Create the project and ensure the files exist
    create(project_name)
    assert _project_exists(project_name)

    # Before syncing, the database should be empty
    assert _pageviews_count(project_name) == 0
    assert _main_page_views(project_name) is None
    assert _log_count(project_name) == 0

    # Make sure a failed sync stops the process
    with patch(
        "pvduck.cli.parquet_from_url",
        side_effect=RuntimeError("Test error"),
    ):
        with pytest.raises(SystemExit):
            sync(project_name, max_files=1)

    # Ensure the database is still empty
    assert _pageviews_count(project_name) == 0
    assert _main_page_views(project_name) is None
    assert _log_count(project_name) == 1
    assert _log_count(project_name, success_only=True) == 0

    # Delete the project and ensure everything is removed
    rm(project_name)
    assert not _project_exists(project_name)


@pytest.mark.integration
def test_disallowed_commands(monkeypatch: MonkeyPatch) -> None:
    """Test error conditions when calling commands in the wrong order."""
    project_name = "".join(random.choices(string.ascii_lowercase, k=10))

    # Temporarily set the EDITOR to "true" to auto-save the config file
    monkeypatch.setenv("EDITOR", "true")

    # Ensure project does not exist
    assert not _project_exists(project_name)

    # Can't edit non-existing project
    with pytest.raises(SystemExit):
        edit(project_name)

    # Can't sync non-existing project
    with pytest.raises(SystemExit):
        sync(project_name)

    # Can't compact non-existing project
    with pytest.raises(SystemExit):
        compact(project_name)

    # Can't remove non-existing project
    with pytest.raises(SystemExit):
        rm(project_name)


def _project_exists(project_name) -> bool:
    """Check if the project files exist and are returned from `ls`."""
    if not (CONFIG_ROOT / f"{project_name}.yml").is_file():
        return False

    if not (DATA_ROOT / f"{project_name}.duckdb").is_file():
        return False

    out = _capture_stdout(ls)
    return f"- {project_name}" in out


def _capture_stdout(fn: Callable, *args, **kwargs) -> str:
    """Capture the standard output of a function."""
    buf = io.StringIO()
    old_stdout = sys.stdout
    sys.stdout = buf
    try:
        fn(*args, **kwargs)
    finally:
        sys.stdout = old_stdout
    return buf.getvalue()


def _pageviews_count(project_name: str) -> int:
    """Count the number of rows in the database."""
    db_path = DATA_ROOT / f"{project_name}.duckdb"

    with duckdb.connect(db_path) as conn:
        result = conn.execute("SELECT COUNT(*) FROM pageviews").fetchone()
        return result[0] if result else 0


def _main_page_views(project_name: str) -> Optional[int]:
    """Count the number of pageviews for the english desktop main page."""
    db_path = DATA_ROOT / f"{project_name}.duckdb"

    with duckdb.connect(db_path) as conn:
        result = conn.execute(
            """
            SELECT views
            FROM pageviews
            WHERE domain_code='en'
            AND page_title='Main_Page'
            LIMIT 1
            """
        ).fetchone()
        return result[0] if result else None


def _log_count(project_name: str, success_only: bool = False) -> int:
    """Count the number of rows in the log table."""
    db_path = DATA_ROOT / f"{project_name}.duckdb"

    sql = "SELECT COUNT(*) FROM log"
    if success_only:
        sql += " WHERE success"

    with duckdb.connect(db_path) as conn:
        result = conn.execute(sql).fetchone()
        return result[0] if result else 0
