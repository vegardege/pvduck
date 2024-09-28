from pathlib import Path
from typing import Optional

import duckdb

from pvduck.stream import parquet_from_file


def test_parquet_from_file() -> None:
    """Make sure we can stream from a local file."""
    path = Path(__file__).parent.parent / "files" / "pageviews-20240803-060000.gz"
    parquet: Optional[Path] = None

    with parquet_from_file(path) as parquet:
        # Make sure the file is created
        assert parquet.is_file()
        assert parquet.name == "pageviews-20240803-060000.parquet"
        assert parquet.stat().st_size > 0

        # Make sure we can read information correctly from the file
        with duckdb.connect() as connection:
            result = connection.sql(f"SELECT COUNT(*) FROM '{str(parquet)}'")
            assert result.fetchone()[0] == 1000

            result = connection.sql(f"SELECT page_title FROM '{str(parquet)}' LIMIT 1")
            assert result.fetchone()[0] == "circumfluebant"

    # Make sure the file is deleted after the context manager closes
    assert not parquet.is_file()


def test_parquet_from_url() -> None:
    """Because this involves downloading a file from a remote server, it will
    only be part of the integration test."""
    pass
