import shutil
import tempfile
from pathlib import Path

import duckdb
import pytest

from pvduck.db import compact_database, create_db, update_from_parquet, update_log


def test_create() -> None:
    """Test creating a new database."""
    with tempfile.TemporaryDirectory() as tmpdir:

        # Make sure we can create a new table
        db_path = Path(tmpdir) / "test.duckdb"
        assert not db_path.exists()

        create_db(db_path)

        assert db_path.is_file()

        with duckdb.connect(db_path) as connection:
            tables = connection.sql("SHOW TABLES").fetchall()
            assert len(tables) == 2
            assert tables[0][0] == "log"
            assert tables[1][0] == "pageviews"

        # Make sure we can't overwrite an existing table
        with pytest.raises(FileExistsError):
            create_db(db_path)


def test_update_from_parquet() -> None:
    """Test updating the database from a parquet file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        create_db(db_path)

        # Copy the parquet file to the temporary directory
        parquet_src = [
            Path(__file__).parent.parent / "files" / fn
            for fn in [
                "pageviews-20240818-100000.parquet",
                "pageviews-20240818-110000.parquet",
            ]
        ]
        parquet_target = [
            Path(tmpdir) / fn
            for fn in [
                "pageviews-20240818-100000.parquet",
                "pageviews-20240818-110000.parquet",
            ]
        ]

        for src, target in zip(parquet_src, parquet_target):
            if target.exists():
                target.unlink()
            shutil.copy(src, target)

        # Check that the database is empty
        query = """
            SELECT domain_code, language, domain, mobile, page_title, views
            FROM pageviews
            ORDER BY views DESC
        """

        with duckdb.connect(db_path) as connection:
            result = connection.sql(query).fetchall()
            assert len(result) == 0

        # Fill the database from a parquet file and check the result
        update_from_parquet(db_path, parquet_target[0])

        with duckdb.connect(db_path) as connection:
            result = connection.sql(query).fetchall()
            assert len(result) == 17
            assert result[0][5] == 74953

        # Fill with the same parquet file again. This should not insert any
        # new rows, but all view counts should double.
        update_from_parquet(db_path, parquet_target[0])

        with duckdb.connect(db_path) as connection:
            result = connection.sql(query).fetchall()
            assert len(result) == 17
            assert result[0][5] == 74953 * 2

        # Fill with a different parquet file. This should insert new rows
        # and update existing ones.
        update_from_parquet(db_path, parquet_target[1])

        with duckdb.connect(db_path) as connection:
            result = connection.sql(query).fetchall()
            assert len(result) == 19
            assert result[0][5] == 223034

        # Make sure we can't fill from a non-existing parquet file
        with pytest.raises(FileNotFoundError):
            update_from_parquet(Path(tmpdir) / "non_existing.duckdb", parquet_target[0])

        with pytest.raises(FileNotFoundError):
            update_from_parquet(db_path, Path(tmpdir) / "non_existing.parquet")


def test_compact_db() -> None:
    """Make sure compacting the database does not affect data integrity,
    just the size of the table file (which is not deterministic)."""
    with tempfile.TemporaryDirectory() as tmpdir:

        # Make sure we can create a new table
        db_path = Path(tmpdir) / "test.duckdb"
        assert not db_path.exists()

        create_db(db_path)

        parquet_src = (
            Path(__file__).parent.parent / "files" / "pageviews-20240818-100000.parquet"
        )
        parquet_target = Path(tmpdir) / "pageviews-20240818-100000.parquet"

        shutil.copy(parquet_src, parquet_target)

        # Fill the database from a parquet file and check the result
        update_from_parquet(db_path, parquet_target)

        with duckdb.connect(db_path) as connection:
            result = connection.sql(
                """
                SELECT domain_code, language, domain, mobile, page_title, views
                FROM pageviews
                ORDER BY views DESC
            """
            ).fetchall()
            assert len(result) == 17
            assert result[0][5] == 74953

        # Compact the database
        compact_database(db_path)

        # Check that the database is still valid
        with duckdb.connect(db_path) as connection:
            result = connection.sql(
                """
                SELECT domain_code, language, domain, mobile, page_title, views
                FROM pageviews
                ORDER BY views DESC
            """
            ).fetchall()
            assert len(result) == 17
            assert result[0][5] == 74953

        # Make sure we can't compact a non-existing database
        with pytest.raises(FileNotFoundError):
            compact_database(Path(tmpdir) / "non_existing.duckdb")
