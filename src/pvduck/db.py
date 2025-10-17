import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb

logger = logging.getLogger(__name__)

# The log table is used to keep track of which pageviews files we have
# already downloaded and merged into the pageviews table.
LOG_SQL = """
CREATE TABLE log (
    timestamp TIMESTAMP PRIMARY KEY,
    success BOOLEAN NOT NULL,
    error VARCHAR
)
"""

# The pageviews table is simply a flat, aggregated version of the parquet
# files. Denormalizing could benefit the table in some use cases, but for
# the total aggregation use case I decided on, duckdb effectively stores
# this in a flat file without much overhead. Change this if you want a
# representation better suited for larger amounts of data.
PAGEVIEWS_SQL = """
CREATE TABLE pageviews (
    domain_code VARCHAR NOT NULL,
    language VARCHAR NOT NULL,
    domain VARCHAR NOT NULL,
    mobile BOOLEAN NOT NULL,
    page_title VARCHAR NOT NULL,
    views UBIGINT NOT NULL,
    PRIMARY KEY (domain_code, page_title)
)
"""

PAGEVIEWS_INDEX_SQL = """
CREATE UNIQUE INDEX unique_pageviews
    ON pageviews (domain_code, page_title)
"""

# Add views from a parquet file to the table, or create new rows if they
# don't exist yet. This design is created to save space, but note that it
# is destructive, so we need to keep track of which files we have inserted.
UPDATE_PAGEVIEWS = """
UPDATE pageviews
   SET views = pageviews.views + p.views
  FROM read_parquet('{parquet}') AS p
 WHERE pageviews.domain_code = p.domain_code
   AND pageviews.page_title = p.page_title
"""

INSERT_PAGEVIEWS = """
INSERT INTO pageviews
    (domain_code, language, domain, mobile, page_title, views)
   SELECT p.domain_code, p.language, p.domain, p.mobile, p.page_title, p.views
     FROM read_parquet('{parquet}') AS p
LEFT JOIN pageviews AS v
       ON v.domain_code = p.domain_code
      AND v.page_title = p.page_title
    WHERE v.domain_code IS NULL
"""


def create_db(db: Path) -> None:
    """Create a new duckdb database at the specified path.

    Args:
        db (Path): The path to the database file.

    Raises:
        FileExistsError: If the database file already exists.
    """
    if db.exists():
        raise FileExistsError(f"Database already exists at {db}")

    # Create the database and tables
    with duckdb.connect(db) as connection:
        connection.sql("BEGIN TRANSACTION")
        connection.sql(LOG_SQL)
        connection.sql(PAGEVIEWS_SQL)
        connection.sql(PAGEVIEWS_INDEX_SQL)
        connection.sql("COMMIT TRANSACTION")


def read_log_timestamps(
    db: Path, success: Optional[bool] = None
) -> set[datetime]:
    """Get a list of all the files we have already processed."""
    if not db.is_file():
        raise FileNotFoundError(f"Database does not exist at {db}")

    sql = "SELECT timestamp FROM log "
    if success is True:
        sql += "WHERE success"
    elif success is False:
        sql += "WHERE NOT success"

    with duckdb.connect(db) as connection:
        result = connection.sql(sql).fetchall()
        return {row[0] for row in result}


def update_from_parquet(db: Path, parquet: Path) -> tuple[int, int]:
    """Update the database with the content of the parquet file.

    Args:
        db (Path): The path to the database file.
        parquet (Path): The path to the parquet file.

    Returns:
        A tuple with the number of rows inserted and number of rows updated.

    Raises:
        FileNotFoundError: If either file does not exist.
    """
    if not db.is_file():
        raise FileNotFoundError(f"Database does not exist at {db}")
    if not parquet.is_file():
        raise FileNotFoundError(f"Parquet file does not exist at {parquet}")

    with duckdb.connect(db) as connection:
        connection.sql("BEGIN TRANSACTION")

        # First, we update all the rows currently in the table
        update_sql = UPDATE_PAGEVIEWS.format(parquet=str(parquet))
        update_result = connection.execute(update_sql)
        updated_rows = update_result.rowcount

        # Second, we insert all rows that weren't previously in the table
        insert_sql = INSERT_PAGEVIEWS.format(parquet=str(parquet))
        insert_result = connection.execute(insert_sql)
        inserted_rows = insert_result.rowcount

        connection.sql("COMMIT TRANSACTION")

    return inserted_rows, updated_rows


def update_log(
    db: Path, timestamp: datetime, success: bool, error: Optional[str] = None
) -> None:
    """Update the log table with the result of the operation.

    Args:
        db (Path): The path to the database file.
        timestamp (datetime): Date and time for the file we are logging.
        success (bool): Whether the operation was successful.
        error (Optional[str]): The error message if the operation failed.
    """
    if not db.is_file():
        raise FileNotFoundError(f"Database does not exist at {db}")

    # If this is a very recent file, we'll assume it's not available
    # yet and proceed instead of failing.
    if success is False and datetime.now() - timestamp < timedelta(hours=12):
        print("File not available yet, skipping")
        return

    with duckdb.connect(db) as connection:
        connection.execute(
            "INSERT INTO log (timestamp, success, error) VALUES (?, ?, ?)",
            (timestamp, success, error),
        )


def compact_db(db: Path) -> tuple[float, float, float]:
    """Compact the database by creating a new table and dropping the old one.

    DuckDB does not automatically compact the database when upserting data,
    which can lead to fragmentation and increased file size. This function
    creates a new table with the same schema as the original, copies the
    data from the original table to the new table, and then drops the
    original table.

    This requires enough disk space to create a new copy of the table, so it
    is not run automatically.

    This may be improved in the future, keep an eye on:
    https://duckdb.org/docs/stable/operations_manual/footprint_of_duckdb/reclaiming_space.html

    Args:
        db (Path): The path to the database file.

    Raises:
        FileNotFoundError: If the database file does not exist.

    Returns:
        tuple[float, float, float]: The size of the database before and after
            compacting, and the space saved.
    """
    if not db.is_file():
        raise FileNotFoundError(f"Database does not exist at {db}")

    size_pre_compacting = _file_size_mb(db)
    logger.info("Compacting database %s", db)
    logger.info("Size before compacting: %.2f MB", size_pre_compacting)

    with duckdb.connect(db) as connection:
        connection.sql("BEGIN TRANSACTION")
        connection.sql(
            """
            CREATE TABLE pageviews_compacted AS
            SELECT * FROM pageviews
            """
        )
        connection.sql("DROP TABLE pageviews")
        connection.sql(
            """
            ALTER TABLE pageviews_compacted
            RENAME TO pageviews
            """
        )
        connection.sql(PAGEVIEWS_INDEX_SQL)
        connection.sql("COMMIT TRANSACTION")

    size_post_compacting = _file_size_mb(db)
    logger.info("Size after compacting: %.2f MB", size_post_compacting)
    logger.info(
        "Space saved: %.2f MB", size_pre_compacting - size_post_compacting
    )

    return (
        size_pre_compacting,
        size_post_compacting,
        size_pre_compacting - size_post_compacting,
    )


def _file_size_mb(path: Path) -> float:
    """Return the size of the file in MB.

    Args:
        path (Path): The path to the file.

    Returns:
        float: The size of the file in MB.
    """
    return path.stat().st_size / (1024 * 1024)
