from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb

# The log table is used to keep track of which pageviews files we have
# already downloaded and merged into the pageviews table.
LOG_SQL = """
CREATE TABLE log (
    filename VARCHAR PRIMARY KEY,
    success BOOLEAN NOT NULL,
    error VARCHAR
);
"""

# Denormalized domain codes with parsed data about the code components.
DOMAIN_CODE_SQL = """
CREATE SEQUENCE domain_code_seq START 1 MINVALUE 1 MAXVALUE 65535;
CREATE TABLE domain_code (
    domain_code_id USMALLINT PRIMARY KEY,
    domain_code VARCHAR NOT NULL,
    language VARCHAR NOT NULL,
    domain VARCHAR NOT NULL,
    mobile BOOLEAN NOT NULL
);
CREATE UNIQUE INDEX unique_domain_code ON domain_code (domain_code);
"""

# Denormalized page titles.
PAGE_SQL = """
CREATE SEQUENCE page_seq START 1 MINVALUE 1 MAXVALUE 4294967295;
CREATE TABLE page (
    page_id UINTEGER PRIMARY KEY,
    title VARCHAR NOT NULL
);
CREATE UNIQUE INDEX unique_page_title ON page (title);
"""

# Fact table, aggregating views over the selected dimensions.
PAGEVIEWS_SQL = """
CREATE TABLE pageviews (
    domain_code_id USMALLINT NOT NULL
        REFERENCES domain_code(domain_code_id),
    page_id UINTEGER NOT NULL
        REFERENCES page(page_id),
    timestamp TIMESTAMP NOT NULL,
    views UBIGINT NOT NULL
);
CREATE UNIQUE INDEX unique_pageviews
    ON pageviews (domain_code_id, page_id, timestamp);
"""

PAGEVIEWS_FLAT_SQL = """
CREATE TABLE pageviews_flat (
    domain_code VARCHAR NOT NULL,
    language VARCHAR NOT NULL,
    domain VARCHAR NOT NULL,
    mobile BOOLEAN NOT NULL,
    page_title VARCHAR NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    views UBIGINT NOT NULL
);
CREATE UNIQUE INDEX unique_pageviews_flat
    ON pageviews_flat (domain_code, page_title, timestamp);
"""

# Identify domain codes in the parquet file which are not already in the
# database and insert them with an auto-incrementing ID.
UPSERT_DOMAIN_CODE = """
WITH unique_new_domains AS (
  SELECT DISTINCT domain_code, language, domain, mobile
  FROM staging_table
),
filtered_domains AS (
  SELECT u.*
  FROM unique_new_domains u
  LEFT JOIN domain_code d
    ON d.domain_code = u.domain_code
  WHERE d.domain_code_id IS NULL
),
prepared_rows AS (
  SELECT nextval('domain_code_seq'), *
  FROM filtered_domains
)
INSERT INTO domain_code
    (domain_code_id, domain_code, language, domain, mobile)
SELECT * FROM prepared_rows;
"""

# Identify page titles in the parquet file which are not already in the
# database and insert them with an auto-incrementing ID.
UPSERT_PAGES = """
WITH unique_new_pages AS (
    SELECT DISTINCT page_title
    FROM staging_table
),
filtered_pages AS (
    SELECT u.*
    FROM unique_new_pages u
    LEFT JOIN page p
        ON p.title = u.page_title
    WHERE p.page_id IS NULL
),
prepared_rows AS (
    SELECT
        nextval('page_seq'), *
    FROM filtered_pages
)
INSERT INTO page
    (page_id, title)
SELECT * FROM prepared_rows;
"""

UPSERT_PAGEVIEWS = """
WITH unique_new_pageviews AS (
    SELECT
        d.domain_code_id, p.page_id, f.views
    FROM
        staging_table f
    JOIN
        domain_code d ON d.domain_code = f.domain_code
    JOIN
        page p ON p.title = f.page_title
)
INSERT INTO pageviews
    (domain_code_id, page_id, timestamp, views)
SELECT
    domain_code_id, page_id, {timestamp} AS timestamp, views
FROM unique_new_pageviews
ON CONFLICT (domain_code_id, page_id, timestamp)
    DO UPDATE SET
        views = pageviews.views + excluded.views
"""

UPSERT_PAGEVIEWS_FLAT = """
INSERT INTO pageviews_flat
    (domain_code, language, domain, mobile, page_title, timestamp, views)
SELECT
    domain_code, language, domain, mobile, page_title, {timestamp} AS timestamp, views
FROM staging_table
ON CONFLICT (domain_code, page_title, timestamp)
    DO UPDATE SET
        views = pageviews_flat.views + excluded.views
"""


def create_db(db: Path, flat: bool) -> None:
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
        if flat:
            connection.sql(PAGEVIEWS_FLAT_SQL)
        else:
            connection.sql("BEGIN TRANSACTION")
            connection.sql(LOG_SQL)
            connection.sql(DOMAIN_CODE_SQL)
            connection.sql(PAGE_SQL)
            connection.sql(PAGEVIEWS_SQL)
            connection.sql("COMMIT TRANSACTION")


def update_db_from_parquet(
    db: Path, parquet: Path, timestamp: datetime, flat: bool
) -> None:
    """Update the database with the content of the parquet file.

    Args:
        db (Path): The path to the database file.
        parquet (Path): The path to the parquet file.
        timestamp (datetime): The timestamp to use for the pageviews.

    Raises:
        FileNotFoundError: If either file does not exist.
    """
    if not db.exists():
        raise FileNotFoundError(f"Database does not exist at {db}")
    if not parquet.is_file() or parquet.suffix != ".parquet":
        raise FileNotFoundError(f"Parquet file does not exist at {parquet}")

    with duckdb.connect(db) as connection:
        if flat:
            connection.sql(
                f"CREATE TEMP TABLE staging_table AS SELECT * FROM '{parquet}'"
            )
            connection.sql(UPSERT_PAGEVIEWS_FLAT.format(timestamp=f"'{timestamp}'"))
        else:
            connection.sql("BEGIN TRANSACTION")
            connection.sql(
                f"CREATE TEMP TABLE staging_table AS SELECT * FROM '{parquet}'"
            )
            connection.sql(UPSERT_DOMAIN_CODE)
            connection.sql(UPSERT_PAGES)
            connection.sql(UPSERT_PAGEVIEWS.format(timestamp=f"'{timestamp}'"))
            connection.sql("COMMIT TRANSACTION")


def update_log(
    db: Path, filename: str, success: bool, error: Optional[str] = None
) -> None:
    """Update the log table with the result of the operation.

    Args:
        db (Path): The path to the database file.
        filename (str): The name of the file.
        success (bool): Whether the operation was successful.
        error (Optional[str]): The error message if the operation failed.
    """
    with duckdb.connect(db) as connection:
        connection.sql(
            "INSERT INTO log (filename, success, error) VALUES (?, ?, ?)",
            (filename, success, error),
        )
