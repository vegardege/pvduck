import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import pvstream


@contextmanager
def parquet_from_file(
    path: Path,
    batch_size: Optional[int] = None,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> Generator[Path, None, None]:
    """Stream a file from the local file system and store it in a temporary
    parquet file which is deleted when the context manager closes.

    This is a thin wrapper around the Rust library `pvstream`, which stream
    downloads, parses, and filters a Wikimedia dump file before writing it to
    a parquet file. We just need the parquet file long enough to load it into
    the database.

    Args:
        path (Path): The path to the file on the local file system.
        batch_size (Optional[int]): The number of rows to process at a time.
            Defaults to the default parquet row group size of 122 880. Increase
            to use more memory and speed up the process, or lower to use less
            memory at longer run times.
        line_regex (Optional[str]): A regex to filter the lines in the file.
        domain_codes (Optional[list[str]]): A list of domain codes to filter
            the lines in the file. For example, `["en", "de.m"]` will include
            only lines from the English desktop and German mobile Wikipedia.
        page_title (Optional[str]): A regex to filter the page titles in the
            file. For example, `".*"` will include all page titles.
        min_views (Optional[int]): The minimum number of views to include.
        max_views (Optional[int]): The maximum number of views to include.
        languages (Optional[list[str]]): A list of languages to include.
            For example, `["en", "de"]` will only include lines from the
            English and German Wikipedia.
        domains (Optional[list[str]]): A list of domains to include.
            For example, `["wikipedia.org", "wikimedia.org"]` will only include
            lines from the English Wikipedia and Wikimedia Foundation.
        mobile (Optional[bool]): Whether to include mobile views. If True,
            only mobile views will be included. If False, only desktop views
            will be included.

    Yields:
        Path: The path to the parquet file.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        fn = path.name
        target_path = Path(tmpdir) / fn.replace(".gz", ".parquet")

        pvstream.parquet_from_file(
            str(path),
            str(target_path),
            batch_size=batch_size,
            line_regex=line_regex,
            domain_codes=domain_codes,
            page_title=page_title,
            min_views=min_views,
            max_views=max_views,
            languages=languages,
            domains=domains,
            mobile=mobile,
        )

        yield target_path


@contextmanager
def parquet_from_url(
    url: str,
    batch_size: Optional[int] = None,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> Generator[Path, None, None]:
    """Stream a file from the remote server and store it in a temporary parquet
    file which is deleted when the context manager closes.

    This is a thin wrapper around the Rust library `pvstream`, which stream
    downloads, parses, and filters a Wikimedia dump file before writing it to
    a parquet file. We just need the parquet file long enough to load it into
    the database.

    Args:
        url (str): The URL of the file to stream.
        batch_size (Optional[int]): The number of rows to process at a time.
            Defaults to the default parquet row group size of 122 880. Increase
            to use more memory and speed up the process, or lower to use less
            memory at longer run times.
        line_regex (Optional[str]): A regex to filter the lines in the file.
        domain_codes (Optional[list[str]]): A list of domain codes to filter
            the lines in the file. For example, `["en", "de.m"]` will include
            only lines from the English desktop and German mobile Wikipedia.
        page_title (Optional[str]): A regex to filter the page titles in the
            file. For example, `".*"` will include all page titles.
        min_views (Optional[int]): The minimum number of views to include.
        max_views (Optional[int]): The maximum number of views to include.
        languages (Optional[list[str]]): A list of languages to include.
            For example, `["en", "de"]` will only include lines from the
            English and German Wikipedia.
        domains (Optional[list[str]]): A list of domains to include.
            For example, `["wikipedia.org", "wikimedia.org"]` will only include
            lines from the English Wikipedia and Wikimedia Foundation.
        mobile (Optional[bool]): Whether to include mobile views. If True,
            only mobile views will be included. If False, only desktop views
            will be included.

    Yields:
        Path: The path to the parquet file.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        fn = url.split("/")[-1]
        path = Path(tmpdir) / fn.replace(".gz", ".parquet")

        pvstream.parquet_from_url(
            url,
            str(path),
            batch_size=batch_size,
            line_regex=line_regex,
            domain_codes=domain_codes,
            page_title=page_title,
            min_views=min_views,
            max_views=max_views,
            languages=languages,
            domains=domains,
            mobile=mobile,
        )

        yield path
