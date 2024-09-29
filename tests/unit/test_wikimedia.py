from datetime import datetime

from pvduck.wikimedia import url_from_timestamp


def test_url_from_timestamp() -> None:
    """Test the URL generation from a timestamp."""
    mirror = "https://mirror.accum.se/mirror/wikimedia.org/"

    # Test with default URL
    timestamp = datetime(2024, 8, 18, 10, 0, 0)
    expected_url = "https://dumps.wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240818-100000.gz"

    url = url_from_timestamp(timestamp=timestamp)

    assert url == expected_url

    # Test with a mirror
    timestamp = datetime(2024, 8, 24, 9, 0, 0)
    expected_url = "https://mirror.accum.se/mirror/wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240824-090000.gz"

    url = url_from_timestamp(mirror, timestamp)

    assert url == expected_url
