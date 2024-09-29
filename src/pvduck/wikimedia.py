from datetime import datetime

# Official mirror. You are recommended to look for a mirror closer to you:
# https://meta.wikimedia.org/wiki/Mirroring_Wikimedia_project_XML_dumps
BASE_URL = "https://dumps.wikimedia.org/"


def url_from_timestamp(
    base_url: str = BASE_URL,
    timestamp: datetime = datetime.now(),
) -> str:
    """Create a full Wikimedia dump URL from a timestamp.

    Example URL:
    https://dumps.wikimedia.org/other/pageviews/2024/2024-08/pageviews-20240818-100000.gz

    Args:
        base_url (str): The base URL of the Wikimedia dump server. This URL
            should point to a page that contains an 'other' directory where
            the dumps are stored.
        timestamp (datetime): The timestamp to format.

    Returns:
        str: The full URL to the dump file.
    """
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    dt = timestamp.strftime("%Y%m%d-%H0000")
    fn = f"pageviews-{dt}.gz"

    return f"{base_url}other/pageviews/{year}/{year}-{month}/{fn}"
