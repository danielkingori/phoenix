"""Structured URLs."""
from typing import Iterator

import tentaclio


def get_files_in_directory(dir_url) -> Iterator[str]:
    """Get URLS of all the files in directory recursively."""
    for entry in tentaclio.scandir(dir_url):
        url_str = str(entry.url)
        if entry.is_dir:
            yield from get_files_in_directory(url_str)
        else:
            yield url_str
