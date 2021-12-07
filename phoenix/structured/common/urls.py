"""Structured URLs."""
from typing import Iterator, List

import prefect
import tentaclio


@prefect.task
def get_list_of_urls_in_directory(dir_url) -> List[str]:
    """G."""
    return list(get_files_in_directory(dir_url))


def get_files_in_directory(dir_url) -> Iterator[str]:
    """Get URLS of all the files in directory recursively."""
    for entry in tentaclio.scandir(dir_url):
        url_str = str(entry.url)
        if entry.is_dir:
            yield from get_files_in_directory(url_str)
        else:
            yield url_str
