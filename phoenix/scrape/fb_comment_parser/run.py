"""Functions for running the Facebook comment parsing process."""
from typing import Any, Dict, Optional, Tuple, Union

import logging
import os
from itertools import islice

import pandas as pd
import tentaclio

from phoenix.common import artifacts
from phoenix.scrape.fb_comment_parser import fb_comment_parser


def get_files(dir_url):
    """Get files that can be processed from a folder recursively."""
    for entry in tentaclio.scandir(dir_url):
        # There is a extremly strange behaviour in tentaclio
        # that URLs with a # are cut at the hash tag.
        # Sometime the automatically generated files of
        # facebook_comments_pages have a # in
        # https://gitlab.com/howtobuildup/phoenix/-/issues/65
        url_str = str(entry.url)
        logging.debug(entry.url)
        if url_str.endswith(".html"):
            logging.debug(f"Processing {url_str}...")
            yield url_str

        if entry.is_dir:
            logging.debug("Folder found {url_str}")
            yield from get_files(url_str)


def get_single_file(file_url) -> Tuple[str, str]:
    """Retrieve contents of a single file.

    Open single file and return the contents.

    Returns:
        Tuple[contents, file name]
    """
    with tentaclio.open(file_url, mode="rb") as f:
        contents = f.read()
    parsed_url = tentaclio.urls.URL(file_url)
    basename = os.path.basename(parsed_url.path)
    return contents, basename


def move_processed_file(file_url, to_path, filename):
    """Move processed files.

    This is done so that it is clear which files throw errors.
    """
    from_url = file_url
    to_url = f"{to_path}{filename}"
    artifacts.utils.move(from_url, to_url)
    return


def parse_fb_page(contents, filename):
    """Return a parsed Facebook page."""
    page = fb_comment_parser.Page(contents, filename)
    page.run()
    return page


def process_single_file(file_url, parsed_url, fail_url) -> Optional[Dict[Any, Any]]:
    """Process a single file."""
    try:
        # Some times there is an error that a file is not found
        # This is string since the URLs are generated by `tentaclio.scandir`
        contents, basename = get_single_file(file_url)
    except Exception as e:
        logging.info(f"Failure: {e} from {basename}.")
        return None
    try:
        page = parse_fb_page(contents, basename)
        move_processed_file(file_url, parsed_url, basename)
        # Parsed status False means that it can't be processed but there are
        # no errors
        if page.parse_status:
            return page.as_dict
        return None
    except Exception as e:
        # We want to save failed files but continue processing.
        logging.info(f"Failure: {e} from {basename}.")
        move_processed_file(file_url, fail_url, basename)
        return None


def run_fb_page_parser(
    file_url, parsed_url, fail_url, max_files_to_process: Union[int, bool] = False
):
    """Run the parser and return a list of parsed pages."""
    pages = []
    iterator = get_files(file_url)
    if max_files_to_process and isinstance(max_files_to_process, int):
        iterator = islice(iterator, 0, max_files_to_process)
    for file_url in iterator:
        logging.info(f"PROCESSING: {file_url}")
        page = process_single_file(file_url, parsed_url, fail_url)
        if page is not None:
            pages.append(page)
    return pages


def get_page_urls(folder_url, max_files_to_process: Union[int, bool] = False):
    """Run the parser and return a list of parsed pages."""
    pages = []
    iterator = get_files(folder_url)
    if max_files_to_process and isinstance(max_files_to_process, int):
        iterator = islice(iterator, 0, max_files_to_process)
    for file_url in iterator:
        logging.debug(f"PROCESSING: {file_url}")
        contents, basename = get_single_file(file_url)
        page = fb_comment_parser.Page(contents, basename)
        if page is not None and page.is_mbasic:
            d = {"url": page.url, "file_name": basename, "scrape_url": page.scrape_url}
            pages.append(d | page.url_components)

    return pd.DataFrame(pages)


def save_pretty(soup, path, filename):
    """Utility for saving a human readable version of the html file for debugging."""
    # TODO: Fix this function, as it's broken now.
    file_url = f"{path}{filename}"
    with open(os.path.join(file_url, filename), "w", encoding="utf-8") as f:
        f.write(soup.prettify())
