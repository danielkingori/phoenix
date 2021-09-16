"""Functions for running the Facebook comment parsing process."""
from typing import Tuple

import logging
import os

import tentaclio

from phoenix.common import artifacts
from phoenix.scrape.fb_comment_parser import fb_comment_parser


def get_files(dir_url):
    """Get files from a folder."""
    for url in tentaclio.listdir(dir_url):

        logging.info(f"Processing {url}...")
        yield get_single_file(url)


def get_single_file(file_url) -> Tuple[str, str]:
    """Retrieve contents of a single file.

    Open single file and return the contents.

    Returns:
        Tuple[contents, file name]
    """
    with tentaclio.open(file_url, mode="rb") as f:
        contents = f.read()
    parsed_url = tentaclio.urls.URL(file_url)
    return contents, os.path.basename(parsed_url.path)


def move_processed_file(from_path, to_path, filename):
    """Move processed files.

    This is done so that it is clear which files throw errors.
    """
    # Construct file urls
    from_url = f"{from_path}{filename}"
    to_url = f"{to_path}{filename}"
    artifacts.utils.move(from_url, to_url)
    return


def parse_fb_page(contents, filename):
    """Return a parsed Facebook page."""
    return fb_comment_parser.Page(contents, filename)


def run_fb_page_parser(to_parse_url, parsed_url, fail_url):
    """Run the parser and return a list of parsed pages."""
    pages = []
    for contents, filename in get_files(to_parse_url):
        try:
            page = parse_fb_page(contents, filename)
            pages.append(page.as_dict)
            move_processed_file(to_parse_url, parsed_url, filename)
        except Exception as e:
            # We want to save failed files but continue processing.
            logging.info(f"Failure: {e} from {filename}.")
            move_processed_file(to_parse_url, fail_url, filename)
            continue
    return pages


def save_pretty(soup, path, filename):
    """Utility for saving a human readable version of the html file for debugging."""
    # TODO: Fix this function, as it's broken now.
    file_url = f"{path}{filename}"
    with open(os.path.join(file_url, filename), "w", encoding="utf-8") as f:
        f.write(soup.prettify())
