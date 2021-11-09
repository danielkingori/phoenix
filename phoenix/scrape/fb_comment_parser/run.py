"""Functions for running the Facebook comment parsing process."""
from typing import Any, Dict, Optional, Tuple

import logging
import os

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
        logging.info(entry.url)
        if url_str.endswith(".html"):
            logging.info(f"Processing {url_str}...")
            yield url_str

        if entry.is_dir:
            logging.info("Folder found {url_str}")
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
    return fb_comment_parser.Page(contents, filename)


def process_single_file(file_url, parsed_url, fail_url) -> Optional[Dict[Any, Any]]:
    """Process a single file."""
    contents, basename = get_single_file(file_url)
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


def run_fb_page_parser(file_url, parsed_url, fail_url):
    """Run the parser and return a list of parsed pages."""
    pages = []
    for file_url in get_files(file_url):
        page = process_single_file(file_url, parsed_url, fail_url)
        if page is not None:
            pages.append(page)
    return pages


def save_pretty(soup, path, filename):
    """Utility for saving a human readable version of the html file for debugging."""
    # TODO: Fix this function, as it's broken now.
    file_url = f"{path}{filename}"
    with open(os.path.join(file_url, filename), "w", encoding="utf-8") as f:
        f.write(soup.prettify())
