# type: ignore
"""Functions for running the Facebook comment parsing process."""

import logging
import os

from phoenix.scrape.fb_comment_parser import fb_comment_parser


# import tentaclio


# TODO bring into line with mypy


def file_url_strip(path):
    """File adjustment to remove 'file:' where necessary."""
    if path.startswith("file:"):
        return path[len("file:") :]
    else:
        return path


def get_files(url):
    """Get files from a folder."""
    for filename in os.listdir(file_url_strip(url)):
        logging.info(f"Processing {filename}...")
        yield get_single_file(url, filename)


def get_single_file(path, filename):
    """Retrieve contents of a single file."""
    file_url = f"{path}{filename}"
    """Open single file and return the contents."""
    logging.info(file_url)
    # with tentaclio.open(file_url, mode="rb") as f:
    with open(file_url_strip(file_url), mode="rb") as f:
        contents = f.read()
    return contents, filename


def move_processed_file(from_path, to_path, filename):
    """Move file from one folder to another."""
    # Construct file urls
    from_url = f"{from_path}{filename}"
    to_url = f"{to_path}{filename}"
    # Make a new directory if it's not there already
    os.makedirs(os.path.dirname(file_url_strip(to_path)), exist_ok=True)

    os.rename(file_url_strip(from_url), file_url_strip(to_url))
    # tentaclio.copy(from_url, to_url)
    # tentaclio.remove(from_url)
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
