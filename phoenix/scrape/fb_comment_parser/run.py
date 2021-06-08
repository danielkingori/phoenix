# type: ignore
"""Functions for running the Facebook comment parsing process."""

import datetime
import logging
import os
import pathlib

import tentaclio

from phoenix.scrape.fb_comment_parser import fb_comment_parser

# TODO bring into line with mypy

TO_PARSE_FOLDER = (
    pathlib.Path(__file__).parents[3] / "local_artifacts" / "fb_comments" / "to_parse"
)
PARSED_FOLDER = pathlib.Path(__file__).parents[3] / "local_artifacts" / "fb_comments" / "parsed"
FAIL_FOLDER = pathlib.Path(__file__).parents[3] / "local_artifacts" / "fb_comments" / "failed"

RUN_DATE_FORMAT = "%Y-%m-%d"
# This can be overwritten at execution time by Papermill to enable historic runs and backfills etc.
RUN_ISO_TIMESTAMP = datetime.datetime.now().isoformat()
run_iso_datetime = datetime.datetime.fromisoformat(RUN_ISO_TIMESTAMP)
RUN_DATE = datetime.datetime.today().strftime(RUN_DATE_FORMAT)

# TODO change this over to a file: url structure

def get_files(path):
    """Get files from a folder."""
    for filename in os.listdir(path):
        logging.info(f"Processing {filename}...")
        yield get_single_file(path, filename)


def get_single_file(path, filename):
    """Open single file and return the contents."""
    with tentaclio.open(os.path.join(path, filename), mode="rb") as f:
        contents = f.read()
    return contents, filename


def move_processed_file(from_path, to_path, file):
    """Move file from one folder to another."""
    os.makedirs(os.path.join(to_path, RUN_DATE), exist_ok=True)
    os.replace(os.path.join(from_path, file), os.path.join(to_path, RUN_DATE, file))
    return


def parse_fb_page(contents, filename):
    """Return a parsed Facebook page."""
    return fb_comment_parser.Page(contents, filename)


def run_fb_page_parser():
    """Run the parser and return a list of parsed pages."""
    pages_json = []
    for contents, filename in get_files(TO_PARSE_FOLDER):
        try:
            page = parse_fb_page(contents, filename)
            move_processed_file(TO_PARSE_FOLDER, PARSED_FOLDER, filename)
            pages_json.append(page.json)
        except Exception as e:
            logging.info(f"Failure: {e} from {filename}.")
            move_processed_file(TO_PARSE_FOLDER, FAIL_FOLDER, filename)
    return pages_json


def save_pretty(soup, filename):
    """Save a human readable version of the html file."""
    with open(os.path.join(FAIL_FOLDER, filename), "w", encoding="utf-8") as f:
        f.write(soup.prettify())
