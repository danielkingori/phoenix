"""Test pages in fb_comment_parser.py."""

import os

import tentaclio
from tentaclio import fs, urls

from phoenix.scrape.fb_comment_parser.run import parse_fb_page


# TODO: Remove the type:ignore above if better fix found, see:
#  https://stackoverflow.com/questions/39382937/
#  mypy-spurious-error-module-has-no-attribute-xpath-with-etree


CURRENT_DIRECTORY = os.path.abspath(os.path.dirname(__file__))
TEST_FILE_DIRECTORY = f"file:///{CURRENT_DIRECTORY}/test_html_pages/"


# Tenticlio seems to have a bug with the `listdir` function see issue:
# https://gitlab.com/howtobuildup/phoenix/-/issues/32
# This is a monkey patch so that it works in this test.
def _from_os_dir_entry(original: os.DirEntry) -> fs.DirEntry:
    return fs.DirEntry(
        url=urls.URL("file:///" + os.path.abspath(original.path)),
        is_dir=bool(original.is_dir()),
        is_file=bool(original.is_file()),
    )


def test_fb_comment_parser():
    """Test parsing of pages in fb_comment_parser."""
    tentaclio.clients.local_fs_client._from_os_dir_entry = _from_os_dir_entry
    for filename in tentaclio.listdir(TEST_FILE_DIRECTORY):

        with tentaclio.open(filename, "rb") as f:
            print(filename)
            contents = f.read()
        page = parse_fb_page(contents, filename)
        assert page
