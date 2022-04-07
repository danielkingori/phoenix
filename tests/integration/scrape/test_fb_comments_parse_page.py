"""Test pages in fb_comment_parser.py."""
import logging

import mock
import pytest

from phoenix.common import utils
from phoenix.scrape.fb_comment_parser import run


@pytest.fixture
def test_html_pages_url():
    """Get the test_html_pages."""
    test_html_pages_path = utils.relative_path("./test_html_pages/", __file__)
    return f"file://{test_html_pages_path}"


@pytest.fixture
def test_get_files_url():
    """Get the test_get_files_url."""
    test_html_pages_path = utils.relative_path("./test_get_files/", __file__)
    return f"file://{test_html_pages_path}"


@mock.patch("phoenix.common.artifacts.utils.move")
def test_fb_comment_parser(m_move, test_html_pages_url):
    """Test parsing of pages in fb_comment_parser."""
    logging.info(test_html_pages_url)
    for file_url in run.get_files(test_html_pages_url):
        # Filter for testing
        #  if not file_url.endswith("3_27_2022_10_09_30 PM_منصة ش….html"):
        #  continue

        logging.info(f"Processing: {file_url}")
        contents, basename = run.get_single_file(file_url)
        page = run.parse_fb_page(contents, basename)
        # All pages should be processable
        assert page


@mock.patch("phoenix.common.artifacts.utils.move")
def test_get_files(m_move, test_get_files_url):
    """Test get files.

    This checks that the recursive get of files works and that
    the director of the files are also correct
    """
    logging.info(test_get_files_url)
    result = run.get_files(test_get_files_url)

    # Ordering can vary for different operating systems. Sorting solves this.
    expected_fileurls_endswith = [
        "/test_get_files/nested/nested_2/6.html",
        "/test_get_files/nested/4.html",
        "/test_get_files/2.html",
        "/test_get_files/1.html",
    ]
    sorted_result = sorted(result)
    for index, expected in enumerate(sorted(expected_fileurls_endswith)):
        assert sorted_result[index].endswith(expected)
