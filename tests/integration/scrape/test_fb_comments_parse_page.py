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


def test_fb_comment_parser(test_html_pages_url):
    """Test parsing of pages in fb_comment_parser."""
    logging.info(test_html_pages_url)
    for contents, directory, filename in run.get_files(test_html_pages_url):
        if not filename.endswith(".html"):
            logging.info(f"Skipping invalid file: {filename}")
            continue

        logging.info(f"Processing: {filename}")
        page = run.parse_fb_page(contents, filename)
        assert page


@mock.patch("phoenix.common.artifacts.utils.move")
def test_get_files(m_move, test_get_files_url):
    """Test get files.

    This checks that the recursive get of files works and that
    the director of the files are also correct
    """
    logging.info(test_get_files_url)
    file_names = []
    directories = []
    for content, directory, filename in run.get_files(test_get_files_url):
        assert isinstance(content, bytes)
        # Should be a base name and not a folder
        assert "/" not in filename
        assert filename.endswith(".html")
        assert directory.endswith("/")
        file_names.append(filename)
        directories.append(directory)

    # Ordering can vary for different operating systems. Sorting solves this.
    assert sorted(file_names) == sorted(["6.html", "4.html", "1.html", "2.html"])
    expected_directories_endswith = [
        "/test_get_files/nested/nested_2/",
        "/test_get_files/nested/",
        "/test_get_files/",
        "/test_get_files/",
    ]
    sorted_directories = sorted(directories)
    for index, expected in enumerate(sorted(expected_directories_endswith)):
        assert sorted_directories[index].endswith(expected)


@mock.patch("phoenix.common.artifacts.utils.move")
def test_run_fb_page_parser(m_move, test_html_pages_url):
    """Test run."""
    logging.info(test_html_pages_url)
    failed_url = "file:///failed/"
    success_url = "file:///success/"
    run.run_fb_page_parser(test_html_pages_url, success_url, failed_url)
    for move_call in m_move.mock_calls:
        assert move_call.startswith(success_url)
