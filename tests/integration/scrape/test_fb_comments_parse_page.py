"""Test pages in fb_comment_parser.py."""
import logging

import mock
import pytest
import tentaclio

from phoenix.common import utils
from phoenix.scrape.fb_comment_parser import run


@pytest.fixture
def test_html_pages_url():
    """Get the test_html_pages."""
    test_html_pages_path = utils.relative_path("./test_html_pages/", __file__)
    return f"file://{test_html_pages_path}"


def test_fb_comment_parser(test_html_pages_url):
    """Test parsing of pages in fb_comment_parser."""
    logging.info(test_html_pages_url)
    for filename in tentaclio.listdir(test_html_pages_url):
        if not filename.endswith(".html"):
            logging.info(f"Skipping invalid file: {filename}")
            continue

        logging.info(f"Processing: {filename}")
        with tentaclio.open(filename, "rb") as f:
            contents = f.read()
        page = run.parse_fb_page(contents, filename)
        assert page


@mock.patch("phoenix.common.artifacts.utils.move")
def test_get_files(m_move, test_html_pages_url):
    """Test get files."""
    logging.info(test_html_pages_url)
    for content, filename in run.get_files(test_html_pages_url):
        assert isinstance(content, bytes)
        # Should be a base name and not a folder
        assert "/" not in filename
        assert filename.endswith(".html")


@mock.patch("phoenix.common.artifacts.utils.move")
def test_run_fb_page_parser(m_move, test_html_pages_url):
    """Test run."""
    logging.info(test_html_pages_url)
    failed_url = "file:///failed/"
    success_url = "file:///success/"
    run.run_fb_page_parser(test_html_pages_url, success_url, failed_url)
    for move_call in m_move.mock_calls:
        assert move_call.startswith(success_url)
