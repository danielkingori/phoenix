"""Test pages in fb_comment_parser.py."""
import logging

import pytest
import tentaclio

from phoenix.common import utils
from phoenix.scrape.fb_comment_parser.run import parse_fb_page


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
        page = parse_fb_page(contents, filename)
        assert page
