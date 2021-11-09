"""Test run."""
import mock

from phoenix.scrape.fb_comment_parser import run


@mock.patch("phoenix.common.artifacts.utils.move")
@mock.patch("phoenix.scrape.fb_comment_parser.run.parse_fb_page")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_files")
def test_run_fb_page_parser(m_get_files, m_parse_fb_page, m_move):
    """Test the run fb page parser."""
    file_name_1 = "file1.txt"
    file_name_2 = "file2.txt"
    contents_1 = "contents1"
    contents_2 = "contents2"
    to_parse_url = "file:///d/"
    m_get_files.return_value = [
        (contents_1, to_parse_url, file_name_1),
        (contents_2, to_parse_url, file_name_2),
    ]
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"

    run.run_fb_page_parser(to_parse_url, parsed_url, fail_url)

    m_get_files.assert_called_once_with(to_parse_url)
    calls = [
        mock.call(contents_1, file_name_1),
        mock.call(contents_2, file_name_2),
    ]

    m_parse_fb_page.assert_has_calls(calls)
    calls = [
        mock.call(f"{to_parse_url}{file_name_1}", f"{parsed_url}{file_name_1}"),
        mock.call(f"{to_parse_url}{file_name_2}", f"{parsed_url}{file_name_2}"),
    ]
    m_move.assert_has_calls(calls)


@mock.patch("phoenix.common.artifacts.utils.move")
@mock.patch("phoenix.scrape.fb_comment_parser.run.parse_fb_page")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_files")
def test_run_fb_page_parser_errors(m_get_files, m_parse_fb_page, m_move):
    """Test the run fb page parser when there is errors."""
    file_name_1 = "file1.txt"
    file_name_2 = "file2.txt"
    contents_1 = "contents1"
    contents_2 = "contents2"
    to_parse_url = "file:///d/"
    m_get_files.return_value = [
        (contents_1, to_parse_url, file_name_1),
        (contents_2, to_parse_url, file_name_2),
    ]
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"
    m_parse_fb_page.side_effect = KeyError("foo")

    run.run_fb_page_parser(to_parse_url, parsed_url, fail_url)

    m_get_files.assert_called_once_with(to_parse_url)
    calls = [
        mock.call(contents_1, file_name_1),
        mock.call(contents_2, file_name_2),
    ]

    m_parse_fb_page.assert_has_calls(calls)
    calls = [
        mock.call(f"{to_parse_url}{file_name_1}", f"{fail_url}{file_name_1}"),
        mock.call(f"{to_parse_url}{file_name_2}", f"{fail_url}{file_name_2}"),
    ]
    m_move.assert_has_calls(calls)
