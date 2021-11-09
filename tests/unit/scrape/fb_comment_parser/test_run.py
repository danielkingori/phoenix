"""Test run."""
import mock

from phoenix.scrape.fb_comment_parser import run


@mock.patch("phoenix.common.artifacts.utils.move")
@mock.patch("phoenix.scrape.fb_comment_parser.run.parse_fb_page")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_single_file")
def test_process_single_file(m_get_single_file, m_parse_fb_page, m_move):
    """Test the process_single_file."""
    filename_1 = "file1.html"
    to_parse_url = f"file:///d/{filename_1}"
    contents_1 = "contents1"
    m_get_single_file.return_value = (contents_1, filename_1)
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"
    m_parse_fb_page.return_value.parse_status = True
    page_as_dict = m_parse_fb_page.return_value.as_dict

    assert run.process_single_file(to_parse_url, parsed_url, fail_url) == page_as_dict

    m_get_single_file.assert_called_once_with(to_parse_url)
    m_parse_fb_page.assert_called_once_with(contents_1, filename_1)
    m_move.assert_called_once_with(to_parse_url, f"{parsed_url}{filename_1}")


@mock.patch("phoenix.common.artifacts.utils.move")
@mock.patch("phoenix.scrape.fb_comment_parser.run.parse_fb_page")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_single_file")
def test_process_single_file_parse_status(m_get_single_file, m_parse_fb_page, m_move):
    """Test the process_single_file when the parse status is Flase."""
    filename_1 = "file1.html"
    to_parse_url = f"file:///d/{filename_1}"
    contents_1 = "contents1"
    m_get_single_file.return_value = (contents_1, filename_1)
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"
    m_parse_fb_page.return_value.parse_status = False

    assert run.process_single_file(to_parse_url, parsed_url, fail_url) is None

    m_get_single_file.assert_called_once_with(to_parse_url)
    m_parse_fb_page.assert_called_once_with(contents_1, filename_1)
    m_move.assert_called_once_with(to_parse_url, f"{parsed_url}{filename_1}")


@mock.patch("phoenix.common.artifacts.utils.move")
@mock.patch("phoenix.scrape.fb_comment_parser.run.parse_fb_page")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_single_file")
def test_process_single_file_errors(m_get_single_file, m_parse_fb_page, m_move):
    """Test the process_single_file when there is errors."""
    filename_1 = "file1.html"
    to_parse_url = f"file:///d/{filename_1}"
    contents_1 = "contents1"
    m_get_single_file.return_value = (contents_1, filename_1)
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"
    m_parse_fb_page.side_effect = KeyError("foo")

    assert run.process_single_file(to_parse_url, parsed_url, fail_url) is None

    m_get_single_file.assert_called_once_with(to_parse_url)
    m_parse_fb_page.assert_called_once_with(contents_1, filename_1)
    m_move.assert_called_once_with(to_parse_url, f"{fail_url}{filename_1}")


@mock.patch("phoenix.scrape.fb_comment_parser.run.process_single_file")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_files")
def test_run_fb_page_parser(m_get_files, m_process_single_file):
    """Test the run fb page parser when there is errors."""
    filename_1 = "f1"
    filename_2 = "f2"
    to_parse_url = "file:///d/"
    m_get_files.return_value = [
        filename_1,
        filename_2,
    ]
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"

    pages = run.run_fb_page_parser(to_parse_url, parsed_url, fail_url)

    m_get_files.assert_called_once_with(to_parse_url)
    calls = [
        mock.call(filename_1, parsed_url, fail_url),
        mock.call(filename_2, parsed_url, fail_url),
    ]
    m_process_single_file.assert_has_calls(calls)
    assert pages == [
        m_process_single_file.return_value,
        m_process_single_file.return_value,
    ]


@mock.patch("phoenix.scrape.fb_comment_parser.run.process_single_file")
@mock.patch("phoenix.scrape.fb_comment_parser.run.get_files")
def test_run_fb_page_parser_none(m_get_files, m_process_single_file):
    """Test the run fb page parser when there is errors."""
    filename_1 = "f1"
    filename_2 = "f2"
    to_parse_url = "file:///d/"
    m_get_files.return_value = [
        filename_1,
        filename_2,
    ]
    parsed_url = "file:///parsed/"
    fail_url = "file:///failed/"
    m_process_single_file.return_value = None

    pages = run.run_fb_page_parser(to_parse_url, parsed_url, fail_url)

    m_get_files.assert_called_once_with(to_parse_url)
    calls = [
        mock.call(filename_1, parsed_url, fail_url),
        mock.call(filename_2, parsed_url, fail_url),
    ]
    m_process_single_file.assert_has_calls(calls)
    assert pages == []
