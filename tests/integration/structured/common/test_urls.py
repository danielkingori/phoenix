"""Test URLS."""
import pytest

from phoenix.common import utils
from phoenix.structured.common import urls


@pytest.fixture
def folder_of_test_files():
    """Get URL for the folder of test files."""
    test_folder = utils.relative_path("./data/test_folder/", __file__)
    return f"file://{test_folder}"


def test_get_files_in_directory(folder_of_test_files):
    """Test get_files_in_directory."""
    result = urls.get_files_in_directory(folder_of_test_files)
    assert list(result) == [
        f"{folder_of_test_files}/dir_1/dir_2/4.txt",
        f"{folder_of_test_files}/dir_1/3.txt",
        f"{folder_of_test_files}/1.txt",
        f"{folder_of_test_files}/2.txt",
    ]
