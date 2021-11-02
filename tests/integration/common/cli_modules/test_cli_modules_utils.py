"""Integration tests for cli_modules/utils.py"""
import pytest

from phoenix.common.cli_modules import utils


def test_file_exists(tmp_path):
    """Test file_exists."""
    test_artefact_dir = tmp_path / "dataframe_artifacts/"
    test_artefact_dir.mkdir()
    test_file_path = test_artefact_dir / "hello.txt"
    test_file_path.write_text("content")
    assert utils.file_exists(f"file://{test_file_path}")


def test_file_exists_not_found(tmp_path):
    """Test file_exists not found."""
    test_artefact_dir = tmp_path / "dataframe_artifacts/"
    test_artefact_dir.mkdir()
    test_file_path = test_artefact_dir / "hello.txt"
    url = f"file://{test_file_path}"
    with pytest.raises(RuntimeError) as error:
        utils.file_exists(url)
        assert url in str(error.value)


def test_file_exists_not_found_silence(tmp_path):
    """Test file_exists not found silenced."""
    test_artefact_dir = tmp_path / "dataframe_artifacts/"
    test_artefact_dir.mkdir()
    test_file_path = test_artefact_dir / "hello.txt"
    assert not utils.file_exists(f"file://{test_file_path}", True)
