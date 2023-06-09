"""Artifacts Utils

Tests in this module are testing the full functionality of the artifacts on the local file system.
"""
import os

from phoenix.common import artifacts


def test_create_folders_if_needed(tmp_path):
    """Test create folder if needed."""
    test_artefact_dir = tmp_path / "needed_folder/"
    test_artefact_url = f"file://{test_artefact_dir.absolute()}/"
    test_artefact_file_url = f"{test_artefact_url}file.txt"
    assert not os.path.isdir(test_artefact_dir)
    artifacts.utils.create_folders_if_needed(test_artefact_file_url)
    assert os.path.isdir(test_artefact_dir)


def test_create_folders_if_needed_already_exists(tmp_path):
    """Test create folder if needed if exists."""
    test_artefact_dir = tmp_path
    test_artefact_url = f"file://{test_artefact_dir.absolute()}/"
    test_artefact_file_url = f"{test_artefact_url}file.txt"
    assert os.path.isdir(test_artefact_dir)
    artifacts.utils.create_folders_if_needed(test_artefact_file_url)
    assert os.path.isdir(test_artefact_dir)


def test_copy(tmp_path):
    """Test copy."""
    CONTENT = "content"
    # Check that the new directory is created
    target_file = tmp_path / "target/hello_copy.txt"
    target_artefact_url = f"file://{target_file.absolute()}"
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    source_file = source_dir / "hello.txt"
    source_artefact_url = f"file://{source_file.absolute()}"
    source_file.write_text(CONTENT)
    assert source_file.read_text() == CONTENT
    assert len(list(tmp_path.iterdir())) == 1
    artifacts.utils.copy(source_artefact_url, target_artefact_url)
    assert target_file.read_text() == CONTENT
    assert len(list(tmp_path.iterdir())) == 2


def test_move(tmp_path):
    """Test move."""
    CONTENT = "content"
    target_file = tmp_path / "target.txt"
    target_artefact_url = f"file://{target_file.absolute()}"
    source_dir = tmp_path
    source_file = source_dir / "source.txt"
    source_artefact_url = f"file://{source_file.absolute()}"
    source_file.write_text(CONTENT)
    assert source_file.read_text() == CONTENT
    assert len(list(tmp_path.iterdir())) == 1
    artifacts.utils.move(source_artefact_url, target_artefact_url)
    assert target_file.read_text() == CONTENT
    assert len(list(tmp_path.iterdir())) == 1
    assert not source_file.exists()
