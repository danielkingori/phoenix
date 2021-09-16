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
