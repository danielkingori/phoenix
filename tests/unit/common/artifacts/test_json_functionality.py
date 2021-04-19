"""Artefacts Json functionality test.

The idea is that if they integrate with the file system they will be able
to work with a remote URL since we use tentaclio to homogenize
the system and cloud provider file functionality.
"""
from phoenix.common import artifacts


def test_json_artifact(tmp_path):
    """Test full functionality of Json artifacts."""
    test_artifact_dir = tmp_path / "json_artifacts/"
    artifact_basename = "json"
    artifact_url = artifacts.json.url(test_artifact_dir, artifact_basename)
    obj = [1, 2]

    a_json = artifacts.json.persist(artifact_url, obj)
    e_url = f"{test_artifact_dir}{artifact_basename}{artifacts.json.VALID_SUFFIX}"

    assert a_json.url == e_url
    assert obj == a_json.obj

    a_json_2 = artifacts.json.get(artifact_url)
    assert a_json.url == a_json_2.url
    assert a_json.obj == a_json_2.obj
