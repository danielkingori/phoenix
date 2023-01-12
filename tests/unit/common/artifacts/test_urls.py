"""Test artifacts urls module."""
from phoenix.common.artifacts import urls


def test_get_local():
    """Test path returned from get_local."""
    local_url = urls.get_local()
    assert (
        local_url[-1 - 7 - 1 - 15 - 1 :] == "/phoenix/local_artifacts/"
        or local_url[-1 - 3 - 1 - 15 - 1 :] == "/src/local_artifacts/"
    )


def test_get_local_models():
    """Test path returned from get_local_models."""
    local_url = urls.get_local_models()
    assert (
        local_url[-1 - 7 - 1 - 12 - 1 :] == "/phoenix/local_models/"
        or local_url[-1 - 3 - 1 - 12 - 1 :] == "/src/local_models/"
    )
