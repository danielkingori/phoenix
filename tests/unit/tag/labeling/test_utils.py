"""Unit tests for the utility functions of the labelling submodule."""
import pytest

from phoenix.tag.labeling import utils


@pytest.mark.parametrize(
    "object_type,expected_account_object_type",
    [
        ("facebook_posts", "facebook_pages"),
        ("tweets", "twitter_handles"),
        ("youtube_videos", "youtube_channels"),
    ],
)
def test_get_account_object_type(object_type, expected_account_object_type):
    """Test the right account_object_type is returned."""
    assert utils.get_account_object_type(object_type) == expected_account_object_type


def test_get_account_object_type_fail():
    """Test the right error is raised when a key is not found."""
    with pytest.raises(KeyError) as e:
        utils.get_account_object_type("not_an_object_type")

    assert "not_an_object_type" in str(e)
