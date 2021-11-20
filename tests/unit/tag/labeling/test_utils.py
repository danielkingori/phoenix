"""Unit tests for the utility functions of the labelling submodule."""
import pandas as pd
import pytest

from phoenix.tag.labeling import utils
from phoenix.tag.labeling.generate_label_sheet import EXPECTED_COLUMNS_ACCOUNT_LABELING_SHEET


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


def test_is_valid_account_labeling_sheet():
    """Test that dataframes with exactly the right columns returns True."""
    input_df = pd.DataFrame(columns=EXPECTED_COLUMNS_ACCOUNT_LABELING_SHEET)
    assert utils.is_valid_account_labeling_sheet(input_df)


def test_is_valid_account_labeling_sheet_extra_cols():
    """Test that dataframes with exactly the right columns plus more columns returns True."""
    columns = EXPECTED_COLUMNS_ACCOUNT_LABELING_SHEET.copy()
    columns.append("some_other_column")
    input_df = pd.DataFrame(columns=columns)
    assert utils.is_valid_account_labeling_sheet(input_df)


def test_is_valid_account_labeling_sheet_fail():
    """Test that dataframes not exactly the right columns returns False."""
    columns = EXPECTED_COLUMNS_ACCOUNT_LABELING_SHEET.copy()
    columns.pop()
    input_df = pd.DataFrame(columns=columns)
    assert not utils.is_valid_account_labeling_sheet(input_df)
