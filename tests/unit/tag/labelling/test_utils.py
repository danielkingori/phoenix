"""Unit tests for the utility functions of the labelling submodule."""
import pandas as pd
import pytest

from phoenix.tag.labelling import utils
from phoenix.tag.labelling.generate_label_sheet import EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET


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


def test_is_valid_account_labelling_sheet():
    """Test that dataframes with exactly the right columns returns True."""
    input_df = pd.DataFrame(columns=EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET)
    assert utils.is_valid_account_labelling_sheet(input_df)


def test_is_valid_account_labelling_sheet_extra_cols():
    """Test that dataframes with exactly the right columns plus more columns returns True."""
    columns = EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET.copy()
    columns.append("some_other_column")
    input_df = pd.DataFrame(columns=columns)
    assert utils.is_valid_account_labelling_sheet(input_df)


def test_is_valid_account_labelling_sheet_fail():
    """Test that dataframes not exactly the right columns returns False."""
    columns = EXPECTED_COLUMNS_ACCOUNT_LABELLING_SHEET.copy()
    columns.pop()
    input_df = pd.DataFrame(columns=columns)
    assert not utils.is_valid_account_labelling_sheet(input_df)


def test_filter_out_duplicates():
    """Test that filter_out_duplicates filters out duplicates."""
    to_filter_df = pd.DataFrame(
        data={
            "object_id": ["1", "2", "3", "4", "5"],
            "data_column": ["data_1", "data_2", "data_3", "data_4", "data_5"],
        }
    )

    filter_using_this_df = pd.DataFrame(
        data={
            "object_id": ["1", "3", "5"],
            "different_data_column": ["best data", "better data", "ok data"],
        }
    )

    expected_df = pd.DataFrame(
        data={"object_id": ["2", "4"], "data_column": ["data_2", "data_4"]}, index=[1, 3]
    )

    output_df = utils.filter_out_duplicates(filter_using_this_df, to_filter_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_filter_out_duplicates_inputted_col_name():
    """Test that filter_out_duplicates filters out duplicates."""
    to_filter_df = pd.DataFrame(
        data={
            "new_col_name": ["1", "2", "3", "4", "5"],
            "data_column": ["data_1", "data_2", "data_3", "data_4", "data_5"],
        }
    )

    filter_using_this_df = pd.DataFrame(
        data={
            "new_col_name": ["1", "3", "5"],
            "different_data_column": ["best data", "better data", "ok data"],
        }
    )

    expected_df = pd.DataFrame(
        data={"new_col_name": ["2", "4"], "data_column": ["data_2", "data_4"]}, index=[1, 3]
    )

    output_df = utils.filter_out_duplicates(
        filter_using_this_df, to_filter_df, cols=["new_col_name"]
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_filter_out_duplicates_multi_col_name():
    """Test that filter_out_duplicates filters out duplicates."""
    to_filter_df = pd.DataFrame(
        data={
            "new_col_name": ["1", "2", "3", "4", "5"],
            "another_col_name": ["a", "b", "c", "d", "e"],
            "data_column": ["data_1", "data_2", "data_3", "data_4", "data_5"],
        }
    )

    filter_using_this_df = pd.DataFrame(
        data={
            "new_col_name": ["1", "3", "5"],
            "another_col_name": ["a", "not_c", "e"],
            "different_data_column": ["best data", "better data", "ok data"],
        }
    )

    expected_df = pd.DataFrame(
        data={
            "new_col_name": ["2", "3", "4"],
            "another_col_name": ["b", "c", "d"],
            "data_column": ["data_2", "data_3", "data_4"],
        },
        index=[1, 2, 3],
    )

    output_df = utils.filter_out_duplicates(
        filter_using_this_df, to_filter_df, cols=["new_col_name", "another_col_name"]
    )
    pd.testing.assert_frame_equal(output_df, expected_df)
