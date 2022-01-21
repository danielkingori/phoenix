"""Unit tests for clustering utils."""
import mock
import pandas as pd
import pytest

from phoenix.common import artifacts
from phoenix.tag.clustering import utils


@pytest.fixture()
def input_object_df():
    """Input object_df"""
    return pd.DataFrame(
        {
            "object_id": [1, 2],
            "clean_text": ["c", "c2"],
            "not_included": ["n", "n2"],
        }
    )


@mock.patch("phoenix.common.artifacts.dataframes.get")
def test_apply_grouping_objects_topics(m_get, input_object_df):
    """Test of apply_grouping_to_objects with "topic"."""
    topic_df_url = "topic_url"
    topic_df = pd.DataFrame({"object_id": [1, 1, 2], "topic": ["t", "t1", "t3"]})
    m_get.return_value = artifacts.dtypes.ArtifactDataFrame(url="url", dataframe=topic_df)
    expected_output = pd.DataFrame(
        {
            "object_id": [1, 1, 2],
            "topic": ["t", "t1", "t3"],
            "clean_text": ["c", "c", "c2"],
        }
    )
    result_df = utils.apply_grouping_to_objects(
        grouping_type="topic",
        object_df=input_object_df,
        topic_df_url=topic_df_url,
    )
    pd.testing.assert_frame_equal(result_df, expected_output)
    m_get.assert_called_once_with(topic_df_url)


def test_apply_grouping_objects_none(input_object_df):
    """Test of apply_grouping_to_objects with "none"."""
    result_df = utils.apply_grouping_to_objects(
        grouping_type="none",
        object_df=input_object_df,
    )
    pd.testing.assert_frame_equal(result_df, input_object_df[["object_id", "clean_text"]])
