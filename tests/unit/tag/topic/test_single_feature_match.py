"""Test of multi feature topic match."""
import mock
import pandas as pd

from phoenix.tag.topic import single_feature_match as sfm


def test_get_topics():
    """Test the get_topics."""
    topic_config = pd.DataFrame(
        {
            "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4"],
            "topic": ["t1", "t2", "t2", "t3", "t4", "t5"],
        }
    )

    features_df = pd.DataFrame(
        {
            "features": ["f1", "f2", "f5", "f1", "f5"],
            "object_id": ["o1", "o1", "o1", "o2", "o3"],
            "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2"],
        }
    )
    result_df = sfm.get_topics(topic_config, features_df)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2", "o2"],
                "topic": ["t1", "t2", "t1", "t2"],
                "object_type": ["ot1", "ot1", "ot1", "ot1"],
                "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"]],
            }
        ),
    )


@mock.patch("phoenix.tag.topic.single_feature_match._get_raw_topic_config")
def test_get_topic_config(m__get_raw_topic_config):
    """Test the get_topic_config."""
    # Taken from
    # https://docs.google.com/spreadsheets/d/10Wkj_eXr27Ko1Gw6HuHV0JJfre0IZ2cIxtCJ-YsfQK0/edit#gid=1489965930
    m__get_raw_topic_config.return_value = pd.DataFrame(
        {"features": ["f1", "f2", "f3 f4", "f5"], "topic": ["t1, t2", "t2", "t3, T4,t5", "No Tag"]}
    )

    result_df = sfm.get_topic_config()
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4"],
                "topic": ["t1", "t2", "t2", "t3", "t4", "t5"],
            }
        ),
    )
