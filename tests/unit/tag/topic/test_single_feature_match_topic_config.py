"""Test of multi feature topic match."""
import mock
import pandas as pd

from phoenix.tag.topic import single_feature_match_topic_config as sfm_topic_config


@mock.patch("phoenix.tag.topic.single_feature_match_topic_config._get_raw_topic_config")
def test_get_topic_config(m__get_raw_topic_config):
    """Test the get_topic_config."""
    # Taken from
    # https://docs.google.com/spreadsheets/d/10Wkj_eXr27Ko1Gw6HuHV0JJfre0IZ2cIxtCJ-YsfQK0/edit#gid=1489965930
    m__get_raw_topic_config.return_value = pd.DataFrame(
        {"features": ["f1", "f2", "f3 f4", "f5"], "topic": ["t1, t2", "t2", "t3, T4,t5", "No Tag"]}
    )

    result_df = sfm_topic_config.get_topic_config()
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4"],
                "topic": ["t1", "t2", "t2", "t3", "t4", "t5"],
            }
        ),
    )


def test_merge_new_topic_config():
    """Test the get_topic_config."""
    original = pd.DataFrame(
        {
            "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4"],
            "topic": ["t1", "t2", "t2", "t3", "t4", "t5"],
        }
    )
    new = pd.DataFrame(
        {
            "features": ["f1", "f1", "n-f1", "n-f2"],
            "topic": ["n-t1", "t1", "n-t1", None],
        }
    )
    result_df = sfm_topic_config.merge_new_topic_config(original, new)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4", "f1", "n-f1"],
                "topic": ["t1", "t2", "t2", "t3", "t4", "t5", "n-t1", "n-t1"],
            }
        ),
    )


def test_committable_topic_config():
    """Test the create of a committable of topic config."""
    topic_config = pd.DataFrame(
        {
            "features": ["f1", "f1", "f2", "f3 f4", "f3 f4", "f3 f4", "f1", "n-f1"],
            "topic": ["t1", "t2", "t2", "t3", "t4", "t5", "n-t1", "n-t1"],
        }
    )
    result_df = sfm_topic_config.committable_topic_config(topic_config)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "features": ["f1", "f2", "f3 f4", "n-f1"],
                "topic": ["n-t1, t1, t2", "t2", "t3, t4, t5", "n-t1"],
            }
        ),
    )
