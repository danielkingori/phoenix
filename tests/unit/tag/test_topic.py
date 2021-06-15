"""Test of topic."""
import mock
import pandas as pd

from phoenix.tag import topic


def test_get_topics():
    """Test get topics."""
    topic_config = pd.DataFrame(
        {
            "features": [
                # Features for topic 1
                "f1-t1",
                "f2-t1",
                # Features for topic 2
                "f1-t2",
                "f2-t2",
                "f3-t2",
                # Features for topic 3
                "f1-t3",
            ],
            "topic": ["t1", "t1", "t2", "t2", "t2", "t3"],
            "features_completeness": [
                # Features completeness for topic 1
                0.5,
                0.5,
                # Features completeness for topic 2
                1 / 3,
                1 / 3,
                1 / 3,
                # Features completeness for topic 3
                1,
            ],
        }
    )

    features_df = pd.DataFrame(
        {
            "features": [
                # Features for topic 1
                "f1-t1",
                "f2-t1",
                "f1-t1",
                "f1-t1",
                "f2-t1",
                "f1-t1",
                "f2-t1",
                # Features for topic 2
                "f1-t2",
                "f2-t2",
                "f3-t2",
                "f3-t2",
                # Features for topic 3
                "f1-t3",
                "f1-non",
            ],
            "object_id": [
                # topic 1
                1,
                1,
                1,
                2,
                3,
                4,
                4,
                # topic 2
                4,
                4,
                4,
                5,
                # topic 3
                1,
                6,
            ],
        }
    )

    result_df, confidence_df = topic.get_topics(topic_config, features_df)

    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame({"object_id": [1, 4, 4, 1], "topic": ["t1", "t1", "t2", "t3"]}),
    )

    e_confidence_df = pd.DataFrame(
        {
            "object_id": list(range(1, 7)) * 3,
            "confidence": [
                # topic 1
                1.5,
                0.5,
                0.5,
                1,
                0,
                0,
                # topic 2
                0,
                0,
                0,
                1,
                1 / 3,
                0,
                # topic 3
                1,
                0,
                0,
                0,
                0,
                0,
            ],
            "topic": (["t1"] * 6) + (["t2"] * 6) + (["t3"] * 6),
        }
    )

    pd.testing.assert_frame_equal(confidence_df, e_confidence_df)


@mock.patch("phoenix.tag.topic._get_raw_topic_config")
def test_get_topic_config(m__get_raw_topic_config):
    """Test the get_topic_config."""
    # Taken from
    # https://docs.google.com/spreadsheets/d/1IOU2jiAWtq9S767PZzDZ6i2ywLiqmCGiihHG8oSc3rg/edit#gid=127151492
    m__get_raw_topic_config.return_value = pd.DataFrame(
        {"Features": ["f1-t1", "f1-t2,f2-t2", "f1-t3,f2-t3,قبض"], "Topic": ["t1", "t2", "t3"]}
    )

    result_df = topic.get_topic_config()
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "features": ["f1-t1", "f1-t2", "f2-t2", "f1-t3", "f2-t3", "قبض"],
                "topic": ["t1", "t2", "t2", "t3", "t3", "t3"],
                "features_completeness": [1, 0.5, 0.5, 1 / 3, 1 / 3, 1 / 3],
            }
        ),
    )
