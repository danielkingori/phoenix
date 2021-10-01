"""Test of multi feature topic match."""
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
                "object_id": ["o1", "o1", "o2", "o2", "o3"],
                "topic": ["t1", "t2", "t1", "t2", sfm.FILL_TOPIC],
                "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2"],
                "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"], None],
                "has_topic": [True, True, True, True, False],
            }
        ),
    )
