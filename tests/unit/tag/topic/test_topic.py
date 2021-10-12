"""Test topic."""
import pandas as pd

from phoenix.tag import topic
from phoenix.tag.topic import single_feature_match as sfm


def test_get_object_topics():
    """Test get_object_topics."""
    topics_df = pd.DataFrame(
        {
            "object_id": ["o1", "o1", "o2", "o2", "o3"],
            "topic": ["t1", "t2", "t1", "t2", sfm.FILL_TOPIC],
            "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2"],
            "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"], None],
            "has_topic": [True, True, True, True, False],
        }
    )

    objects_df = pd.DataFrame(
        {
            "object_id": ["o1", "o2", "o3"],
            "object_type": ["ot1", "ot1", "ot2"],
            "objects_col": ["i", "i", "i"],
        }
    )

    objects_topics_df = topic.get_object_topics(topics_df, objects_df)

    pd.testing.assert_frame_equal(
        objects_topics_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o2", "o3"],
                "object_type": ["ot1", "ot1", "ot2"],
                "objects_col": ["i", "i", "i"],
                "topics": [["t1", "t2"], ["t1", "t2"], [sfm.FILL_TOPIC]],
                "has_topics": [True, True, False],
            }
        ),
    )
