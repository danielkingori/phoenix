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
            "features": ["f1", "f2", "f5", "f1", "f5", "f3", "f3 f5"],
            "object_id": ["o1", "o1", "o1", "o2", "o3", "o4", "o5"],
            "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2", "ot1", "ot1"],
        }
    )
    result_df = sfm.get_topics(topic_config, features_df)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2", "o2", "o3", "o4", "o5"],
                "topic": ["t1", "t2", "t1", "t2", sfm.FILL_TOPIC, sfm.FILL_TOPIC, sfm.FILL_TOPIC],
                "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2", "ot1", "ot1"],
                "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"], None, None, None],
                "has_topic": [True, True, True, True, False, False, False],
            }
        ),
    )


def test_get_topics_set_fill_topic():
    """Test the get_topics with set of the fill topic."""
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
    fill_topic = "fill_topic"
    result_df = sfm.get_topics(topic_config, features_df, fill_topic=fill_topic)
    pd.testing.assert_frame_equal(
        result_df,
        pd.DataFrame(
            {
                "object_id": ["o1", "o1", "o2", "o2", "o3"],
                "topic": ["t1", "t2", "t1", "t2", fill_topic],
                "object_type": ["ot1", "ot1", "ot1", "ot1", "ot2"],
                "matched_features": [["f1"], ["f1", "f2"], ["f1"], ["f1"], None],
                "has_topic": [True, True, True, True, False],
            }
        ),
    )


def test_get_topics_analysis_df():
    """Test the get_topic analysis."""
    input_df = pd.DataFrame(
        {
            "topic": ["dog", "dog", "cat", "cat", "cat", "cat"],
            "matched_features": [
                ["woof"],
                ["woof", "bark"],
                ["meow"],
                ["meow"],
                ["meow"],
                ["meow", "purr"],
            ],
            "other_column": ["any", "input", "here", "doesn't", "persist", "further"],
        }
    )

    expected_df = pd.DataFrame(
        {
            "topic": ["cat", "cat", "dog", "dog"],
            "matched_features": ["meow", "purr", "woof", "bark"],
            "matched_object_count": [4, 1, 2, 1],
        }
    )

    actual_df = sfm.get_topics_analysis_df(input_df)

    pd.testing.assert_frame_equal(actual_df, expected_df)


@mock.patch("phoenix.tag.topic.single_feature_match.plt.show")
@mock.patch("phoenix.tag.topic.single_feature_match.get_topics_analysis_df")
def test_analyse(mock_get_topics_analysis, mock_show):
    """Test the analyse function."""
    analysis_df = pd.DataFrame(
        {
            "topic": ["cat", "cat", "dog", "dog"],
            "matched_features": ["meow", "purr", "woof", "bark"],
            "matched_object_count": [4, 1, 2, 1],
        }
    )
    mock_get_topics_analysis.return_value = analysis_df
    input_df = pd.DataFrame(
        {
            "topic": ["a", "b"],
            "matched_features": [["aa"], ["bb"]],
        }
    )
    expected_analysis_df = sfm.analyse(input_df)

    mock_get_topics_analysis.assert_called_with(input_df)
    mock_show.assert_called_once()
    pd.testing.assert_frame_equal(analysis_df, expected_analysis_df)
