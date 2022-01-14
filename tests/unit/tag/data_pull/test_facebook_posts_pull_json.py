"""Test facebook JSON pull."""
import numpy as np
import pandas as pd

from phoenix.tag.data_pull import constants, facebook_posts_pull_json


def test_map_score():
    """Test map score."""
    df = pd.DataFrame({"score": [1, 2]})
    r_df = facebook_posts_pull_json.map_score("total_interactions", df)
    pd.testing.assert_frame_equal(
        r_df,
        pd.DataFrame(
            {
                "total_interactions": [1.0, 2.0],
                "overperforming_score": [np.nan, np.nan],
                "interaction_rate": [np.nan, np.nan],
                "underperforming_score": [np.nan, np.nan],
            }
        ),
    )


def test_medium_type():
    """Test medium_type."""
    df = pd.DataFrame(
        {
            "type": [
                "link",
                "album",
                "photo",
                "igtv",
                "live_video",
                "live_video_complete",
                "live_video_scheduled",
                "native_video",
                "video",
                "vine",
                "youtube",
            ]
        }
    )
    r_ser = facebook_posts_pull_json.medium_type(df)
    pd.testing.assert_series_equal(
        r_ser,
        pd.Series(
            [constants.MEDIUM_TYPE_LINK]
            + [constants.MEDIUM_TYPE_PHOTO] * 2
            + [constants.MEDIUM_TYPE_VIDEO] * 8,
            name="medium_type",
        ),
    )
