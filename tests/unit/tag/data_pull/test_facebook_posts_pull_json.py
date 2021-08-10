"""Test facebook JSON pull."""
import numpy as np
import pandas as pd

from phoenix.tag.data_pull import facebook_posts_pull_json


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
