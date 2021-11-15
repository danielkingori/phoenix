"""Test channels functionality."""
import pandas as pd

from phoenix.scrape.youtube import channels


def test_get_channel_ids_str():
    """Test _get_channel_ids_str."""
    df = pd.DataFrame({"channel_id": ["1", "2", "3"]})
    assert "1,2,3" == channels._get_channel_ids_str(df)
