"""Test channels functionality."""
import mock
import pandas as pd

from phoenix.scrape.youtube import channels


YOUTUBE_MODULE_STR = "phoenix.scrape.youtube"


def test_get_channel_ids_str():
    """Test _get_channel_ids_str."""
    df = pd.DataFrame({"channel_id": ["1", "2", "3"]})
    assert "1,2,3" == channels._get_channel_ids_str(df)


@mock.patch(f"{YOUTUBE_MODULE_STR}.channels.DEFAULT_PARTS_TO_REQUEST", ["d1", "d2", "d3"])
def test_get_part_str_default():
    """Test _get_channel_ids_str."""
    assert "d1,d2,d3" == channels._get_part_str()


@mock.patch(f"{YOUTUBE_MODULE_STR}.channels.DEFAULT_PARTS_TO_REQUEST", ["d1", "d2", "d3"])
def test_get_part_str_set():
    """Test _get_channel_ids_str."""
    assert "g1,g2" == channels._get_part_str(["g1", "g2"])
