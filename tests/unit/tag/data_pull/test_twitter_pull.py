"""Test twitter JSON pull."""
from typing import Any, Dict

import pandas as pd

from phoenix.tag.data_pull import constants, twitter_pull


def test_medium_type():
    """Test medium_type."""
    entities_with_url = {"urls": [{"url": "https://some.url.com"}]}
    entities_with_empty_urls: Dict[str, Any] = {"urls": []}
    entities_with_no_urls: Dict[str, Any] = {"some_prop": []}
    includes_with_video = {
        "media": [
            {"type": "video"},
            {"type": "photo"},
            {"type": "animated_gif"},
        ]
    }
    includes_with_photo = {
        "media": [
            {"type": "photo"},
            {"type": "video"},
            {"type": "animated_gif"},
        ]
    }
    includes_with_animated_gif = {
        "media": [
            {"type": "animated_gif"},
        ]
    }
    includes_with_no_media = {"some_prop": ["str"]}
    includes_with_empty_media: Dict[str, Any] = {"media": []}
    df = pd.DataFrame(
        [
            # VIDEOS
            {
                "entities": entities_with_url,
                "includes": includes_with_video,
            },
            {
                "entities": entities_with_no_urls,
                "includes": includes_with_video,
            },
            {
                "entities": entities_with_empty_urls,
                "includes": includes_with_video,
            },
            # PHOTOS
            {
                "entities": entities_with_url,
                "includes": includes_with_photo,
            },
            {
                "entities": entities_with_no_urls,
                "includes": includes_with_photo,
            },
            {
                "entities": entities_with_url,
                "includes": includes_with_animated_gif,
            },
            {
                "entities": entities_with_empty_urls,
                "includes": includes_with_animated_gif,
            },
            # LINK
            {
                "entities": entities_with_url,
                "includes": includes_with_no_media,
            },
            {
                "entities": entities_with_url,
                "includes": includes_with_empty_media,
            },
            # TEXT
            {
                "entities": entities_with_no_urls,
                "includes": includes_with_no_media,
            },
            {
                "entities": entities_with_no_urls,
                "includes": includes_with_empty_media,
            },
            {
                "entities": entities_with_empty_urls,
                "includes": includes_with_empty_media,
            },
        ]
    )
    expected_ser = pd.Series(
        [constants.MEDIUM_TYPE_VIDEO] * 3
        + [constants.MEDIUM_TYPE_PHOTO] * 4
        + [constants.MEDIUM_TYPE_LINK] * 2
        + [constants.MEDIUM_TYPE_TEXT] * 3,
        name="medium_type",
    )
    r_ser = twitter_pull.medium_type(df)
    pd.testing.assert_series_equal(r_ser, expected_ser)
