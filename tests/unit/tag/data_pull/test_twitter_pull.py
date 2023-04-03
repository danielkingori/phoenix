"""Test twitter JSON pull."""
from typing import Any, Dict

import pandas as pd

from phoenix.tag.data_pull import constants, twitter_pull


def test_medium_type():
    """Test medium_type."""
    url = "https://some.url.com"
    entities_with_url = {"urls": [{"url": url}]}
    entities_with_empty_urls: Dict[str, Any] = {"urls": []}
    entities_with_no_urls: Dict[str, Any] = {"some_prop": []}
    media_type_video = "video"
    media_type_photo = "photo"
    media_type_animated_gif = "animated_gif"
    includes_with_video = {
        "media": [
            {"type": media_type_video},
            {"type": media_type_photo},
            {"type": media_type_animated_gif},
        ]
    }
    includes_with_photo = {
        "media": [
            {"type": media_type_photo},
            {"type": media_type_video},
            {"type": media_type_animated_gif},
        ]
    }
    includes_with_animated_gif = {
        "media": [
            {"type": media_type_animated_gif},
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
    expected_medium_ser = pd.Series(
        [constants.MEDIUM_TYPE_VIDEO] * 3
        + [constants.MEDIUM_TYPE_PHOTO] * 4
        + [constants.MEDIUM_TYPE_LINK] * 2
        + [constants.MEDIUM_TYPE_TEXT] * 3,
        name="medium_type",
    )
    expected_type_of_first_media = pd.Series(
        [media_type_video] * 3
        + [media_type_photo] * 2
        + [media_type_animated_gif] * 2
        + [None] * 5,  # type: ignore[list-item]
        name="platform_media_type",
    )
    expected_url_of_first_entity = pd.Series(
        [url, None, None, url, None, url, None, url, url, None, None, None],
        name="url_within_text",
    )
    r_df = twitter_pull.add_medium_type_and_determinants(df)
    pd.testing.assert_series_equal(r_df["medium_type"], expected_medium_ser)
    pd.testing.assert_series_equal(r_df["platform_media_type"], expected_type_of_first_media)
    pd.testing.assert_series_equal(r_df["url_within_text"], expected_url_of_first_entity)
    assert r_df.columns.all(
        [
            "entities",
            "includes",
            "medium_type",
            "platform_media_type",
            "url_within_text",
        ]
    )


def test_stringify_columns_geo_withheld_in_countries():
    """Test stringify_columns stringifies geo and withheld_in_countries columns.

    Also tests that if a column is not in the dataframe(place, coordinates), no new columns are
    instantiated.
    """
    withheld_in_countries = [["NL", "UK"], ["IN"]]
    geo = [
        {"type": "point", "coordinates": [1.0, 2.0]},
        {"type": "point", "coordinates": [1.0, 2.0]},
    ]
    df = pd.DataFrame({"geo": geo, "withheld_in_countries": withheld_in_countries})

    expected_withheld_in_countries = ["""['NL', 'UK']""", """['IN']"""]
    expected_geo = [
        """{'type': 'point', 'coordinates': [1.0, 2.0]}""",
        """{'type': 'point', 'coordinates': [1.0, 2.0]}""",
    ]

    expected_df = pd.DataFrame(
        {"geo": expected_geo, "withheld_in_countries": expected_withheld_in_countries}
    )
    expected_df = expected_df.astype("string")
    result_df = twitter_pull.stringify_columns(df)

    pd.testing.assert_frame_equal(result_df, expected_df)


def test_stringify_columns_place_coordinates():
    """Test stringify_columns stringifies place and coordinates columns.

    Also tests that if a column is not in the dataframe(geo and withheld_in_countries ), no new
    columns are instantiated.
    """
    place = [
        {"place": {"id": "07d9db48bc083000", "name": "Madurodam"}},
        {"place": {"id": "1111111111111", "name": "Big Ben"}},
    ]
    coordinates = [
        [1.0, 2.0],
        [1.0, 2.0],
    ]
    df = pd.DataFrame({"place": place, "coordinates": coordinates})

    expected_place = [
        """{'place': {'id': '07d9db48bc083000', 'name': 'Madurodam'}}""",
        """{'place': {'id': '1111111111111', 'name': 'Big Ben'}}""",
    ]
    expected_coordinates = [
        """[1.0, 2.0]""",
        """[1.0, 2.0]""",
    ]

    expected_df = pd.DataFrame({"place": expected_place, "coordinates": expected_coordinates})
    expected_df = expected_df.astype("string")
    result_df = twitter_pull.stringify_columns(df)

    pd.testing.assert_frame_equal(result_df, expected_df)
