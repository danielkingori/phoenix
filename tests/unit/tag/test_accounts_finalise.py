"""Test finalising accounts and text snippets joined with account classes."""
import pandas as pd
import pytest

from phoenix.tag import accounts_finalise
from phoenix.tag.data_pull import constants


@pytest.fixture
def accounts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "some_other_column": [1, 2, 3],
            "object_user_url": ["acc_2", "acc_2", "acc_3"],
            "account_label": ["label_1", "label_2", "label_1"],
        }
    )


@pytest.fixture
def facebook_posts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "account_url": ["acc_1", "acc_2", "acc_2"],
            "some_column": ["a", "b", "c"],
        }
    )


def test_facebook_posts_objects_accounts_classes(facebook_posts_df, accounts_df):
    expected_df = pd.DataFrame(
        {
            "account_url": ["acc_2", "acc_2", "acc_2", "acc_2"],
            "some_column": ["b", "b", "c", "c"],
            "account_label": ["label_1", "label_2", "label_1", "label_2"],
        }
    )
    out_df = accounts_finalise.objects_accounts_classes(
        "facebook_posts", facebook_posts_df, accounts_df
    )
    pd.testing.assert_frame_equal(expected_df, out_df)


@pytest.fixture
def youtube_videos_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "channel_url": ["acc_1", "acc_2", "acc_2"],
            "some_column": ["a", "b", "c"],
        }
    )


def test_youtube_videos_objects_accounts_classes(youtube_videos_df, accounts_df):
    expected_df = pd.DataFrame(
        {
            "channel_url": ["acc_2", "acc_2", "acc_2", "acc_2"],
            "some_column": ["b", "b", "c", "c"],
            "account_label": ["label_1", "label_2", "label_1", "label_2"],
        }
    )
    out_df = accounts_finalise.objects_accounts_classes(
        "youtube_videos", youtube_videos_df, accounts_df
    )
    pd.testing.assert_frame_equal(expected_df, out_df)


@pytest.fixture
def youtube_comments_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "author_channel_id": ["acc_1", "acc_2", "acc_2"],
            "some_column": ["a", "b", "c"],
        }
    )


@pytest.fixture
def youtube_comments_accounts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "some_other_column": [1, 2, 3],
            "object_user_url": [
                constants.YOUTUBE_CHANNEL_URL + x for x in ["acc_2", "acc_2", "acc_3"]
            ],
            "account_label": ["label_1", "label_2", "label_1"],
        }
    )


def test_youtube_comments_objects_accounts_classes(youtube_comments_df, accounts_df):
    expected_df = pd.DataFrame(
        {
            "author_channel_id": ["acc_2", "acc_2", "acc_2", "acc_2"],
            "some_column": ["b", "b", "c", "c"],
            "account_label": ["label_1", "label_2", "label_1", "label_2"],
        }
    )
    out_df = accounts_finalise.objects_accounts_classes(
        "youtube_comments", youtube_comments_df, accounts_df
    )
    pd.testing.assert_frame_equal(expected_df, out_df)


@pytest.fixture
def tweets_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_screen_name": ["acc_1", "acc_2", "acc_2"],
            "some_column": ["a", "b", "c"],
        }
    )


@pytest.fixture
def tweets_accounts_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "some_other_column": [1, 2, 3],
            "object_user_url": [constants.TWITTER_URL + x for x in ["acc_2", "acc_2", "acc_3"]],
            "account_label": ["label_1", "label_2", "label_1"],
        }
    )


def test_tweets_objects_accounts_classes(tweets_df, accounts_df):
    expected_df = pd.DataFrame(
        {
            "user_screen_name": ["acc_2", "acc_2", "acc_2", "acc_2"],
            "some_column": ["b", "b", "c", "c"],
            "account_label": ["label_1", "label_2", "label_1", "label_2"],
        }
    )
    out_df = accounts_finalise.objects_accounts_classes("tweets", tweets_df, accounts_df)
    pd.testing.assert_frame_equal(expected_df, out_df)
