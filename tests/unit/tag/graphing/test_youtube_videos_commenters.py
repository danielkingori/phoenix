"""Tests for the youtube videos commenters graph."""

import pandas as pd
import pytest

from phoenix.tag.graphing import youtube_videos_commenters


@pytest.fixture
def input_final_youtube_videos_classes() -> pd.DataFrame:
    """Input dataframe of youtube_videos with classes."""
    return pd.DataFrame(
        [
            # Object 1
            {
                "id": "fp_1",
                "channel_id": "u_1",
                "class": "c_1",
                "language_sentiment": "POS",
            },
            {
                "id": "fp_1",
                "channel_id": "u_1",
                "class": "c_2",
                "language_sentiment": "POS",
            },
            # YouTube videos 2
            {
                "id": "fp_2",
                "channel_id": "u_1",
                "class": "c_1a",
                "language_sentiment": "NEG",
            },
            # YouTube videos 3
            {
                "id": "fp_3",
                "channel_id": "u_2",
                "class": "c_3",
                "language_sentiment": "NEU",
            },
            {
                "id": "fp_3",
                "channel_id": "u_2",
                "class": "c_4",
                "language_sentiment": "NEU",
            },
            # YouTube videos 4
            {
                "id": "fp_4",
                "channel_id": "u_3",
                "class": None,
                "language_sentiment": "POS",
            },
        ]
    )


@pytest.fixture
def input_final_youtube_comments_objects_accounts_classes() -> pd.DataFrame:
    """Input dataframe of youtube comments accounts classes."""
    return pd.DataFrame(
        [
            # Commenter 1 for Video 1
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_1",
                "account_label": "commenter_label_1",
            },
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_1",
                "account_label": "commenter_label_2",
            },
            # Commenter 1 for post not found
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_non",
                "account_label": "commenter_label_1",
            },
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_non",
                "account_label": "commenter_label_2",
            },
            # Commenter 2 for post 1
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_1",
                "account_label": "commenter_label_1",
            },
            # Commenter 2 for post 2
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_2",
                "account_label": "commenter_label_1",
            },
            # Commenter 2 for post 3
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_3",
                "account_label": "commenter_label_1",
            },
            # Commenter 3 has no account_label so is excluded from this
            # User 1 for post 4
            {
                "author_channel_id": "u_1",
                "author_display_name": "u_1",
                "video_id": "fp_4",
                "account_label": "commenter_label_3",
            },
        ]
    )


@pytest.fixture
def input_final_youtube_comments() -> pd.DataFrame:
    """Input dataframe of youtube comments accounts classes."""
    return pd.DataFrame(
        [
            # Commenter 1 for Video 1
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_1",
            },
            # Commenter 1 for post not found
            {
                "author_channel_id": "ca_1",
                "author_display_name": "ca_d_1",
                "video_id": "fp_non",
            },
            # Commenter 2 for post 1
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_1",
            },
            # Commenter 2 for post 2
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_2",
            },
            # Commenter 2 for post 3
            {
                "author_channel_id": "ca_2",
                "author_display_name": "ca_2",
                "video_id": "fp_3",
            },
            # Commenter 3 for post 3
            {
                "author_channel_id": "ca_3",
                "author_display_name": "ca_3",
                "video_id": "fp_3",
            },
            # User 1 for post 4
            {
                "author_channel_id": "u_1",
                "author_display_name": "u_1",
                "video_id": "fp_4",
            },
        ]
    )


@pytest.fixture
def input_final_youtube_videos_objects_accounts_classes() -> pd.DataFrame:
    """Input dataframe of facebook posts objects account classes."""
    return pd.DataFrame(
        {
            "id": ["fp_1", "fp_2", "fp_3", "fp_3", "fp_4"],
            "channel_id": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "channel_title": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "account_label": [
                "acc_label_1",
                "acc_label_1",
                "acc_label_1",
                "acc_label_2",
                "acc_label_2",
            ],
            "language_sentiment": ["POS", "NEG", "NEU", "NEU", "POS"],
        }
    )


@pytest.fixture
def edges() -> pd.DataFrame:
    """Expected output edges dataframe."""
    return pd.DataFrame(
        [
            # User 1 commented on Video 4
            {
                "account_id_1": "u_3",
                "account_id_2": "u_1",
                "video_count": 1,
                "class": "",
                "language_sentiment": "POS",
            },
            # Commenter 1 commented on Video 1
            {
                "account_id_1": "u_1",
                "account_id_2": "ca_1",
                "video_count": 1,
                "class": "c_1, c_2",
                "language_sentiment": "POS",
            },
            # Commenter 2 commented on Video 1 and Video 2
            {
                "account_id_1": "u_1",
                "account_id_2": "ca_2",
                "video_count": 2,
                "class": "c_1, c_1a, c_2",
                "language_sentiment": "NEG, POS",
            },
            # Commenter 2 commented on Video 3
            {
                "account_id_1": "u_2",
                "account_id_2": "ca_2",
                "video_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 3 commented on Video 3
            {
                "account_id_1": "u_2",
                "account_id_2": "ca_3",
                "video_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 2 and commenter 3 commented on Video 3
            {
                "account_id_1": "ca_2",
                "account_id_2": "ca_3",
                "video_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 1 and commenter 2 commented on Video 1
            {
                "account_id_1": "ca_1",
                "account_id_2": "ca_2",
                "video_count": 1,
                "class": "c_1, c_2",
                "language_sentiment": "POS",
            },
        ]
    )


@pytest.fixture
def nodes() -> pd.DataFrame:
    """Expected output nodes dataframe."""
    return pd.DataFrame(
        [
            {
                "node_name": "u_1",
                "node_label": "u_1",
                "account_label": "acc_label_1",
                "type": "channel",
            },
            {
                "node_name": "u_2",
                "node_label": "u_2",
                "account_label": "acc_label_1, acc_label_2",
                "type": "channel",
            },
            {
                "node_name": "u_3",
                "node_label": "u_3",
                "account_label": "acc_label_2",
                "type": "channel",
            },
            {
                "node_name": "ca_1",
                "node_label": "ca_d_1",
                # Inherits the account label from the connected u_1
                "account_label": "acc_label_1, commenter_label_1, commenter_label_2",
                "type": "commenter",
            },
            {
                "node_name": "ca_2",
                "node_label": "ca_2",
                # Inherits the account label from the connected u_1 and u_2
                "account_label": "acc_label_1, acc_label_2, commenter_label_1",
                "type": "commenter",
            },
            {
                "node_name": "ca_3",
                "node_label": "ca_3",
                # Inherits the account label from the connected u_2
                # It has no account labale
                "account_label": "acc_label_1, acc_label_2",
                "type": "commenter",
            },
        ]
    )


def test_process(
    input_final_youtube_videos_classes,
    input_final_youtube_comments,
    input_final_youtube_comments_objects_accounts_classes,
    input_final_youtube_videos_objects_accounts_classes,
    edges,
    nodes,
):
    """Test processing inputs to edges and nodes."""
    ycoac = input_final_youtube_comments_objects_accounts_classes
    yvoac = input_final_youtube_videos_objects_accounts_classes
    output_edges, output_nodes = youtube_videos_commenters.process(
        final_youtube_videos_classes=input_final_youtube_videos_classes,
        final_youtube_comments=input_final_youtube_comments,
        final_youtube_comments_objects_accounts_classes=ycoac,
        final_youtube_videos_objects_accounts_classes=yvoac,
        quantile_of_commenters=0.1,
    )
    r_edges = edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(drop=True)
    r_output_edges = output_edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(r_edges, r_output_edges)
    pd.testing.assert_frame_equal(nodes, output_nodes)
