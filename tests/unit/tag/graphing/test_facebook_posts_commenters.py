"""Tests for the facebook posts commenters graph."""

import pandas as pd
import pytest

from phoenix.tag.graphing import facebook_posts_commenters


@pytest.fixture
def input_final_facebook_posts_classes() -> pd.DataFrame:
    """Input dataframe of facebook_posts with classes."""
    return pd.DataFrame(
        [
            # Facebook posts 1
            {
                "url_post_id": "fp_1",
                "account_handle": "u_1",
                "class": "c_1",
                "language_sentiment": "POS",
            },
            {
                "url_post_id": "fp_1",
                "account_handle": "u_1",
                "class": "c_2",
                "language_sentiment": "POS",
            },
            # Facebook posts 2
            {
                "url_post_id": "fp_2",
                "account_handle": "u_1",
                "class": "c_1a",
                "language_sentiment": "NEG",
            },
            # Facebook posts 3
            {
                "url_post_id": "fp_3",
                "account_handle": "u_2",
                "class": "c_3",
                "language_sentiment": "NEU",
            },
            {
                "url_post_id": "fp_3",
                "account_handle": "u_2",
                "class": "c_4",
                "language_sentiment": "NEU",
            },
            # Facebook posts 4
            {
                "url_post_id": "fp_4",
                "account_handle": "u_3",
                "class": "c_4",
                "language_sentiment": "POS",
            },
        ]
    )


@pytest.fixture
def input_final_facebook_comments_inherited_accounts_classes() -> pd.DataFrame:
    """Input dataframe of facebook comments inheretied accounts classes."""
    return pd.DataFrame(
        [
            # Commenter 1 for Post 1
            {
                "user_name": "ca_1",
                "user_display_name": "ca_d_1",
                "post_id": "fp_1",
                "account_label": "acc_label_1",
            },
            {
                "user_name": "ca_1",
                "user_display_name": "ca_d_1",
                "post_id": "fp_1",
                "account_label": "acc_label_2",
            },
            # Commenter 1 for post not found
            {
                "user_name": "ca_1",
                "user_display_name": "ca_d_1",
                "post_id": "fp_non",
                "account_label": "acc_label_1",
            },
            {
                "user_name": "ca_1",
                "user_display_name": "ca_d_1",
                "post_id": "fp_non",
                "account_label": "acc_label_2",
            },
            # Commenter 2 for post 1
            {
                "user_name": "ca_2",
                "user_display_name": "ca_2",
                "post_id": "fp_1",
                "account_label": "acc_label_1",
            },
            # Commenter 2 for post 2
            {
                "user_name": "ca_2",
                "user_display_name": "ca_2",
                "post_id": "fp_2",
                "account_label": "acc_label_1",
            },
            # Commenter 2 for post 3
            {
                "user_name": "ca_2",
                "user_display_name": "ca_2",
                "post_id": "fp_3",
                "account_label": "acc_label_1",
            },
            # Commenter 3 for post 3
            {
                "user_name": "ca_3",
                "user_display_name": "ca_3",
                "post_id": "fp_3",
                "account_label": "acc_label_1",
            },
            # User 1 for post 4
            {
                "user_name": "u_1",
                "user_display_name": "u_1",
                "post_id": "fp_4",
                "account_label": "acc_label_3",
            },
        ]
    )


@pytest.fixture
def input_final_facebook_posts_objects_accounts_classes() -> pd.DataFrame:
    """Input dataframe of facebook posts objects account classes."""
    return pd.DataFrame(
        {
            "url_post_id": ["fp_1", "fp_2", "fp_3", "fp_3", "fp_4"],
            "account_handle": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "account_name": ["u_1", "u_1", "u_2", "u_2", "u_3"],
            "account_label": [
                "acc_label_3",
                "acc_label_3",
                "acc_label_3",
                "acc_label_4",
                "acc_label_4",
            ],
            "language_sentiment": ["POS", "NEG", "NEU", "NEU", "POS"],
        }
    )


@pytest.fixture
def edges() -> pd.DataFrame:
    """Expected output edges dataframe."""
    return pd.DataFrame(
        [
            # User 1 commented on Post 4
            {
                "account_id_1": "u_3",
                "account_id_2": "u_1",
                "post_count": 1,
                "class": "c_4",
                "language_sentiment": "POS",
            },
            # Commenter 1 commented on Post 1
            {
                "account_id_1": "u_1",
                "account_id_2": "ca_1",
                "post_count": 1,
                "class": "c_1, c_2",
                "language_sentiment": "POS",
            },
            # Commenter 2 commented on Post 1 and Post 2
            {
                "account_id_1": "u_1",
                "account_id_2": "ca_2",
                "post_count": 2,
                "class": "c_1, c_1a, c_2",
                "language_sentiment": "NEG, POS",
            },
            # Commenter 2 commented on Post 3
            {
                "account_id_1": "u_2",
                "account_id_2": "ca_2",
                "post_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 3 commented on Post 3
            {
                "account_id_1": "u_2",
                "account_id_2": "ca_3",
                "post_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 2 and commenter 3 commented on Post 3
            {
                "account_id_1": "ca_2",
                "account_id_2": "ca_3",
                "post_count": 1,
                "class": "c_3, c_4",
                "language_sentiment": "NEU",
            },
            # Commenter 1 and commenter 2 commented on Post 1
            {
                "account_id_1": "ca_1",
                "account_id_2": "ca_2",
                "post_count": 1,
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
                "account_label": "acc_label_3",
                "type": "account",
            },
            {
                "node_name": "u_2",
                "node_label": "u_2",
                "account_label": "acc_label_3, acc_label_4",
                "type": "account",
            },
            {
                "node_name": "u_3",
                "node_label": "u_3",
                "account_label": "acc_label_4",
                "type": "account",
            },
            {
                "node_name": "ca_1",
                "node_label": "ca_d_1",
                "account_label": "acc_label_1, acc_label_2",
                "type": "commenter",
            },
            {
                "node_name": "ca_2",
                "node_label": "ca_2",
                "account_label": "acc_label_1",
                "type": "commenter",
            },
            {
                "node_name": "ca_3",
                "node_label": "ca_3",
                "account_label": "acc_label_1",
                "type": "commenter",
            },
        ]
    )


def test_process(
    input_final_facebook_posts_classes,
    input_final_facebook_comments_inherited_accounts_classes,
    input_final_facebook_posts_objects_accounts_classes,
    edges,
    nodes,
):
    """Test processing inputs to edges and nodes."""
    fciac = input_final_facebook_comments_inherited_accounts_classes
    fpoac = input_final_facebook_posts_objects_accounts_classes
    output_edges, output_nodes = facebook_posts_commenters.process(
        final_facebook_posts_classes=input_final_facebook_posts_classes,
        final_facebook_comments_inherited_accounts_classes=fciac,
        final_facebook_posts_objects_accounts_classes=fpoac,
    )
    r_edges = edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(drop=True)
    r_output_edges = output_edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(
        drop=True
    )
    pd.testing.assert_frame_equal(r_edges, r_output_edges)
    pd.testing.assert_frame_equal(nodes, output_nodes)


def test_process_limited(
    input_final_facebook_posts_classes,
    input_final_facebook_comments_inherited_accounts_classes,
    input_final_facebook_posts_objects_accounts_classes,
    edges,
    nodes,
):
    """Test processing inputs to edges and nodes."""
    fciac = input_final_facebook_comments_inherited_accounts_classes
    fpoac = input_final_facebook_posts_objects_accounts_classes
    output_edges, output_nodes = facebook_posts_commenters.process(
        final_facebook_posts_classes=input_final_facebook_posts_classes,
        final_facebook_comments_inherited_accounts_classes=fciac,
        final_facebook_posts_objects_accounts_classes=fpoac,
        limit_top_commenters=3,
    )
    r_edges = edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(drop=True)
    r_output_edges = output_edges.sort_values(by=["account_id_1", "account_id_2"]).reset_index(
        drop=True
    )
    r_edges_filtered = r_edges[r_edges["account_id_2"] != "ca_3"]
    r_edges_filtered = r_edges_filtered.reset_index(drop=True)
    pd.testing.assert_frame_equal(r_edges_filtered, r_output_edges)
    nodes_filtered = nodes[~nodes["node_name"].isin(["ca_3"])]
    nodes_filtered = nodes_filtered.reset_index(drop=True)
    pd.testing.assert_frame_equal(nodes_filtered, output_nodes)
