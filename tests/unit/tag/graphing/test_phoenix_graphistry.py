"""Test Graphistry and related functionality."""
import numpy as np
import pandas as pd
import pytest

from phoenix.tag.graphing import phoenix_graphistry


def test_fillna_string_type_cols():
    """Test filling string cols with empty string."""

    df = pd.DataFrame({"string_col": ["a", None, np.nan, "b"], "num_col": [None, 1, np.nan, 2]})
    expected_df = pd.DataFrame({"string_col": ["a", "", "", "b"], "num_col": [None, 1, np.nan, 2]})
    out_df = phoenix_graphistry.fillna_string_type_cols(df)
    pd.testing.assert_frame_equal(out_df, expected_df)


@pytest.fixture
def in_edges() -> pd.DataFrame:
    """Input edges."""
    df = pd.DataFrame(
        {
            "start": ["node_0", 1, "node_0", "node_0"],
            "end": [1, 4, 3, 2],
            "edge_attrib": ["a", "b", "c", "d"],
        }
    )
    return df


@pytest.fixture
def in_nodes() -> pd.DataFrame:
    """Input nodes."""
    df = pd.DataFrame(
        {"node_col": [5, 2, "node_0", 1, 3, 4], "node_attrib": ["", "x", "xx", "y", "z", "zz"]}
    )
    return df


def test_compute_graph_metrics(in_edges, in_nodes):
    """Test computing and concating metrics to edges and nodes df."""
    plot_config = phoenix_graphistry.PlotConfig(
        edge_source_col="start",
        edge_destination_col="end",
        nodes_col="node_col",
        graph_name="foo",
        graph_description="bar",
        edge_weight_col=None,
        directed=True,
    )
    expected_edges = pd.DataFrame(
        {
            "start": ["node_0", 1, "node_0", "node_0"],
            "end": [1, 4, 3, 2],
            "edge_attrib": ["a", "b", "c", "d"],
            "edge_betweenness": [2.0, 2.0, 1.0, 1.0],
        }
    )
    expected_nodes = pd.DataFrame(
        {
            "node_col": [5, 2, "node_0", 1, 3, 4],
            "node_attrib": ["", "x", "xx", "y", "z", "zz"],
            "pagerank": [None, 0.185, 0.144, 0.185, 0.185, 0.301],
            "betweenness": [
                None,
                0.0,
                0.0,
                1.0,
                0.0,
                0.0,
            ],
            # Spin glass is non-deterministic and not worth (right now) figuring fixing seed
            # "community_spin_glass": [None, 1, 1, 0, 1, 0],
            "community_infomap": [None, 0, 0, 0, 0, 0],
            "community_louvain": [None, 0, 0, 1, 0, 1],
        }
    )

    out_edges, out_nodes = phoenix_graphistry.compute_graph_metrics(
        in_edges, in_nodes, plot_config
    )
    pd.testing.assert_frame_equal(out_edges, expected_edges)
    pd.testing.assert_frame_equal(
        out_nodes.drop(columns=["community_spin_glass"]), expected_nodes, check_less_precise=True
    )


def test_compute_graph_metrics_undirected(in_edges, in_nodes):
    """Test computing and concating metrics to edges and nodes df when undirected graph."""
    plot_config = phoenix_graphistry.PlotConfig(
        edge_source_col="start",
        edge_destination_col="end",
        nodes_col="node_col",
        graph_name="foo",
        graph_description="bar",
        edge_weight_col=None,
        directed=False,
    )
    expected_edges = pd.DataFrame(
        {
            "start": ["node_0", 1, "node_0", "node_0"],
            "end": [1, 4, 3, 2],
            "edge_attrib": ["a", "b", "c", "d"],
            "edge_betweenness": [6.0, 4.0, 4.0, 4.0],
        }
    )
    expected_nodes = pd.DataFrame(
        {
            "node_col": [5, 2, "node_0", 1, 3, 4],
            "node_attrib": ["", "x", "xx", "y", "z", "zz"],
            "pagerank": [None, 0.131, 0.358, 0.2455, 0.131, 0.134],
            "betweenness": [None, 0.0, 5.0, 3.0, 0.0, 0.0],
            # Spin glass is non-deterministic and not worth (right now) figuring fixing seed
            # "community_spin_glass": [None, 1, 1, 0, 1, 0],
            "community_infomap": [None, 0, 0, 0, 0, 0],
            "community_louvain": [None, 0, 0, 1, 0, 1],
        }
    )

    out_edges, out_nodes = phoenix_graphistry.compute_graph_metrics(
        in_edges, in_nodes, plot_config
    )
    pd.testing.assert_frame_equal(out_edges, expected_edges)
    pd.testing.assert_frame_equal(
        out_nodes.drop(columns=["community_spin_glass"]), expected_nodes, check_less_precise=True
    )


def test_compute_graph_metrics_undirected_edge_weight(in_edges, in_nodes):
    """Test computing and concating metrics when undirected graph with edge weights.

    Annoyingly, setting the edge weight _seems_ to only affect `pagerank` output. I'm pretty sure
    it should affect betweenness etc. also.
    """
    in_edges["weights"] = [0.1, 1, 0.2, 0.1]
    plot_config = phoenix_graphistry.PlotConfig(
        edge_source_col="start",
        edge_destination_col="end",
        nodes_col="node_col",
        graph_name="foo",
        graph_description="bar",
        edge_weight_col="weights",
        directed=False,
    )
    expected_edges = pd.DataFrame(
        {
            "start": ["node_0", 1, "node_0", "node_0"],
            "end": [1, 4, 3, 2],
            "edge_attrib": ["a", "b", "c", "d"],
            "weights": [0.1, 1, 0.2, 0.1],
            "edge_betweenness": [6.0, 4.0, 4.0, 4.0],
        }
    )
    expected_nodes = pd.DataFrame(
        {
            "node_col": [5, 2, "node_0", 1, 3, 4],
            "node_attrib": ["", "x", "xx", "y", "z", "zz"],
            "pagerank": [None, 0.0784, 0.228, 0.303, 0.127, 0.264],
            "betweenness": [None, 0.0, 5.0, 3.0, 0.0, 0.0],
            # Spin glass is non-deterministic and not worth (right now) figuring fixing seed
            # "community_spin_glass": [None, 2, 1, 0, 1, 0],
            "community_infomap": [None, 0, 0, 0, 0, 0],
            "community_louvain": [None, 0, 0, 1, 0, 1],
        }
    )

    out_edges, out_nodes = phoenix_graphistry.compute_graph_metrics(
        in_edges, in_nodes, plot_config
    )
    pd.testing.assert_frame_equal(out_edges, expected_edges)
    pd.testing.assert_frame_equal(
        out_nodes.drop(columns=["community_spin_glass"]), expected_nodes, check_less_precise=True
    )


def test_compute_graph_metrics_disconnected_graph():
    """Test computing and concating metrics when a graph with unconnected sub-graphs."""
    in_edges = pd.DataFrame(
        {
            "start": [0, 0, 0, 4, 5],
            "end": [1, 2, 3, 5, 4],
            "edge_attrib": ["a", "b", "c", "d", "e"],
            "weights": [0.1, 1, 0.2, 0.1, 0.5],
        }
    )
    in_nodes = pd.DataFrame(
        {"node_col": [0, 1, 2, 3, 4, 5], "node_attrib": ["a", "b", "c", "d", "e", "f"]}
    )
    plot_config = phoenix_graphistry.PlotConfig(
        edge_source_col="start",
        edge_destination_col="end",
        nodes_col="node_col",
        graph_name="foo",
        graph_description="bar",
        edge_weight_col="weights",
        directed=True,
    )
    out_edges, out_nodes = phoenix_graphistry.compute_graph_metrics(
        in_edges, in_nodes, plot_config
    )
    assert "community_spin_glass" not in out_nodes.columns
