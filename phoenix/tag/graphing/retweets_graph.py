"""Create a retweets network graph with Twitter data."""

import community as community_louvain
import networkx as nx
import pandas as pd

from phoenix.common import artifacts
from phoenix.tag.graphing import graph_utilities as graph_util
from phoenix.tag.graphing import webweb_visualition_configuration as viz_config


def get_data(url: str):
    """Get dataframe from artifacts."""
    return artifacts.dataframes.get(url).dataframe


def create_networkx_graph_from_df(df: pd.DataFrame):
    """Create network graph from dataframe."""
    df.rename(columns={"count": "weight"}, inplace=True)
    return nx.from_pandas_edgelist(
        df, "original_screen_name", "retweet_screen_name", edge_attr="weight"
    )


def graph_cleaner(graph):
    """Remove nodes with low edge counts."""
    clean_graph = graph_util.clean_n_edge_nodes(graph)
    clean_graph = graph_util.clean_n_edge_nodes(clean_graph, n=0)
    return clean_graph


def generate_graph(df: pd.DataFrame, resolution=1.0):
    """Run the graph visualization process."""
    # Create network graph & community calculations
    graph = create_networkx_graph_from_df(df)
    # Remove nodes with one edge
    clean_graph = graph_cleaner(graph)
    # Find and implement partitions
    partitions = community_louvain.best_partition(clean_graph, resolution=resolution)
    nx.set_node_attributes(clean_graph, partitions, "community")
    # community_graph = graph_util.assign_partitions(clean_graph, partitions)
    # Set up and configure visualization
    return clean_graph


def create_visualization(graph):
    """Create the webweb graph from a dataframe."""
    return viz_config.create_retweet_visualization(graph)
