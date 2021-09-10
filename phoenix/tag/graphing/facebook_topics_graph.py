"""Facebook topics network visualization."""

import community as community_louvain
import networkx as nx
import pandas as pd

from phoenix.tag.graphing import webweb_visualition_configuration as viz_config


def create_networkx_graph_from_df(data):
    """Create network graph from a dataframe."""
    return nx.from_pandas_edgelist(data, "topic-1", "topic-2", edge_attr="weight")


def assign_partitions(graph):
    """Assigns partition to nodes using community-louvain package."""
    # Define partitions
    partitions = community_louvain.best_partition(graph)
    # Set partitions in graph
    nx.set_node_attributes(graph, partitions, "community")
    return graph


def generate_graph_viz(data: pd.DataFrame):
    """Run the graph visualization process."""
    graph = create_networkx_graph_from_df(data)
    community_graph = assign_partitions(graph)
    web = viz_config.create_facebook_topic_to_topic_visualization(community_graph)
    return web
