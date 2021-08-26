"""Create a friends network graph with Twitter data."""

import community as community_louvain
import networkx as nx
import pandas as pd
from webweb import Web

from phoenix.common import artifacts
from phoenix.tag.graphing.retweets_graph import get_data


def create_networkx_graph_from_df(df: pd.DataFrame):
    return nx.from_pandas_edgelist(df, "user_1", "user_2")


def set_node_attrs(graph: nx.Graph, df: pd.DataFrame):
    """Set seed_user attribute for coloring of seed users."""
    attrs = {}
    for user in list(set(df.user_2)):
        attrs[user] = {}
        attrs[user]["seed_user"] = False
    for user in list(set(df.user_1)):
        # This is a bit crude because it overwrites already written attrs.
        attrs[user] = {}
        attrs[user]["seed_user"] = True
    nx.set_node_attributes(graph, attrs)
    return graph


def clean_single_edge_nodes(graph: nx.Graph):
    """Remove nodes with a degree of 1 for a cleaner graph."""
    to_be_removed = [x for x in graph.nodes() if graph.degree(x) <= 1]
    for x in to_be_removed:
        graph.remove_node(x)
    return graph


def assign_partitions(graph, partitions):
    """Assign partitions from community-louvain calculations."""
    for node in partitions:
        graph.nodes[node]["community"] = partitions[node]
    return graph


def create_visualization(graph: nx.Graph):
    """Create network graph visualization."""
    web = Web(nx_G=graph)
    web.display.scaleLinkWidth = True
    web.display.scaleLinkOpacity = True
    web.display.linkLength = 25
    web.display.linkStrength = 2
    web.display.charge = 10
    web.display.gravity = 0.2
    web.display.colorBy = "community"
    web.display.sizeBy = "degree"
    web.display.width = 1200
    web.display.height = 1200
    # web.show()
    return web


def generate_graph_viz(url: str):
    """Run the graph visualization process."""
    # Get data
    df = get_data(url)
    # Create network graph & community calculations
    graph = create_networkx_graph_from_df(df)
    graph = set_node_attrs(graph, df)
    graph = clean_single_edge_nodes(graph)
    partitions = community_louvain.best_partition(graph)
    community_graph = assign_partitions(graph, partitions)
    web = create_visualization(community_graph)
    return web


