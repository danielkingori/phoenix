"""Create a retweets network graph with Twitter data."""

import community as community_louvain
import networkx as nx
import pandas
from webweb import Web

from phoenix.common import artifacts


def get_data(url: str):
    """Get dataframe from artifacts."""
    return artifacts.dataframes.get(url).dataframe


def create_networkx_graph_from_df(df: pandas.DataFrame):
    """Create network graph from dataframe."""
    df.rename(columns={'count': 'weight'}, inplace=True)
    return nx.from_pandas_edgelist(df, "original_screen_name", "retweet_screen_name", edge_attr="weight")


def assign_partitions(graph, partitions):
    """Assign partitions from community-louvain calculations."""
    for node in partitions:
        graph.nodes[node]['community'] = partitions[node]
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
    web.display.colorBy = 'community'
    web.display.sizeBy = 'degree'
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
    partitions = community_louvain.best_partition(graph)
    community_graph = assign_partitions(graph, partitions)
    # Set up and configure visualization
    web = create_visualization(community_graph)
    return web