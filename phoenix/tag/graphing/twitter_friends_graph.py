"""Create a friends network graph with Twitter data."""

import community as community_louvain
import networkx as nx
import pandas as pd

from phoenix.common import artifacts
from phoenix.scrape import twitter_queries
from phoenix.tag.graphing import webweb_visualition_configuration as webviz


def get_data(url: str):
    """Get dataframe from artifacts."""
    return artifacts.dataframes.get(url).dataframe


def create_networkx_graph_from_df(df: pd.DataFrame):
    """Create networkx graph from dataframe."""
    return nx.from_pandas_edgelist(df, "user_1", "user_2")


def set_node_attrs(graph: nx.Graph, df: pd.DataFrame):
    """Set seed_user attribute for coloring of seed users."""
    attrs: dict = {}
    # This is removed because the order of operations is different
    #   and now the df and graph don't fully match.
    # for user in list(set(df.user_2)):
    #     attrs[user] = {}
    #     attrs[user]["seed_user"] = False
    for user in list(set(df.user_1)):
        # This should go second. It is a bit crude
        #   but there is a chance it overwrites already written attrs.
        attrs[user] = {}
        attrs[user]["seed_user"] = True
    nx.set_node_attributes(graph, attrs)
    return graph


def clean_single_edge_nodes(graph: nx.Graph, degrees):
    """Remove nodes with a degree of 1 for a cleaner graph."""
    to_be_removed = [x for x in graph.nodes() if graph.degree(x) <= degrees]
    for x in to_be_removed:
        graph.remove_node(x)
    return graph


def assign_partitions(graph, partitions):
    """Assign partitions from community-louvain calculations."""
    for node in partitions:
        graph.nodes[node]["community"] = partitions[node]
    return graph


def _get_user_lookup(api, user_ids):
    """Get user lookup."""
    return api.lookup_users(
        user_ids=user_ids,
        include_entities=False,
        tweet_mode="compact",
    )


def get_user_lookup(api, query):
    """Run get user lookup and return results."""
    for users in _get_user_lookup(api, query):
        yield users


def get_user_list(graph: nx.Graph):
    """Split up node list for query size limits."""
    node_list = list(graph.nodes)
    api = twitter_queries.connect_twitter_api()
    users = []
    for n in range(0, len(node_list), 100):
        users.extend(get_user_lookup(api, node_list[n : n + 100]))
    return users


def build_mapper(users):
    """Build networkx mapper."""
    mapper = {}
    for user in users:
        mapper[user.id] = user.screen_name
    return mapper


def map_nodes(graph, mapper):
    """Map new names to nodes."""
    return nx.relabel_nodes(graph, mapper)


def create_mapped_graph(graph):
    """Manage mapper process and return mapped graph."""
    users = get_user_list(graph)
    mapper = build_mapper(users)
    return map_nodes(graph, mapper)


def generate_graph_viz(url: str, resolution=1.0, degrees=1):
    """Run the graph visualization process."""
    # Get data
    df = get_data(url)
    # Create network graph & community calculations
    graph = create_networkx_graph_from_df(df)
    graph = clean_single_edge_nodes(graph, degrees)
    # Create mapped graph with new names
    map_graph = create_mapped_graph(graph)
    map_graph = set_node_attrs(map_graph, df)
    # Calculate and create partitions
    partitions = community_louvain.best_partition(map_graph, resolution=resolution)
    community_graph = assign_partitions(map_graph, partitions)
    ## TODO Find solution that is not webweb because this graph is too large for the package
    # Build webweb visualization
    # web = webviz.create_twitter_friends_visualization(community_graph)
    return community_graph
