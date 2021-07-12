"""Network visualization for social media data."""
import itertools

import community as community_louvain
import networkx as nx
import pandas as pd

from phoenix.common import artifacts


def get_unique_groups(data, user_scheme):
    """Gets distinct users with topic attached."""
    # TODO: fix for 'username' and "user_name"
    return data.groupby(user_scheme)["topic"].unique()


def permutations_remove_duplicates(array):
    """Creates permutations and removes copies."""
    result = []
    for p in itertools.permutations(array, 2):
        if p <= p[::-1]:
            result.append(p)
    return result


def generate_permutations(data):
    """Get all permutations in dataset."""
    return [permutations_remove_duplicates(data[key]) for key in data.keys()]


def flatten_dataset(data):
    """Flattens the dataset."""
    return [item for sublist in data for item in sublist]


def _rename_columns(data):
    """Rename 'size' column to 'weight'."""
    data.columns = ["topic-1", "topic-2", "weight"]
    return data


def get_weighted_dataframe(data):
    """Gets weight of each edge based on duplicates across users in collection."""
    return _rename_columns(data.groupby(data.columns.to_list(), as_index=False).size())


def create_networkx_graph_from_df(data):
    """Create network graph from a dataframe."""
    return nx.from_pandas_edgelist(data, "topic-1", "topic-2", edge_attr="weight")


def assign_partitions(graph):
    """Assigns partition to nodes using community-louvain package."""
    # Define partitions
    partitions = community_louvain.best_partition(graph)
    # Set partitions in graph
    nx.set_node_attributes(graph, partitions, "group")
    return graph


def set_webweb_vis_settings(web):
    """Set webweb visualization settings."""
    web.display.scaleLinkWidth = True
    web.display.linkLength = 400
    web.display.scaleLinkOpacity = True
    web.display.showNodeNames = True
    web.display.colorBy = "degree"
    web.display.sizeBy = "degree"
    web.display.width = "1200"
    web.display.height = "900"


def prepare_dataset(data_url, user_schema):
    """Prepare the dataset."""
    # Get the datafra,e.
    data = artifacts.dataframes.get(data_url).dataframe
    groups = get_unique_groups(data, user_schema)
    perms = generate_permutations(groups)
    flat = flatten_dataset(perms)
    graph_df = pd.DataFrame(flat)
    weighted_df = get_weighted_dataframe(graph_df)
    graph = create_networkx_graph_from_df(weighted_df)
    community_graph = assign_partitions(graph)
    return community_graph
