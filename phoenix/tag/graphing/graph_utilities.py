"""Utilities for graphing."""

import networkx as nx
import tentaclio


def clean_n_edge_nodes(graph: nx.Graph, n: int = 1):
    """Remove nodes with n degree, default 1, for a cleaner graph."""
    to_be_removed = [x for x in graph.nodes() if graph.degree(x) <= n]
    for x in to_be_removed:
        graph.remove_node(x)
    return graph


def get_partitions_set(partitions):
    """Get a list of the different partitions."""
    # This output would go to the webweb metadata for coloring,
    #   but I was unable to make it work.
    parts = [partitions[k] for k in partitions.keys()]
    return list(set(parts))


def save_graph(graph, path):
    """Saves the webweb visualization to the specified path with Tentaclio."""
    with tentaclio.open(path, "w") as f:
        f.write(graph.html)
