"""Utilities for graphing."""

import logging

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
    parsed_url = tentaclio.urls.URL(path)
    open_extra_args = {}
    if parsed_url.scheme == "s3":
        content_type = "text/html"
        open_extra_args["upload_extra_args"] = {"ContentType": content_type}

    with tentaclio.open(path, "w", **open_extra_args) as f:
        f.write(graph.html)


def save_dashboard_graph(graph, url):
    """Save the dashboard graph if URL is set."""
    if not url:
        logging.info("Not saving graph to dashboard URL.")
        return None

    save_graph(graph, url)
    logging.info(f"Saved graph to optional dashboard URL: {url}")
