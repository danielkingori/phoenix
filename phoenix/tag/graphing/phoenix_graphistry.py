"""Graph generation for Graphistry platform.

https://www.graphistry.com/
https://github.com/graphistry/pygraphistry/
"""
from typing import Optional

import dataclasses
import os

import graphistry
import pandas as pd

from phoenix.common.run_params import base


@dataclasses.dataclass
class PlotConfig(base.RunParams):
    """Graphistry plot configuration.

    Used in conjuntion with edges and nodes dataframes to generate a graph plot with `plot`.
    """

    edge_source_col: str
    edge_destination_col: str
    nodes_col: str
    graph_name: str
    graph_description: str
    edge_weight_col: Optional[str] = None


def plot(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    config: PlotConfig,
    graph_name_prefix: Optional[str] = None,
) -> str:
    """Generate a Graphistry plot from edges, nodes, and config given, returning graph URL.

    Args:
        edges: Dataframe of edges, can include extra arbitrary columns of attributes for edges.
        nodes: Dataframe of nodes, can include extra arbitrary columns of attributes for edges.
        config: The phoenix_graphistry dataclass config.
        graph_name_prefix: Optional string that will be prefixed onto the graph name when uploaded
            to Graphistry. Primarily used to inject tenant ID as graph prefix.
    """
    graphistry.register(
        api=3,
        username=os.environ["GRAPHISTRY_USERNAME"],
        password=os.environ["GRAPHISTRY_PASSWORD"],
    )

    if graph_name_prefix is not None:
        graph_name = f"{graph_name_prefix}_{config.graph_name}"
    else:
        graph_name = config.graph_name

    g = graphistry.bind(
        source=config.edge_source_col,
        destination=config.edge_destination_col,
        node=config.nodes_col,
    )
    if config.edge_source_col is not None:
        g = g.bind(edge_weight=config.edge_source_col)

    g = g.edges(edges)
    g = g.nodes(nodes)
    return g.plot(name=graph_name, description=config.graph_description, render=False)


def form_redirect_html(url: str) -> str:
    """Produce string of HTML file that automatically redirects to given URL when loaded."""
    return f'<meta http-equiv="refresh" content="0; url={url}" />'
