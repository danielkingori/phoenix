"""Graph generation for Graphistry platform.

https://www.graphistry.com/
https://github.com/graphistry/pygraphistry/
"""
from typing import Optional, Tuple

import dataclasses
import logging
import os

import graphistry
import pandas as pd
from igraph._igraph import InternalError as IGraphInternalError

from phoenix.common.run_params import base


logger = logging.getLogger(__name__)


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
    directed: bool = True


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
    edges, nodes = compute_graph_metrics(edges, nodes, config)

    edges = fillna_string_type_cols(edges)
    nodes = fillna_string_type_cols(nodes)

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
        point_title=config.nodes_col,
    )
    if config.edge_weight_col is not None:
        g = g.bind(edge_weight=config.edge_weight_col)

    g = g.edges(edges)
    g = g.nodes(nodes)

    g = g.addStyle(
        logo={
            "url": "https://phoenix-dmaps-dashboard.s3.amazonaws.com/datavaluepeople/logo.png",
            "dimensions": {"maxWidth": 100, "maxHeight": 100},
        }
    )

    default_settings = {
        "pointsOfInterestMax": 20,
        "gravity": 0.7,
        "pointOpacity": 0.95,
        "edgeInfluence": 2,
    }
    if config.directed:
        directed_settings = {"showArrows": True, "edgeCurvature": 0.2}
    else:
        directed_settings = {"showArrows": False, "edgeCurvature": 0.05}
    g = g.settings(url_params={**default_settings, **directed_settings})

    return g.plot(name=graph_name, description=config.graph_description, render=False)


def form_redirect_html(url: str) -> str:
    """Produce string of HTML file that automatically redirects to given URL when loaded."""
    return f'<meta http-equiv="refresh" content="0; url={url}" />'


def fillna_string_type_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Fill string type cols nan/None values with empty string.

    Usage reasons:
    - Doing `CONTAINS(some_attribute, "some_string")` filters on a attribute e (col) which
      contained null/None values would (use to) cause an Graphistry error (a bug, now fixed)
    - When selecting a node/edge that has a null/None value for that attribute, then the attribute
      doesn't show up in the data popup for that node/edge. This might be confusing for a user, so
      better to make it always show with an empty string.
    """
    df = df.copy()
    for col in df:
        if pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].fillna("")
    return df


def compute_graph_metrics(
    edges: pd.DataFrame,
    nodes: pd.DataFrame,
    config: PlotConfig,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute metrics for graph and concating as new columns onto edges and nodes dfs.

    Metrics are things like centrality, node importance, and clusters.
    """
    g = graphistry.bind(
        source=config.edge_source_col,
        destination=config.edge_destination_col,
        node=config.nodes_col,
        point_title=config.nodes_col,
    )
    ig = g.pandas2igraph(edges, directed=config.directed)

    # This is only needed for `igraph.betweenness` which bugs out if you give it the attrib name :/
    if config.edge_weight_col is not None:
        weight_values = edges[config.edge_weight_col].values
    else:
        weight_values = None

    ig.vs["pagerank"] = ig.pagerank(directed=config.directed, weights=config.edge_weight_col)
    ig.vs["betweenness"] = ig.betweenness(directed=config.directed, weights=weight_values)
    ig.es["edge_betweenness"] = ig.edge_betweenness(
        directed=config.directed, weights=config.edge_weight_col
    )
    try:
        ig.vs["community_spin_glass"] = ig.community_spinglass(
            spins=12, stop_temp=0.1, cool_fact=0.9, weights=config.edge_weight_col
        ).membership
    except IGraphInternalError:
        logger.info("Graph contains disconnected components, so Spin-Glass clustering failed.")
    uig = ig.copy()
    uig.to_undirected()
    ig.vs["community_infomap"] = uig.community_infomap().membership
    try:
        ig.vs["community_louvain"] = uig.community_multilevel(
            weights=config.edge_weight_col
        ).membership
    except IGraphInternalError:
        logger.info("Graph contains disconnected components, so Louvain clustering failed.")

    ig_nodes = pd.DataFrame([x.attributes() for x in ig.vs])
    ig_edges = pd.DataFrame([x.attributes() for x in ig.es])

    edges = pd.concat([edges, ig_edges["edge_betweenness"]], axis=1)
    nodes = nodes.merge(ig_nodes, how="left", left_on=config.nodes_col, right_on=config.nodes_col)

    return (edges, nodes)
