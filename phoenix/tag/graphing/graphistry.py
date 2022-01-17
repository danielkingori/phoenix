"""Graph generation for Graphistry platform.

https://www.graphistry.com/
https://github.com/graphistry/pygraphistry/
"""
from typing import Optional

import dataclasses

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
