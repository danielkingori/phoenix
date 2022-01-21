"""Get counts of class co-occurences."""
from typing import Tuple

from itertools import combinations

import pandas as pd

from phoenix.tag.graphing import phoenix_graphistry


def process(
    objects_df: pd.DataFrame,
    object_id_col: str = "object_id",
    class_col: str = "class",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process objects with classes into class nodes and class coocurrence edges."""
    # edges
    class_combinations_df = get_class_combinations(objects_df, object_id_col, class_col)

    edges_df = (
        class_combinations_df.groupby([f"{class_col}_0", f"{class_col}_1"]).size().reset_index()
    )
    edges_df = edges_df.rename(columns={0: "times_co-occur"})

    # nodes

    nodes_df = pd.DataFrame(pd.concat([edges_df[f"{class_col}_0"], edges_df[f"{class_col}_1"]]))
    nodes_df = nodes_df.rename(columns={0: class_col})
    nodes_df = nodes_df.drop_duplicates(ignore_index=True)
    nodes_df = nodes_df.sort_values(by=class_col).reset_index(drop=True)

    return edges_df, nodes_df


def get_class_combinations(
    class_df: pd.DataFrame, object_id_col: str = "object_id", class_col: str = "class"
) -> pd.DataFrame:
    """Get combinations of classes co-occurring in objects.

    Args:
        class_df (pd.DataFrame): dataframe with a row for each combination of object_id and
            class. eg. If object_id_1 is labelled with class_1 and class_2, it has two rows [[
            object_id_1, class_1], [object_id_1, class_2]]
        object_id_col (str): column name of the object identifier. Default: "object_id"
        class_col (str): column name of the class. Default: "class"
    Returns:
         pd.DataFrame: dataframe with three columns: `object_id_col`, f"{class_col}_0",
            and f"{class_col}_1". with the unique combination of
    """
    class_df = class_df.sort_values([object_id_col, class_col])
    class_combination_df = pd.DataFrame(
        [
            [o_id, x, y]
            for o_id, g in class_df.groupby(object_id_col)[class_col]
            for x, y in combinations(g, 2)
        ],
        columns=[object_id_col, f"{class_col}_0", f"{class_col}_1"],
    )

    return class_combination_df


def get_plot_config(class_col: str, object_type: str) -> phoenix_graphistry.PlotConfig:
    """Get plot config for class_co-occurences."""
    plot_config = phoenix_graphistry.PlotConfig(
        edge_source_col=f"{class_col}_0",
        edge_destination_col=f"{class_col}_1",
        nodes_col=class_col,
        graph_name=f"{object_type}_{class_col}_co-occurrences",
        graph_description=f"""
            Graph showing co-occurrences of {class_col} between {object_type}.
            Nodes: {object_type}
            Edges: Exist where {object_type} have both of the {class_col}s. Edge
                weight is number of {object_type} that have both of the {class_col}.
        """,
        edge_weight_col="times_co-occur",
    )

    return plot_config
