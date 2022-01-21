"""Get counts of class co-occurences."""
from typing import Tuple

from itertools import combinations

import pandas as pd


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
