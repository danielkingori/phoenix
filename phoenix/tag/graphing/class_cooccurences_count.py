"""Get counts of class co-occurences."""
from itertools import combinations

import pandas as pd


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
