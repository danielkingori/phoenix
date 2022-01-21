"""Processing utilities."""
from typing import List

import pandas as pd


def reduce_concat_classes(
    df: pd.DataFrame,
    row_id_cols: List[str],
    class_col: str,
) -> pd.DataFrame:
    """Group by row ID, concat class col values, and reduce.

    Other columns in `df` are assumed one-to-one relationship with row_id_cols combination values.

    Output will have the one-to-many row-to-class label relationship turned into one-to-one with
    class labels concatenated with `, ` separator.
    """
    one_to_one_df = (
        df.groupby(row_id_cols)[
            [col for col in df.columns if col not in row_id_cols + [class_col]]
        ]
        .first()
        .reset_index()
    )

    concat_classes_df = df.groupby(row_id_cols)[class_col].apply(list)
    concat_classes_df = concat_classes_df.reset_index()
    concat_classes_df[class_col] = concat_classes_df[class_col].apply(
        lambda l: ", ".join(sorted(set(l)))
    )

    out_df = one_to_one_df.merge(
        concat_classes_df, how="inner", on=row_id_cols, validate="one_to_one"
    )
    return out_df
