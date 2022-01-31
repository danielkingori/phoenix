"""Processing utilities."""
from typing import List, Tuple

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


def account_post_commenter_graph_to_commenter_edges(
    edges, nodes
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Turn an account-post-commenter graph into a commenter graph."""
    edges_df = edges[~edges["times_commented"].isna()]
    edges_df = edges_df.merge(
        edges_df, how="inner", left_on="destination_node", right_on="destination_node"
    )
    edges_df = edges_df[edges_df["source_node_x"] != edges_df["source_node_y"]]
    edges_df = edges_df.merge(
        nodes[["node_name", "class"]], how="left", left_on="destination_node", right_on="node_name"
    )
    edges_df["joint_num_comments"] = edges_df["times_commented_x"] + edges_df["times_commented_y"]
    edges_df["edge_key"] = edges_df.apply(
        lambda row: "-".join(sorted([row["source_node_x"], row["source_node_y"]])), axis=1
    )
    edges_df = edges_df[edges_df.duplicated(subset=["edge_key"])]
    edges_df = edges_df.merge(
        edges[edges["times_commented"].isna()],
        how="left",
        left_on="destination_node",
        right_on="destination_node",
    )
    edges_df = edges_df.merge(
        nodes[nodes["type"] == "account"][["node_name", "account_label"]],
        how="left",
        left_on="source_node",
        right_on="node_name",
    )
    count_df = (
        edges_df.groupby(["source_node_x", "source_node_y"])[["joint_num_comments"]]
        .sum()
        .reset_index()
    )
    concat_classes_df = edges_df.groupby(["source_node_x", "source_node_y"])["class"].apply(list)
    concat_classes_df = concat_classes_df.apply(
        lambda l: ", ".join(
            sorted(
                set(
                    [
                        item
                        for sublist in [classes_string.split(", ") for classes_string in l]
                        for item in sublist
                    ]
                )
            )
        )
    )
    concat_classes_df = concat_classes_df.reset_index()
    df = count_df.merge(
        concat_classes_df, how="left", on=["source_node_x", "source_node_y"], validate="1:1"
    )
    concat_account_labels_df = edges_df.groupby(["source_node_x", "source_node_y"])[
        "account_label"
    ].apply(list)
    concat_account_labels_df = concat_account_labels_df.apply(
        lambda l: ", ".join(
            sorted(
                set(
                    [
                        item
                        for sublist in [
                            classes_string.split(", ")
                            for classes_string in l
                            if classes_string and not pd.isnull(classes_string)
                        ]
                        for item in sublist
                    ]
                )
            )
        )
    )
    concat_account_labels_df = concat_account_labels_df.reset_index()
    df = df.merge(
        concat_account_labels_df, how="left", on=["source_node_x", "source_node_y"], validate="1:1"
    )
    df = df.rename(columns={"source_node_x": "source_node", "source_node_y": "destination_node"})

    nodes_df = nodes[nodes["type"] == "commenter"]
    nodes_df = nodes_df.drop(columns=["type"])
    return df, nodes_df


def commenters_group_edges(
    df: pd.DataFrame, object_id_column_name: str, size_column_name: str
) -> pd.DataFrame:
    """Group the edges for commenters.

    Creating `class` `language_sentiment` and size column with given name.
    """
    df_classes = reduce_concat_classes(
        df[["account_id_1", "account_id_2", "class"]], ["account_id_1", "account_id_2"], "class"
    )
    df_language_sentiment = reduce_concat_classes(
        df[["account_id_1", "account_id_2", "language_sentiment"]],
        ["account_id_1", "account_id_2"],
        "language_sentiment",
    )
    df_class_language_sentiment = df_classes.merge(
        df_language_sentiment, on=["account_id_1", "account_id_2"], how="inner"
    )
    # Remove the class duplication
    df_size = df[["account_id_1", "account_id_2", object_id_column_name]]
    # DataFrame with row per post and user name
    df_size = df_size.drop_duplicates()
    df_size = (
        df_size.groupby(["account_id_1", "account_id_2"]).size().reset_index(name=size_column_name)
    )
    df = df_size.merge(
        df_class_language_sentiment, on=["account_id_1", "account_id_2"], validate="one_to_one"
    )
    return df
