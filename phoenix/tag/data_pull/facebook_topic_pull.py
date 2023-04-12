"""Facebook topics data pull."""
from typing import List

import itertools

import pandas as pd

from phoenix.common import artifacts


def get_data(url: str):
    """Get dataframe from artifacts."""
    return artifacts.dataframes.get(url).dataframe


def get_unique_groups(data):
    """Gets distinct users with topic attached."""
    # TODO: "user_name"
    if "user_name" in data.keys():
        key = "user_name"
    else:
        key = "account_name"
    series = data.groupby(key)["topic"].unique()
    series = series.apply(lambda x: [i for i in x if pd.notna(i)])
    return series


def generate_permutations(data):
    """Get all permutations in dataset."""
    return [permutations_remove_duplicates(data[key]) for key in data.keys()]


def permutations_remove_duplicates(array: List[str]):
    """Creates permutations and removes copies."""
    result = []
    for p in itertools.permutations(array, 2):
        if p <= p[::-1]:
            result.append(p)
    return result


def flatten_dataset(data):
    """Flattens the dataset."""
    return [item for sublist in data for item in sublist]


def _rename_columns(data):
    """Rename 'size' column to 'weight'."""
    data.columns = ["topic-1", "topic-2", "weight"]
    return data


def get_weighted_dataframe(data):
    """Gets weight of each edge based on duplicates across users in collection."""
    return _rename_columns(data.groupby(data.columns.to_list(), as_index=False).size())


def prepare_dataset(data):
    """Prepare the dataset."""
    # Get the dataframe.
    # data = get_data(url)  # handle this in the notebook
    # Manipulate the dataframe
    groups = get_unique_groups(data)
    perms = generate_permutations(groups)
    flat = flatten_dataset(perms)
    graph_df = pd.DataFrame(flat)
    weighted_df = get_weighted_dataframe(graph_df)
    return weighted_df
