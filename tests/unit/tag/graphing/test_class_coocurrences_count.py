"""Test class_coocurrences_count."""
import pandas as pd
import pytest

from phoenix.tag.graphing import class_cooccurences_count, phoenix_graphistry


@pytest.fixture
def input_objects_classes() -> pd.DataFrame:
    """Input dataframe of tweets with classes."""
    return pd.DataFrame(
        {
            "object_user_url": ["object_user_1"] * 7,
            "object_id": ["id_1", "id_1", "id_1", "id_2", "id_2", "id_3", "id_3"],
            "class": ["class_1", "class_4", "class_3", "class_1", "class_2", "class_1", "class_3"],
        }
    )


@pytest.fixture
def edges() -> pd.DataFrame:
    """Expected output edges dataframe."""
    return pd.DataFrame(
        {
            "class_0": ["class_1", "class_1", "class_1", "class_3"],
            "class_1": ["class_2", "class_3", "class_4", "class_4"],
            "times_co-occur": [1, 2, 1, 1],
        }
    )


@pytest.fixture
def nodes() -> pd.DataFrame:
    """Expected output nodes dataframe."""
    return pd.DataFrame(
        {
            "class": ["class_1", "class_2", "class_3", "class_4"],
        }
    )


def test_get_class_combinations(input_objects_classes):
    """Test the get_class_combinations returns combinations"""
    expected_df = pd.DataFrame(
        {
            "object_id": ["id_1", "id_1", "id_1", "id_2", "id_3"],
            "class_0": ["class_1", "class_1", "class_3", "class_1", "class_1"],
            "class_1": ["class_3", "class_4", "class_4", "class_2", "class_3"],
        }
    )

    actual_df = class_cooccurences_count.get_class_combinations(input_objects_classes)

    pd.testing.assert_frame_equal(actual_df, expected_df)


@pytest.mark.parametrize(
    "object_id_col,class_col,expected_object_id_col,expected_class_col_a,expected_class_col_b",
    [
        ("phoenix_post_id", "topic", "phoenix_post_id", "topic_0", "topic_1"),
        ("id", "blep", "id", "blep_0", "blep_1"),
    ],
)
def test_get_class_combinations_with_kwargs(
    object_id_col, class_col, expected_object_id_col, expected_class_col_a, expected_class_col_b
):
    """Test the get_class_combinations returns combinations with kwargs"""
    input_df = pd.DataFrame(
        {
            "object_user_url": ["object_user_1"] * 5,
            object_id_col: ["id_1", "id_1", "id_1", "id_2", "id_2"],
            class_col: ["class_1", "class_4", "class_3", "class_1", "class_2"],
        }
    )

    expected_df = pd.DataFrame(
        {
            expected_object_id_col: ["id_1", "id_1", "id_1", "id_2"],
            expected_class_col_a: ["class_1", "class_1", "class_3", "class_1"],
            expected_class_col_b: ["class_3", "class_4", "class_4", "class_2"],
        }
    )

    actual_df = class_cooccurences_count.get_class_combinations(input_df, object_id_col, class_col)

    pd.testing.assert_frame_equal(actual_df, expected_df)


def test_process(input_objects_classes, nodes, edges):
    """Test processing inputs for classes' co-occurence."""
    actual_edges, actual_nodes = class_cooccurences_count.process(input_objects_classes)

    pd.testing.assert_frame_equal(actual_edges, edges)
    pd.testing.assert_frame_equal(actual_nodes, nodes)


def test_get_plot_config():
    """Test get_plot_config returns a plot config with the important options set."""
    plot_config = class_cooccurences_count.get_plot_config("class_col", "object_type")
    assert isinstance(plot_config, phoenix_graphistry.PlotConfig)
    assert plot_config.edge_source_col == "class_col_0"
    assert plot_config.edge_destination_col == "class_col_1"
    assert plot_config.nodes_col == "class_col"
    assert plot_config.graph_name == "object_type_class_col_co-occurrences"
    assert plot_config.edge_weight_col == "times_co-occur"
