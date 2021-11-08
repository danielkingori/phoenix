"""Graphing CLI unit tests."""
import mock
import pytest

from phoenix.common import artifacts
from phoenix.common.cli_modules import graphing


@pytest.mark.parametrize(
    "graph_type,parameters,expected_artifact_get_url,expected_notebook_keys,expected_parameters",
    [
        (
            "facebook_posts_topics",
            {"YEAR_FILTER": 2021},
            "final-facebook_posts_topics",
            [
                "tag/data_pull/facebook_topic_pull_graphing.ipynb",
                "tag/graphing/facebook_topic_graph.ipynb",
            ],
            {"OBJECT_TYPE": "facebook_posts"},
        ),
        (
            "facebook_comments_topics",
            {"YEAR_FILTER": 2021},
            "final-facebook_comments_topics",
            [
                "tag/data_pull/facebook_topic_pull_graphing.ipynb",
                "tag/graphing/facebook_topic_graph.ipynb",
            ],
            {"OBJECT_TYPE": "facebook_comments"},
        ),
    ],
)
def test_get_run_config_for_graph_type_facebook_topics(
    graph_type, parameters, expected_artifact_get_url, expected_notebook_keys, expected_parameters
):
    """Test get_run_config_for_graph_type for facebook topics graphs."""
    m_art_reg_url = mock.Mock(spec=artifacts.registry.ArtifactURLRegistry)
    run_config = graphing.get_run_config_for_graph_type(graph_type, m_art_reg_url, parameters)
    assert run_config["notebook_keys"] == expected_notebook_keys
    assert run_config["parameters"] == expected_parameters

    m_art_reg_url.get_url.assert_called_once_with(expected_artifact_get_url, parameters)
    assert run_config["required_artifacts"] == [m_art_reg_url.get_url.return_value]


@pytest.mark.parametrize(
    "graph_type, parameters, expected_run_config",
    [
        (
            "retweets",
            {"YEAR_FILTER": 2021},
            {
                "notebook_keys": [
                    "tag/data_pull/twitter_pull_retweets.ipynb",
                    "tag/graphing/twitter_retweets_graph.ipynb",
                ],
                "parameters": {"OBJECT_TYPE": "tweets"},
                "required_artifacts": [],
            },
        )
    ],
)
def test_get_run_config_for_graph_type_no_required(graph_type, parameters, expected_run_config):
    """Test get_run_config_for_graph_type for run config with no required."""
    m_art_reg_url = mock.Mock(spec=artifacts.registry.ArtifactURLRegistry)
    run_config = graphing.get_run_config_for_graph_type(graph_type, m_art_reg_url, parameters)
    assert run_config == expected_run_config

    m_art_reg_url.get_url.assert_not_called()


def test_get_run_config_for_graph_type_not_supported():
    """Test get_run_config_for_graph_type not supported ."""
    m_art_reg_url = mock.Mock(spec=artifacts.registry.ArtifactURLRegistry)
    graph_type = "not_supported"
    with pytest.raises(ValueError) as error:
        graphing.get_run_config_for_graph_type(graph_type, m_art_reg_url, {})
        assert graph_type in str(error.value)

    m_art_reg_url.get_url.assert_not_called()
