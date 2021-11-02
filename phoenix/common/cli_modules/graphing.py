"""Graphing CLI Interface."""
from typing import Any, Dict



def get_run_config_for_graph_type(
    graph_type: str,
    art_url_reg: artifacts.registry.ArtifactURLRegistry,
    parameters: Dict[Any, Any],
):
    """Get the run config for the graph type.

    Arguments:
        graph_type (str): facebook_posts_topics, facebook_comments_topics, retweets
        art_url_reg (ArtifactURLRegistry): artifacts URL registiry to get the needed URLs
        parameters for the config (Dict): parameters for the config

    Returns:
        Dict: {
            notebook_keys: List[str],
            parameters: Dict[str, Any],
            required_artifacts: List[urls]
        }
    """
    run_dict: Dict[str, Any] = {}
    if graph_type == "facebook_posts_topics":
        run_dict["notebook_keys"] = [
            "tag/data_pull/facebook_topic_pull_graphing.ipynb",
            "tag/graphing/facebook_topic_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "facebook_posts"}
        run_dict["required_artifacts"] = [
            art_url_reg.get_url("final-facebook_posts_topics", parameters)
        ]
    elif graph_type == "facebook_comments_topics":
        run_dict["notebook_keys"] = [
            "tag/data_pull/facebook_topic_pull_graphing.ipynb",
            "tag/graphing/facebook_topic_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "facebook_comments"}
        run_dict["required_artifacts"] = [
            art_url_reg.get_url("final-facebook_comments_topics", parameters)
        ]
    elif graph_type == "retweets":
        run_dict["notebook_keys"] = [
            "tag/data_pull/twitter_pull_retweets.ipynb",
            "tag/graphing/twitter_retweets_graph.ipynb",
        ]
        run_dict["parameters"] = {"OBJECT_TYPE": "tweets"}
        run_dict["required_artifacts"] = []
    else:
        raise ValueError(f"Graph type: {graph_type} is not supported")

    return run_dict
