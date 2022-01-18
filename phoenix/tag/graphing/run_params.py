"""Run params for graphing."""
from typing import Any, Dict, List, Optional

import dataclasses

from phoenix.common.run_params import base, general


@dataclasses.dataclass
class GraphingRunParamsURLs(base.RunParams):
    """Graph generation and persisting URLs.

    Generalised to apply to all different graph types.
    """

    config: Dict[str, Any]
    input_datasets: Dict[str, str]  # Dict[`artifact_key`, `corresponding_artifact_url`]
    edges: str
    nodes: str
    graphistry_redirect_html: Optional[str]


@dataclasses.dataclass
class GraphingRunParams(base.RunParams):
    """Graph generation and persisting run params.

    Generalised to apply to all different graph types.
    """

    urls: GraphingRunParamsURLs
    general: general.GeneralRunParams


def create(
    artifacts_environment_key: str,
    tenant_id: str,
    run_datetime_str: Optional[str],
    object_type: str,
    year_filter: int,
    month_filter: int,
    graph_type: str,
    input_datasets_artifact_keys: List[str],
    edges_url: Optional[str] = None,
    nodes_url: Optional[str] = None,
    graphistry_redirect_html_url: Optional[str] = None,
) -> GraphingRunParams:
    """Create GraphingRunParams."""
    general_run_params = general.create(artifacts_environment_key, tenant_id, run_datetime_str)

    art_url_reg = general_run_params.art_url_reg
    url_config = {
        "OBJECT_TYPE": object_type,
        "YEAR_FILTER": year_filter,
        "MONTH_FILTER": month_filter,
        "GRAPH_TYPE": graph_type,
    }

    input_datasets_urls = {
        artifact_key: art_url_reg.get_url(artifact_key, url_config)
        for artifact_key in input_datasets_artifact_keys
    }

    if edges_url is None:
        edges_url = art_url_reg.get_url("graphing-edges", url_config)
    if nodes_url is None:
        nodes_url = art_url_reg.get_url("graphing-nodes", url_config)
    if graphistry_redirect_html_url is None:
        graphistry_redirect_html_url = art_url_reg.get_url(
            "graphing-graphistry-redirect_html", url_config
        )

    urls = GraphingRunParamsURLs(
        config=url_config,
        input_datasets=input_datasets_urls,
        edges=edges_url,
        nodes=nodes_url,
        graphistry_redirect_html=graphistry_redirect_html_url,
    )

    return GraphingRunParams(
        general=general_run_params,
        urls=urls,
    )
