"""Registry Graph mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers import shared_url_mapper, shared_urls
from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


GRAPHING = "graphing/"
OBJECT_TYPE_GRAPH_TYPE = "{OBJECT_TYPE}_{GRAPH_TYPE}/"
# graphing used to match the naming the phoenix repo
GRAPHING_PIPELINE_BASE = f"{shared_urls.TAGGING_PIPELINE_BASE}graphing/"


MAPPERS: MapperDict = {
    "graphing-edges": partial(
        url_mapper,
        (GRAPHING_PIPELINE_BASE + OBJECT_TYPE_GRAPH_TYPE + "edges.parquet"),
    ),
    "graphing-nodes": partial(
        url_mapper,
        (GRAPHING_PIPELINE_BASE + OBJECT_TYPE_GRAPH_TYPE + "nodes.parquet"),
    ),
    "graphing-graphistry-redirect_html": partial(
        shared_url_mapper.dashboard_url_mapper,
        (GRAPHING_PIPELINE_BASE + OBJECT_TYPE_GRAPH_TYPE + "graphistry/redirect.html"),
    ),
    # Legacy below this comment
    # Retweet
    "graphing_runs-retweet_pulled": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "retweet_pulled.parquet"
    ),
    "graphing_runs-retweet_input": partial(url_mapper, shared_urls.GROUP_BY_TWEETS),
    "graphing_runs-retweet_output_graph": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "retweet_graph.html"
    ),
    "graphing_runs-retweet_dashboard_graph": partial(
        shared_url_mapper.dashboard_url_mapper, GRAPHING_PIPELINE_BASE + "retweet_graph.html"
    ),
    # Facebook topics
    "graphing_runs-facebook_topics_graph_pulled": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph_pulled.parquet"
    ),
    "graphing_runs-facebook_topics_output_graph": partial(
        url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph.html"
    ),
    "graphing_runs-facebook_topics_dashboard_graph": partial(
        shared_url_mapper.dashboard_url_mapper, GRAPHING_PIPELINE_BASE + "topics_graph.html"
    ),
}
