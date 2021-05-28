"""Artifact Registry."""
from typing import Any, Dict
from typing_extensions import Literal, Protocol

import datetime
from functools import partial

from phoenix.common.artifacts import urls


DEFAULT_ENVIRONMENT_KEY = "local"


ArifactKey = Literal[
    "source-posts",
    "source-fb_post_source_api_notebook",
    "base-to_process_posts",
]


class ArtifactURLMapper(Protocol):
    """Protocal for the artifactURLMapper."""

    def __call__(
        self,
        artifact_key: ArifactKey,
        url_config: Dict[str, Any],
        environment_key: str = DEFAULT_ENVIRONMENT_KEY,
    ) -> str:
        """Protocal for the artifactURLMapper."""
        ...


def url_mapper(
    format_str: str,
    artifact_key: ArifactKey,
    url_config: Dict[str, Any],
    environment_key: str = DEFAULT_ENVIRONMENT_KEY,
):
    """Generalised url mapper."""
    prefix = default_url_prefix(artifact_key, url_config, environment_key)
    url_str_formated = format_str.format(**url_config)
    return f"{prefix}{url_str_formated}"


def default_url_prefix(
    artifact_key: str, url_config: Dict[str, Any], environment_key: str = DEFAULT_ENVIRONMENT_KEY
):
    """URL prefix for static artifacts."""
    if environment_key == DEFAULT_ENVIRONMENT_KEY:
        return f"{urls.get_local()}"

    raise ValueError(f"No url for environment_key: {environment_key}")


DEFAULT_MAPPERS: Dict[ArifactKey, ArtifactURLMapper] = {
    "source-posts": partial(
        url_mapper, "source_runs/{RUN_DATE}/source-posts-{RUN_ISO_TIMESTAMP}.json"
    ),
    "source-fb_post_source_api_notebook": partial(
        url_mapper, "source_runs/{RUN_DATE}/fb_post_source_api-{RUN_ISO_TIMESTAMP}.ipynb"
    ),
    "base-to_process_posts": partial(url_mapper, "base/to_process/posts-{RUN_ISO_TIMESTAMP}.json"),
}


class ArtifactURLRegistry:
    """Registry of the artifact urls."""

    environment_key: str
    run_datetime: datetime.datetime
    mappers: Dict[ArifactKey, ArtifactURLMapper]

    def __init__(
        self,
        run_datetime: datetime.datetime,
        environment_key: str = DEFAULT_ENVIRONMENT_KEY,
        mappers: Dict[ArifactKey, ArtifactURLMapper] = DEFAULT_MAPPERS,
    ):
        """Init ArtifactURLRegistry."""
        self.environment_key = environment_key
        self.run_datetime = run_datetime
        self.mappers = mappers

    def get_run_iso_timestamp(self) -> str:
        """Get the run date."""
        return self.run_datetime.isoformat()

    def get_run_date(self) -> str:
        """Get the run date."""
        return self.run_datetime.strftime("%Y-%m-%d")

    def get_url(self, artifact_key: ArifactKey, url_config: Dict[str, Any] = {}) -> str:
        """Get the URL for the artifact key."""
        url_config = self._build_url_config(url_config)
        for mapper_key, url_fn in self.mappers.items():
            if artifact_key == mapper_key:
                return url_fn(artifact_key, url_config, self.environment_key)

        raise ValueError(f"No url for artifact key: {artifact_key}")

    def _build_url_config(self, url_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build the url config."""
        if "RUN_ISO_TIMESTAMP" not in url_config:
            url_config["RUN_ISO_TIMESTAMP"] = self.get_run_iso_timestamp()

        if "RUN_DATE" not in url_config:
            url_config["RUN_DATE"] = self.get_run_date()

        return url_config
