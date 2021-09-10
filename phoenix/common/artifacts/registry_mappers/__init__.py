"""Artifact Registry Mappers."""
from phoenix.common.artifacts.registry_mappers import base_mappers, source_mappers, tagging_mappers
from phoenix.common.artifacts.registry_mappers.artifact_keys import ArtifactKey  # noqa: F401
from phoenix.common.artifacts.registry_mappers.default_url_mapper import (  # noqa: F401
    ArtifactURLMapper,
    MapperDict,
    url_mapper,
)


# Shared URLs
DEFAULT_MAPPERS: MapperDict = {
    **source_mappers.MAPPERS,
    **base_mappers.MAPPERS,
    **tagging_mappers.MAPPERS,
}
