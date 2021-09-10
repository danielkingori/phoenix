"""Artifact Registry Mappers."""
from typing import Dict

from phoenix.common.artifacts.registry_mappers import base_mappers, source_mappers
from phoenix.common.artifacts.registry_mappers.artifact_keys import ArtifactKey
from phoenix.common.artifacts.registry_mappers.default_url_mapper import (  # noqa: F401
    ArtifactURLMapper,
    url_mapper,
)


# Shared URLs
DEFAULT_MAPPERS: Dict[ArtifactKey, ArtifactURLMapper] = {
    **source_mappers.MAPPERS,
    **base_mappers.MAPPERS,
}
