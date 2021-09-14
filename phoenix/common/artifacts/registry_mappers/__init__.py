"""Artifact Registry Mappers."""
from phoenix.common.artifacts.registry_mappers import (
    base_mappers,
    final_mappers,
    graphing_mappers,
    source_mappers,
    static_mappers,
    tagging_mappers,
)
from phoenix.common.artifacts.registry_mappers.artifact_keys import ArtifactKey  # noqa: F401
from phoenix.common.artifacts.registry_mappers.default_url_mapper import (  # noqa: F401
    ArtifactURLMapper,
    MapperDict,
    url_mapper,
)


# Shared URLs
DEFAULT_MAPPERS: MapperDict = {
    **graphing_mappers.MAPPERS,
    **static_mappers.MAPPERS,
    **source_mappers.MAPPERS,
    **base_mappers.MAPPERS,
    **tagging_mappers.MAPPERS,
    **final_mappers.MAPPERS,
}


def get_default_mappers() -> MapperDict:
    """Get the default mappers.

    This function is needed so that when nested mappers in the DEFAULT_MAPPERS
    are changed during a session in a notebook they will be automatically reloaded
    in a notebook.

    If you have further problems with changes to nested mappers not updating
    during a session in a notebook then:
    1. Add a empty line between the lines:
        `**source_mappers.MAPPERS,` and `**base_mappers.MAPPERS,`
    2. Save file
    3. Re run the initialise of the ArtifactURLRegistry in your notebook
    3. Remove the empty line that was added in step 1 and save the file.
    """
    return DEFAULT_MAPPERS
