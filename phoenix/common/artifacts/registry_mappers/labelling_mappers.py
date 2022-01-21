"""Registry Labelling Mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


MAPPERS: MapperDict = {
    "labelling-output_notebook_base": partial(url_mapper, "process/labelling_run/{RUN_DATETIME}/"),
}
