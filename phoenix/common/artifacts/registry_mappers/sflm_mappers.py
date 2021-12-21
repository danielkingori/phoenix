"""Registry Single Feature to Label Mappers."""
from functools import partial

from phoenix.common.artifacts.registry_mappers.default_url_mapper import MapperDict, url_mapper


SFLM_BASE = "config/sflm/"
OBJECT_BASE = "{OBJECT_TYPE}"


MAPPERS: MapperDict = {
    "sflm-single_object_type": partial(
        url_mapper,
        (SFLM_BASE + OBJECT_BASE + "_sflm_config.parquet"),
    ),
    "sflm-output_notebook_base": partial(url_mapper, "process/sflm_run/{RUN_DATETIME}/"),
}
