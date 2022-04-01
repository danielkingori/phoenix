"""Jobs types.

Typing the data that will be stored between the
start and complete stages of the asynchronous analysis.
"""
from typing import Any, Dict, Union, cast

import dataclasses
import json

import dacite

from phoenix.common import artifacts, run_datetime
from phoenix.tag.third_party_models.aws_async import job_types


def create_async_job_group_meta(
    analysis_type: str, bucket_url: str, run_dt: run_datetime.RunDatetime
) -> job_types.AsyncJobGroupMeta:
    """Create a AsyncGroupJobMeta."""
    group_job_id = f"{analysis_type}-{run_dt.to_file_safe_str()}"
    artifacts_base = f"{bucket_url}{group_job_id}/"
    return job_types.AsyncJobGroupMeta(
        analysis_type=analysis_type, group_job_id=group_job_id, artifacts_base=artifacts_base
    )


def create_async_job_meta(
    async_job_group_meta: job_types.AsyncJobGroupMeta, language_code: str
) -> job_types.AsyncJobMeta:
    """Create a AsyncJobMeta."""
    # Based on the group meta we have a sub folder for the language
    artifacts_base = f"{async_job_group_meta.artifacts_base}{language_code}/"
    input_url = f"{artifacts_base}input.txt"
    output_url = f"{artifacts_base}output"
    objects_analysed_url = artifacts.dataframes.url(f"{artifacts_base}", "objects_analysed")
    job_name = f"{async_job_group_meta.group_job_id}-{language_code}"
    return job_types.AsyncJobMeta(
        job_name=job_name,
        artifacts_base=artifacts_base,
        language_code=language_code,
        input_url=input_url,
        output_url=output_url,
        objects_analysed_url=objects_analysed_url,
    )


def persist_json(url: str, to_persist: Union[job_types.AsyncJobGroup, None]):
    """Persist a AsyncJobGroup or None."""
    return artifacts.json.persist(url, _normalise_before_persists(to_persist))


def _normalise_before_persists(
    to_persist: Union[job_types.AsyncJobGroup, None]
) -> Union[Dict[Any, Any], None]:
    if isinstance(to_persist, job_types.AsyncJobGroup):
        return dataclasses.asdict(to_persist)

    if to_persist is None:
        return to_persist

    raise ValueError("Not able to persist type {type(to_persist)}")


def get_json(url: str):
    """Get the AsyncJobGroup."""
    raw_dict = artifacts.json.get(url).obj
    if not raw_dict:
        return None
    raw_dict = cast(Dict, raw_dict)  # casting type for mypy because we know it is correct
    return dacite.from_dict(data=raw_dict, data_class=job_types.AsyncJobGroup)


def are_jobs_equal(job_1, job_2):
    """Are jobs equal."""
    job_1_dict = dataclasses.asdict(job_1)
    job_2_dict = dataclasses.asdict(job_2)
    return json.dumps(job_1_dict, sort_keys=True) == json.dumps(job_2_dict, sort_keys=True)
