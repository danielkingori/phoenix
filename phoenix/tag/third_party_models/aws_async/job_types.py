"""Jobs types.

Typing the data that will be stored between the
start and complete stages of the asynchronous analysis.
"""
from typing import List, Literal

import dataclasses
import uuid

from phoenix.common import artifacts


# TODO refactor so that the Literal is created from the constants
JobStatusType = Literal[
    "SUBMITTED",
    "IN_PROGRESS",
    "COMPLETED",
    "FAILED",
    "STOP_REQUESTED",
    "STOPPED",
]

# JobStatus Constants
JOB_STATUS_SUBMITTED: JobStatusType = "SUBMITTED"
JOB_STATUS_IN_PROGRESS: JobStatusType = "IN_PROGRESS"
JOB_STATUS_COMPLETED: JobStatusType = "COMPLETED"
JOB_STATUS_FAILED: JobStatusType = "FAILED"
JOB_STATUS_STOP_REQUESTED: JobStatusType = "STOP_REQUESTED"
JOB_STATUS_STOPPED: JobStatusType = "STOPPED"


@dataclasses.dataclass
class AsyncJobMeta:
    """Meta for an asynchronous job.

    A separate class because the meta data is needed for create an AsyncJob
    and AWSStartedJob.
    """

    job_name: str
    artifacts_base: str
    language_code: str
    input_url: str
    output_url: str
    objects_analysed_url: str


@dataclasses.dataclass
class AWSStartedJob:
    """AWS start job data."""

    job_id: str
    job_arn: str
    job_status: JobStatusType


@dataclasses.dataclass
class AsyncJob:
    """Asynchronous job for an async AWS job."""

    async_job_meta: AsyncJobMeta
    aws_started_job: AWSStartedJob


@dataclasses.dataclass
class AsyncJobGroupMeta:
    """Meta for a group of AsyncJobGroupMeta.

    A separate class because the meta data is needed for create the AsyncJobGroup.
    """

    analysis_type: str
    group_job_id: str
    artifacts_base: str


@dataclasses.dataclass
class AsyncJobGroup:
    """A group of AsyncJobs."""

    async_job_group_meta: AsyncJobGroupMeta
    async_jobs: List[AsyncJob] = dataclasses.field(default_factory=list)


def create_async_job_group_meta(analysis_type: str, bucket_url: str) -> AsyncJobGroupMeta:
    """Create a AsyncGroupJobMeta."""
    # Using uuid for the moment but it might be better to use RunDatetime at some point
    group_job_id = f"{analysis_type}-{uuid.uuid4()}"
    artifacts_base = f"{bucket_url}{group_job_id}/"
    return AsyncJobGroupMeta(
        analysis_type=analysis_type, group_job_id=group_job_id, artifacts_base=artifacts_base
    )


def create_async_job_meta(
    async_job_group_meta: AsyncJobGroupMeta, language_code: str
) -> AsyncJobMeta:
    """Create a AsyncJobMeta."""
    # Based on the group meta we have a sub folder for the language
    artifacts_base = f"{async_job_group_meta.artifacts_base}{language_code}/"
    input_url = f"{artifacts_base}input.txt"
    output_url = f"{artifacts_base}output"
    objects_analysed_url = artifacts.dataframes.url(f"{artifacts_base}", "objects_analysed")
    job_name = f"{async_job_group_meta.group_job_id}-{language_code}"
    return AsyncJobMeta(
        job_name=job_name,
        artifacts_base=artifacts_base,
        language_code=language_code,
        input_url=input_url,
        output_url=output_url,
        objects_analysed_url=objects_analysed_url,
    )
