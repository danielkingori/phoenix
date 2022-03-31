"""Jobs types.

Typing the data that will be stored between the
start and complete stages of the asynchronous analysis.
"""
from typing import List, Literal, Optional

import dataclasses
import datetime


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

DEFAULT_JOB_ARN = "not-given"


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


@dataclasses.dataclass
class AWSDescribeJob:
    """AWS describe job data."""

    job_id: str
    job_status: JobStatusType
    output_url: str
    submit_time: datetime.datetime
    end_time: Optional[datetime.datetime]
