"""Complete the sentiment analysis."""
from typing import Any, Dict, List

import boto3

from phoenix.tag.third_party_models.aws_async import job_types


def get_job_infos(async_job_group, client=None) -> List[job_types.AWSDescribeJob]:
    """Get the status and information of the jobs."""
    job_infos = []
    for async_job in async_job_group.async_jobs:
        job_info = _describe_sentiment_analysis_job(async_job, client)
        job_infos.append(job_info)
    return job_infos


def _describe_sentiment_analysis_job(async_job: job_types.AsyncJob, client=None):
    """Make AWS call.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/comprehend.html?highlight=start_sentiment#Comprehend.Client.describe_sentiment_detection_job
    """
    if client is None:
        client = boto3.client("comprehend")

    job_dict = client.describe_sentiment_detection_job(JobId=async_job.aws_started_job.job_id)
    return create_aws_describe_job(job_dict, async_job)


def create_aws_describe_job(
    job_dict: Dict[Any, Any], async_job: job_types.AsyncJob
) -> job_types.AWSDescribeJob:
    """Create AWSDescribeJob."""
    job_dict_main = job_dict["SentimentDetectionJobProperties"]
    return job_types.AWSDescribeJob(
        job_id=job_dict_main["JobId"],
        job_status=job_dict_main["JobStatus"],
        output_url=job_dict_main["OutputDataConfig"]["S3Uri"],
        submit_time=job_dict_main["SubmitTime"],
        end_time=job_dict_main.get("EndTime", None),
    )


def are_processable_jobs(jobs: List[job_types.AWSDescribeJob]):
    """Are the AWSDescribeJob processable."""
    for job in jobs:
        is_processable_job(job)
    return True


def is_processable_job(job: job_types.AWSDescribeJob):
    """Raise error is not valid status."""
    waiting_status = [job_types.JOB_STATUS_SUBMITTED, job_types.JOB_STATUS_IN_PROGRESS]
    complete_status = job_types.JOB_STATUS_COMPLETED
    valid_status_list = waiting_status + [complete_status]
    if job.job_status not in valid_status_list:
        raise RuntimeError(
            f"Job with id {job.job_id}, has invalid status {job.job_status}."
            " Please go to AWS comprehend console for more information."
        )

    if job.job_status in waiting_status:
        raise RuntimeError(
            f"Job with id {job.job_id}, is not yet complete."
            " Please try again once complete. See AWS comprehend console for more information."
        )

    return True
