"""Start the sentiment analysis."""
from typing import Any, Dict, Union

import boto3
import pandas as pd

from phoenix.common import artifacts, run_datetime
from phoenix.tag.third_party_models import aws_utils
from phoenix.tag.third_party_models.aws_async import job_types, jobs, text_documents_for_analysis


VALID_LANGUAGE_CODES = ["en", "ar"]


def start_sentiment_analysis_jobs(
    run_dt: run_datetime.RunDatetime,
    data_access_role_arn: str,
    bucket_url: str,
    objects: pd.DataFrame,
    client=None,
) -> Union[job_types.AsyncJobGroup, None]:
    """Start sentiment analysis jobs in AWS Comprehend.

    Will start a job for each supported language.
    See docs on async:
    https://docs.aws.amazon.com/comprehend/latest/dg/how-async.html

    Arguments:
        data_access_role_arn: the role that will be used by AWS comprehend
            to access and write to the bucket. See docs:
            https://docs.aws.amazon.com/comprehend/latest/dg/access-control-managing-permissions.html#auth-role-permissions
        bucket_url: the bucket url where the artifacts will be written. Artifacts including:
            - input data as a txt file
            - output data (written by comprehend)
            - objects have been processed as parquet
        objects: objects to do the analysis on
        client: Optional boto 3 client

    Returns:
        AsyncJobGroup when there was a sentiment analysis started
        None if no objects can be analysed
    """
    if count_of_objects_to_be_analysed(objects) == 0:
        return None
    objects["text_bytes_truncate"] = aws_utils.text_bytes_truncate(objects["text"])
    async_job_group_meta = jobs.create_async_job_group_meta(
        "sentiment_analysis", bucket_url, run_dt
    )
    async_jobs = []
    language_codes = [
        obj_lang_code
        for obj_lang_code in objects["language"].unique()
        if obj_lang_code in VALID_LANGUAGE_CODES
    ]
    for language_code in language_codes:
        async_jobs.append(
            start_sentiment_analysis_job(
                data_access_role_arn,
                async_job_group_meta,
                objects,
                language_code,
                client,
            )
        )

    async_job_group = job_types.AsyncJobGroup(
        async_job_group_meta=async_job_group_meta, async_jobs=async_jobs
    )

    return async_job_group


def start_sentiment_analysis_job(
    data_access_role_arn: str,
    async_job_group_meta: job_types.AsyncJobGroupMeta,
    objects: pd.DataFrame,
    language_code: str,
    client=None,
) -> job_types.AsyncJob:
    """Start the sentiment_analysis_jobs filtering objects for language."""
    async_job_meta = jobs.create_async_job_meta(async_job_group_meta, language_code)
    objects_to_analyse = get_objects_to_analyse(objects, async_job_meta.language_code)
    _ = persist_for_sentiment_analysis(async_job_meta.input_url, objects_to_analyse)
    # Need to persist the objects that have been analysed
    # so we can link them to the sentiment results
    _ = artifacts.dataframes.persist(async_job_meta.objects_analysed_url, objects_to_analyse)
    aws_started_job = _start_sentiment_detection_job(
        data_access_role_arn,
        async_job_meta,
        client,
    )
    return job_types.AsyncJob(async_job_meta=async_job_meta, aws_started_job=aws_started_job)


def get_objects_to_analyse(objects: pd.DataFrame, language_code: str) -> pd.DataFrame:
    """Get the object to analyse.

    Adding the line number that will correspond to the line number
    when the output analyse is returned.
    """
    objects_to_analyse = objects[objects["language"] == language_code]
    objects_to_analyse = objects_to_analyse.reset_index(drop=True)
    objects_to_analyse["aws_input_line_number"] = objects_to_analyse.index
    return objects_to_analyse


def _start_sentiment_detection_job(
    data_access_role_arn: str, async_job_meta: job_types.AsyncJobMeta, client
) -> job_types.AWSStartedJob:
    """Make API call to start the detection.

    Raises:
        Will raise an error if not submitted.

    Returns:
        AWSStartedJob
    """
    if client is None:
        client = boto3.client("comprehend")

    job_dict = client.start_sentiment_detection_job(
        InputDataConfig={"S3Uri": async_job_meta.input_url, "InputFormat": "ONE_DOC_PER_LINE"},
        OutputDataConfig={
            "S3Uri": async_job_meta.output_url,
        },
        DataAccessRoleArn=data_access_role_arn,
        JobName=async_job_meta.job_name,
        LanguageCode=async_job_meta.language_code,
    )
    return create_aws_started_job(job_dict, async_job_meta)


def create_aws_started_job(
    job_dict: Dict[Any, Any], async_job_meta: job_types.AsyncJobMeta
) -> job_types.AWSStartedJob:
    """Create the AWSStartedJob."""
    job_status = job_dict["JobStatus"]
    if job_status != job_types.JOB_STATUS_SUBMITTED:
        raise RuntimeError(
            f"Job Name: {async_job_meta.job_name}."
            " AWS job does not have correct status: {job_dict}"
        )

    job_id = job_dict.get("JobId", None)

    if not job_id:
        raise RuntimeError(
            f"Job Name: {async_job_meta.job_name}. AWS job does not have id {job_dict}"
        )

    # It would seem that there is no JobArn when the job has just been submitted
    job_arn = job_dict.get("JobArn", job_types.DEFAULT_JOB_ARN)

    return job_types.AWSStartedJob(job_id=job_id, job_arn=job_arn, job_status=job_status)


def persist_for_sentiment_analysis(url: str, objects_to_analyse: pd.DataFrame):
    """Persist for sentiment_analysis_jobs."""
    return text_documents_for_analysis.persist_text_series(
        url, objects_to_analyse["text_bytes_truncate"]
    )


def count_of_objects_to_be_analysed(df: pd.DataFrame) -> int:
    """Get the count of objects that can be analyised."""
    can_be_analysied = df[df["language"].isin(VALID_LANGUAGE_CODES)]
    return can_be_analysied.shape[0]
