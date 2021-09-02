"""Unit tests for create_aws_started_job."""
import pytest

from phoenix.tag.third_party_models.aws_async import job_types, start_sentiment


def create_async_job_meta():
    """Create AsyncJobMeta."""
    return job_types.AsyncJobMeta(
        job_name="job_name",
        artifacts_base="artifacts_base",
        language_code="language_code",
        input_url="input_url",
        output_url="output_url",
        objects_analysed_url="objects_analysed_url",
    )


def test_create_aws_started_job():
    """Test the create_aws_started_job."""
    async_job_meta = create_async_job_meta()
    job_id = "job_id"
    job_arn = "job_arn"
    job_status = job_types.JOB_STATUS_SUBMITTED
    job_dict = {
        "JobId": job_id,
        "JobArn": job_arn,
        "JobStatus": job_status,
    }
    aws_started_job = start_sentiment.create_aws_started_job(job_dict, async_job_meta)

    assert job_types.are_jobs_equal(
        aws_started_job,
        job_types.AWSStartedJob(
            job_id=job_id,
            job_arn=job_arn,
            job_status=job_status,
        ),
    )


def test_create_aws_started_job_no_arn():
    """Test the create_aws_started_job with no arn."""
    async_job_meta = create_async_job_meta()
    job_id = "job_id"
    job_status = job_types.JOB_STATUS_SUBMITTED
    job_dict = {
        "JobId": job_id,
        "JobStatus": job_status,
    }
    aws_started_job = start_sentiment.create_aws_started_job(job_dict, async_job_meta)

    assert job_types.are_jobs_equal(
        aws_started_job,
        job_types.AWSStartedJob(
            job_id=job_id,
            job_arn=job_types.DEFAULT_JOB_ARN,
            job_status=job_status,
        ),
    )


def test_create_aws_started_job_failed():
    """Test the create_aws_started_job with no arn."""
    async_job_meta = create_async_job_meta()
    job_id = "job_id"
    job_status = job_types.JOB_STATUS_FAILED
    job_dict = {
        "JobId": job_id,
        "JobStatus": job_status,
    }
    with pytest.raises(RuntimeError) as error:
        start_sentiment.create_aws_started_job(job_dict, async_job_meta)
        assert async_job_meta.job_name in str(error.value)


def test_create_aws_started_job_no_id():
    """Test the create_aws_started_job with no id."""
    async_job_meta = create_async_job_meta()
    job_status = job_types.JOB_STATUS_SUBMITTED
    job_dict = {
        "JobStatus": job_status,
    }
    with pytest.raises(RuntimeError) as error:
        start_sentiment.create_aws_started_job(job_dict, async_job_meta)
        assert async_job_meta.job_name in str(error.value)
