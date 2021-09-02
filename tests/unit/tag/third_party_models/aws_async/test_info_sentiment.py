"""Unit tests for info_sentiment."""
import datetime

import mock
import pytest

from phoenix.tag.third_party_models.aws_async import info_sentiment, job_types


def create_aws_describe_job(job_status):
    """Create an AWSDescribeJob."""
    return job_types.AWSDescribeJob(
        job_id="id",
        job_arn="arn",
        job_status=job_status,
        output_url="url",
        start_time=datetime.datetime.now(),
        end_time=datetime.datetime.now(),
    )


@mock.patch(
    "phoenix.tag.third_party_models.aws_async.info_sentiment._describe_sentiment_analysis_job"
)
def test_get_job_infos(
    m_describe,
):
    """Test the get_job_infos."""
    client = "client"
    m_async_job_1 = mock.MagicMock(spec=job_types.AsyncJob)
    m_async_job_2 = mock.MagicMock(spec=job_types.AsyncJob)
    m_async_jobs = [m_async_job_1, m_async_job_2]
    m_async_job_group_meta = mock.MagicMock(spec=job_types.AsyncJobGroupMeta)
    async_job_group = job_types.AsyncJobGroup(m_async_job_group_meta, m_async_jobs)  # type: ignore
    m_describe.return_value = create_aws_describe_job(job_types.JOB_STATUS_SUBMITTED)
    job_infos = info_sentiment.get_job_infos(async_job_group, client)

    assert job_infos == [m_describe.return_value, m_describe.return_value]

    m_describe.assert_has_calls(
        [
            mock.call(m_async_job_1, client),
            mock.call(m_async_job_2, client),
        ]
    )


@pytest.mark.parametrize(
    "job_infos",
    [
        [
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
            create_aws_describe_job(job_types.JOB_STATUS_SUBMITTED),
        ],
        [
            create_aws_describe_job(job_types.JOB_STATUS_SUBMITTED),
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
            create_aws_describe_job(job_types.JOB_STATUS_FAILED),
        ],
    ],
)
def test_are_processable_jobs_not_complete(job_infos):
    """Test the processable jobs that are not complete."""
    with pytest.raises(RuntimeError) as error:
        info_sentiment.are_processable_jobs(job_infos)
        assert "not yet complete" in str(error.value)


@pytest.mark.parametrize(
    "job_infos",
    [
        [
            create_aws_describe_job(job_types.JOB_STATUS_FAILED),
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
        ],
        [
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
            create_aws_describe_job(job_types.JOB_STATUS_FAILED),
        ],
    ],
)
def test_are_processable_jobs_invalid(job_infos):
    """Test the processable jobs that are invalid."""
    with pytest.raises(RuntimeError) as error:
        info_sentiment.are_processable_jobs(job_infos)
        assert "invalid" in str(error.value)
        assert job_types.JOB_STATUS_FAILED in str(error.value)


@pytest.mark.parametrize(
    "job_infos",
    [
        [
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
        ],
        [
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
            create_aws_describe_job(job_types.JOB_STATUS_COMPLETED),
        ],
    ],
)
def test_are_processable_jobs(job_infos):
    """Test the processable jobs that are not complete."""
    assert info_sentiment.are_processable_jobs(job_infos)
