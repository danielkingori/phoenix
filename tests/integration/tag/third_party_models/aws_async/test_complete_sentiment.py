"""Integration test for completion of the sentiment analysis."""
import datetime

import mock
import pytest

from phoenix.tag.third_party_models.aws_async import info_sentiment, job_types, start_sentiment


@mock.patch(
    "phoenix.tag.third_party_models.aws_async.info_sentiment._describe_sentiment_analysis_job"
)
@mock.patch(
    "phoenix.tag.third_party_models.aws_async.start_sentiment._start_sentiment_detection_job"
)
def test_complete_sentiment(
    m_start,
    m_describe,
    tmpdir_url,
    aws_sentiment_objects,
    aws_sentiment_ar_output_url,
    aws_sentiment_en_output_url,
):
    """Test the complete of the sentiment analysis."""
    client = mock.Mock()
    data_access_role_arn = "data_access_role_arn"
    m_start.return_value = job_types.AWSStartedJob(
        job_id="id", job_arn="arn", job_status=job_types.JOB_STATUS_SUBMITTED
    )
    async_job_group = start_sentiment.start_sentiment_analysis_jobs(
        data_access_role_arn, tmpdir_url, aws_sentiment_objects, client
    )
    assert async_job_group

    async_job_group_url = f"{tmpdir_url}/async_job_group.json"

    # Doing the persist as this is part of the full functionality
    _ = job_types.persist_json(async_job_group_url, async_job_group)
    async_job_group_gotten = job_types.get_json(async_job_group_url)

    # Pointing to the test data
    async_job_group_gotten.async_jobs[0].async_job_meta.output_url = aws_sentiment_ar_output_url
    async_job_group_gotten.async_jobs[1].async_job_meta.output_url = aws_sentiment_en_output_url

    m_describe.return_value = job_types.AWSDescribeJob(
        job_id="id",
        job_arn="arn",
        job_status=job_types.JOB_STATUS_SUBMITTED,
        output_url="url",
        start_time=datetime.datetime.now(),
        end_time=datetime.datetime.now(),
    )
    job_infos = info_sentiment.get_job_infos(async_job_group_gotten, client)

    with pytest.raises(RuntimeError) as error:
        info_sentiment.are_processable_jobs(job_infos)
        assert "not yet complete" in str(error.value)

    m_describe.return_value = job_types.AWSDescribeJob(
        job_id="id",
        job_arn="arn",
        job_status=job_types.JOB_STATUS_COMPLETED,
        output_url="url",
        start_time=datetime.datetime.now(),
        end_time=datetime.datetime.now(),
    )

    job_infos = info_sentiment.get_job_infos(async_job_group_gotten, client)
    assert job_infos == [m_describe.return_value, m_describe.return_value]

    m_describe.assert_has_calls(
        [
            mock.call(async_job_group_gotten.async_jobs[0], client),
            mock.call(async_job_group_gotten.async_jobs[1], client),
        ]
    )

    assert info_sentiment.are_processable_jobs(job_infos)
