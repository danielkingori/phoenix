"""Integration test for start of sentiment."""
import abc
import datetime

import mock
import pandas as pd

from phoenix.common import artifacts, run_datetime
from phoenix.tag.third_party_models.aws_async import (
    job_types,
    start_sentiment,
    text_documents_for_analysis,
)


def Any(*cls):
    """Mock class for an argument."""

    class Any(metaclass=abc.ABCMeta):
        def __eq__(self, other):
            return isinstance(other, cls)

    for c in cls:
        Any.register(c)
    return Any()


@mock.patch(
    "phoenix.tag.third_party_models.aws_async.start_sentiment._start_sentiment_detection_job"
)
def test_start_sentiment(m_aws_call, tmpdir_url, aws_sentiment_objects):
    """Test the start of the sentiment."""
    client = mock.Mock()
    data_access_role_arn = "data_access_role_arn"
    m_aws_call.return_value = job_types.AWSStartedJob(
        job_id="id", job_arn="arn", job_status=job_types.JOB_STATUS_SUBMITTED
    )
    dt = datetime.datetime(2000, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    run_dt = run_datetime.RunDatetime(dt)
    async_job_group = start_sentiment.start_sentiment_analysis_jobs(
        run_dt, data_access_role_arn, tmpdir_url, aws_sentiment_objects, client
    )
    assert async_job_group
    assert (
        async_job_group.async_job_group_meta.group_job_id
        == "sentiment_analysis-20000102T030405.000000Z"
    )
    # For each language a job should be made
    assert len(async_job_group.async_jobs) == 2

    # Two calls as each language should make an API call
    calls = [
        mock.call(data_access_role_arn, Any(job_types.AsyncJobMeta), client),
        mock.call(data_access_role_arn, Any(job_types.AsyncJobMeta), client),
    ]
    m_aws_call.assert_has_calls(calls)

    # Arabic language checks
    ar_job = async_job_group.async_jobs[0]
    ar_objects = artifacts.dataframes.get(ar_job.async_job_meta.objects_analysed_url).dataframe
    # Only Arabic language should be submitted
    expected_ar = pd.DataFrame(
        {
            "object_id": [1, 2, 3],
            "text": ["t1", "t2", "t3"],
            "language": ["ar", "ar", "ar"],
            "text_bytes_truncate": ["t1", "t2", "t3"],
            # important that line numbers are correct
            "aws_input_line_number": [0, 1, 2],
        }
    )
    pd.testing.assert_frame_equal(ar_objects, expected_ar)

    text_to_analysis = text_documents_for_analysis.read_text_series(
        ar_job.async_job_meta.input_url
    )
    pd.testing.assert_series_equal(
        expected_ar["text_bytes_truncate"],
        text_to_analysis,
        check_names=False,
    )

    # English language checks
    en_job = async_job_group.async_jobs[1]
    en_objects = artifacts.dataframes.get(en_job.async_job_meta.objects_analysed_url).dataframe
    # Only English language should be submitted
    expected_en = pd.DataFrame(
        {
            "object_id": [4, 5],
            "text": ["t4", "t5"],
            "language": ["en", "en"],
            "text_bytes_truncate": ["t4", "t5"],
            # Line numbers must be correct
            "aws_input_line_number": [0, 1],
        }
    )
    pd.testing.assert_frame_equal(en_objects, expected_en)
    text_to_analysis = text_documents_for_analysis.read_text_series(
        en_job.async_job_meta.input_url
    )
    pd.testing.assert_series_equal(
        expected_en["text_bytes_truncate"],
        text_to_analysis,
        check_names=False,
    )
