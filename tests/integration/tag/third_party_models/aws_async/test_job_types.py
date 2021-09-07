"""Integration test for start of sentiment."""
from phoenix.tag.third_party_models.aws_async import job_types


def test_job_types_persist(tmpdir_url):
    """Test persist of job types."""
    async_jobs = []
    for job_id in ["1", "2", "3"]:
        async_jobs.append(
            job_types.AsyncJob(
                async_job_meta=job_types.AsyncJobMeta(
                    job_name="job_name",
                    artifacts_base="artifacts_base",
                    language_code="language_code",
                    input_url="input_url",
                    output_url="output_url",
                    objects_analysed_url="objects_analysed_url",
                ),
                aws_started_job=job_types.AWSStartedJob(
                    job_id=job_id,
                    job_arn="job_arn",
                    job_status=job_types.JOB_STATUS_SUBMITTED,
                ),
            )
        )

    async_job_group = job_types.AsyncJobGroup(
        async_job_group_meta=job_types.AsyncJobGroupMeta(
            analysis_type="analysis_type",
            group_job_id="group_job_id",
            artifacts_base="artifacts_base",
        ),
        async_jobs=async_jobs,
    )

    async_job_group_url = f"{tmpdir_url}/async_job_group.json"

    _ = job_types.persist_json(async_job_group_url, async_job_group)
    async_job_group_gotten = job_types.get_json(async_job_group_url)

    assert job_types.are_jobs_equal(async_job_group, async_job_group_gotten)
