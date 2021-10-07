"""Complete the sentiment analysis."""
from typing import List

import json
import tarfile

import pandas as pd
import tentaclio

from phoenix.common import artifacts, pd_utils
from phoenix.tag.third_party_models.aws_async import job_types


def complete_sentiment_analysis(
    async_job_group: job_types.AsyncJobGroup, job_infos: List[job_types.AWSDescribeJob]
) -> pd.DataFrame:
    """Complete the sentiment analysis."""
    objects = []
    for index, async_job in enumerate(async_job_group.async_jobs):
        job_info = get_job_info_for_async_job(async_job, job_infos)
        objects.append(complete_async_job(job_info, async_job))

    return pd.concat(objects, ignore_index=True)


def get_job_info_for_async_job(
    async_job: job_types.AsyncJob, job_infos: List[job_types.AWSDescribeJob]
) -> job_types.AWSDescribeJob:
    """Get the AWSDescribeJob for the AsyncJob."""
    for job_info in job_infos:
        if job_info.job_id == async_job.aws_started_job.job_id:
            return job_info
    raise RuntimeError(
        "Job infos and AsyncJobs do not match."
        " This is an expection that should not happen."
        " Please run `get_job_infos` again for for the AsyncJobGroup."
    )


def complete_async_job(
    job_info: job_types.AWSDescribeJob, async_job: job_types.AsyncJob
) -> pd.DataFrame:
    """Complete the sentiment analysis for an async job."""
    objects = artifacts.dataframes.get(async_job.async_job_meta.objects_analysed_url).dataframe
    aws_comprehend_output = aws_sentiment_data_from_output(job_info.output_url)
    objects = objects.set_index("aws_input_line_number")
    aws_comprehend_output = aws_comprehend_output.set_index("line")
    df = objects.merge(aws_comprehend_output, left_index=True, right_index=True)
    df.index.name = "aws_input_line_number"
    return df.reset_index()


def aws_sentiment_data_from_output(output_url) -> pd.DataFrame:
    """From the tar file create a dataframe."""
    all_file_dfs = []
    with tentaclio.open(output_url, "rb") as file_io:
        tar = tarfile.open(mode="r:gz", fileobj=file_io)
        for member in tar.getmembers():
            f = tar.extractfile(member)
            if f is not None:
                all_file_dfs.append(process_output_file(f))

    return pd.concat(all_file_dfs)


def process_output_file(fileobj) -> pd.DataFrame:
    """Process the comprehend output file."""
    json_objs = []
    while True:
        # Get next line from file
        line = fileobj.readline().decode()
        # if line is empty
        # end of file is reached
        if not line:
            break

        json_objs.append(json.loads(line))

    joined_lines = pd.DataFrame(json_objs)
    return normalise_output(joined_lines)


def normalise_output(joined_lines: pd.DataFrame) -> pd.DataFrame:
    """Normalise the comprehend output."""
    df = joined_lines.rename(pd_utils.camel_to_snake, axis="columns")
    df_nested = pd.json_normalize(df["sentiment_score"])
    df_nested = df_nested.rename(lambda x: f"language_sentiment_score_{x.lower()}", axis="columns")
    df = df.drop(columns=["file", "sentiment_score"])
    df = df.rename(columns={"sentiment": "language_sentiment"})
    return df.merge(df_nested, left_index=True, right_index=True)
