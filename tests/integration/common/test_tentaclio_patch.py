"""Test patch."""
import boto3
import pytest
import tentaclio
from tentaclio import urls

from phoenix import common  # noqa need to do the patch


@pytest.mark.auth
def test_content_type(tmp_s3_dir):
    """Test tentaclio patch Content type."""
    test_artefact_dir = tmp_s3_dir
    content_type = "text/html"
    extra_args = {"ContentType": content_type}
    url = f"{test_artefact_dir}/testing.html"
    with tentaclio.open(url, "w", conn_encrypt=True, upload_extra_args=extra_args) as fo:
        fo.write("hello")

    parsed_url = urls.URL(url)
    bucket = parsed_url.hostname or None
    key_name = parsed_url.path[1:] if parsed_url.path != "" else ""

    client = boto3.client("s3")
    head = client.head_object(Bucket=bucket, Key=key_name)
    assert head["ContentType"] == content_type
    tentaclio.remove(url)


@pytest.mark.auth
def test_content_type_not_set(tmp_s3_dir):
    """Test tentaclio patch works for default content type."""
    test_artefact_dir = tmp_s3_dir
    default_content_type = "binary/octet-stream"
    url = f"{test_artefact_dir}/testing.html"
    with tentaclio.open(url, "w", conn_encrypt=True) as fo:
        fo.write("hello")

    parsed_url = urls.URL(url)
    bucket = parsed_url.hostname or None
    key_name = parsed_url.path[1:] if parsed_url.path != "" else ""

    client = boto3.client("s3")
    head = client.head_object(Bucket=bucket, Key=key_name)
    assert head["ContentType"] == default_content_type
    tentaclio.remove(url)
