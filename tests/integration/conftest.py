"""Integration config."""
import re
import uuid

import boto3
import pytest


@pytest.fixture
def tmp_s3_dir(request):
    """Get a temporary s3 storage path for the function.

    Based on the tmp_path function:
    https://docs.pytest.org/en/stable/_modules/_pytest/tmpdir.html#tmp_path


    Will create a temporary folder like `<name of test function>/<UUID>/` where the
    test can persist files to. After the test is finished all files in that folder will
    be deleted.

    This implementation is not full proof and can leave temporary objects around.
    For instance if an integration test is interrupted, Ctrl-c. As such the s3
    storage bucket must have a lifecycle behaviour that will delete
    old objects. It is recommended to set this up in terraform.

    Here are the example CLI commands:
    `aws s3 mb s3://phoenix-integration-testing`
    ```
    aws s3api put-bucket-lifecycle-configuration  \
        --bucket phoenix-integration-testing  \
        --lifecycle-configuration file://lifecycle.xml
    ```
    where lifecycle.xml is a file that has something like:
    ```
    <LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Rule>
            <ID>ExpireAll</ID>
            <Expiration>
                <Days>1</Days>
            </Expiration>
            <Status>Enabled</Status>
        </Rule>
    </LifecycleConfiguration>
    ```

    Unfortunately the CLI commands do not work ??
    You can then do the lifecycle configuration via the s3 aws console.
    """
    s3_integration_bucket = "phoenix-integration-testing"
    temp_folder = uuid.uuid4()
    name = request.node.name
    name = re.sub(r"[\W]", "_", name)
    MAXVAL = 40
    name = name[:MAXVAL]
    tmp_dir = f"{name}/{temp_folder}/"
    yield f"s3://{s3_integration_bucket}/{tmp_dir}"

    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_integration_bucket)
    for key in bucket.objects.filter(Prefix=tmp_dir):
        key.delete()
