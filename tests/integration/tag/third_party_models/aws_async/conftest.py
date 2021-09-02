"""Conftest for the aws_async."""
import pandas as pd
import pytest

from phoenix.common import utils


@pytest.fixture
def aws_sentiment_objects():
    """Objects for AWS sentiment."""
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3, 4, 5, 6],
            "text": ["t1", "t2", "t3", "t4", "t5", "t6"],
            "language": ["ar", "ar", "ar", "en", "en", "ar_biz"],
        }
    )


@pytest.fixture
def aws_sentiment_ar_output_url():
    """Output URL for the test data of Arabic language.

    This data creates test data that matches what aws comprehend outputs.
    As defined here:
    https://docs.aws.amazon.com/comprehend/latest/dg/how-async.html#async-sentiment
    The line numbers are not ordered in the original and should not be in the test data.

    To recreate the tarball use this comment from the phoenix root
    directory:
    ```
    tar -czvf \
        tests/integration/tag/third_party_models/aws_async/test_data/ar_output.tar.gz \
        tests/integration/tag/third_party_models/aws_async/test_data/ar_output
    ```
    """
    file_path = utils.relative_path("./test_data/ar_output.tar.gz", __file__)
    return f"file:///{file_path}"


@pytest.fixture
def aws_sentiment_en_output_url():
    """Output URL for the test data of Arabic language.

    This data creates test data that matches what aws comprehend outputs.
    As defined here:
    https://docs.aws.amazon.com/comprehend/latest/dg/how-async.html#async-sentiment
    The line numbers are not ordered in the original and should not be in the test data.

    To recreate the tarball use this comment from the phoenix root
    directory:
    ```
    tar -czvf \
        tests/integration/tag/third_party_models/aws_async/test_data/en_output.tar.gz \
        tests/integration/tag/third_party_models/aws_async/test_data/en_output
    ```
    """
    file_path = utils.relative_path("./test_data/en_output.tar.gz", __file__)
    return f"file:///{file_path}"
