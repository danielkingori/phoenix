"""Conftest for the aws_async."""
import pandas as pd
import pytest


@pytest.fixture
def aws_sentiment_objects():
    """Objects for AWS sentiment."""
    return pd.DataFrame(
        {
            "object_id": [1, 2, 3, 4, 5],
            "text": ["t1", "t2", "t3", "t4", "t5"],
            "language": ["ar", "ar", "ar", "en", "en"],
        }
    )
