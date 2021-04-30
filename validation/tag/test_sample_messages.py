"""Run the tagging on sample messages."""
import pandas as pd
import pytest

from phoenix.common import utils
from phoenix.tag import language, text_features_analyser


@pytest.fixture
def sample_messages_df():
    """Get the sample message df."""
    return pd.read_csv(utils.relative_path("./sample_messages.csv", __file__))


def test_sample_messages(sample_messages_df):
    """Run all tagging on the sample messages.

    This will write "./output_tagging.csv" which
    can then be checked by hand.
    """
    assert "message" in sample_messages_df

    df = sample_messages_df.copy()
    df[["language", "confidence"]] = language.execute(df["message"])
    tfa = text_features_analyser.create()
    df["features"] = tfa.features(df[["message", "language"]], "message")
    df.to_csv(utils.relative_path("./output_tagging.csv", __file__))
