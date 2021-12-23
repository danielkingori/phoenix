"""Unit tests for features bug. Is in its own file ONLY because the rest of tfa tests take 5+s.

Has differing outcomes when run multiple times, but only the full sized
parallel_vs_non_parallel. You'll need to run this suite multiple (+- 20) times to reliably get
the differing outcomes.
"""
from typing import List

import pandas as pd
import pytest
from snowballstemmer import stemmer

from phoenix.tag import text_features_analyser as tfa


TEST_REPEAT = 2000

ENGLISH_STEMMER_PARAMS = {
    "en": {
        "stemmer": stemmer("english"),
        "stop_words": tfa.get_stopwords(),
        "strip_accents": "unicode",
        "encoding": "utf-8",
        "token_pattern": r"#?\b\w\w+\b",
    },
}


@pytest.fixture
def unparallelised_tfa():
    """Unparallelised tfa. Does stemming for English."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ENGLISH_STEMMER_PARAMS.keys()),
        default_params=ENGLISH_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=False,
    )


@pytest.fixture
def parallelised_tfa():
    """Parallelised tfa. Does stemming for English."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ENGLISH_STEMMER_PARAMS.keys()),
        default_params=ENGLISH_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=True,
    )


@pytest.fixture
def input_df() -> pd.DataFrame:
    """Input dataframe of English messages."""
    input_df = pd.DataFrame(
        data={
            "message": [
                "Such an analysis can reveal features",
                "that are not easily visible",
                "form the variations in the individual genes",
                "and can lead to a picture of expression that is more biologically",
                "antidisestablishmentarianism",
                "transparent and accessible to interpretation",
            ],
            "language": ["en"] * 6,
        }
    )
    return input_df


@pytest.fixture
def expected_processed_features() -> List[List[str]]:
    """Expected features after processing."""
    features = [
        [
            "analysi",
            "reveal",
            "featur",
            "analysi reveal",
            "reveal featur",
            "analysi reveal featur",
        ],
        ["easili", "visibl", "easili visibl"],
        [
            "form",
            "variat",
            "individu",
            "gene",
            "form variat",
            "variat individu",
            "individu gene",
            "form variat individu",
            "variat individu gene",
        ],
        [
            "lead",
            "pictur",
            "express",
            "biolog",
            "lead pictur",
            "pictur express",
            "express biolog",
            "lead pictur express",
            "pictur express biolog",
        ],
        [
            "antidisestablishmentarian",
        ],
        [
            "transpar",
            "access",
            "interpret",
            "transpar access",
            "access interpret",
            "transpar access interpret",
        ],
    ]
    return features


@pytest.mark.parametrize("execution_number", range(TEST_REPEAT))
def test_TextFeaturesAnalyser_features_unparallelised(
    execution_number, unparallelised_tfa, input_df, expected_processed_features
):
    """Test that the features for a certain row don't skip to another row.

    This is the control to see if unparallelised tfa's always output the same output.  40/40 times
    it passed the test.

    NOTE: this also worked with two separate instances of unparallelised_tfa,
    before it was a fixture
    """
    output_processed_features_df = unparallelised_tfa.features(input_df)
    output_processed_features_series = output_processed_features_df.iloc[:, 0]

    assert isinstance(output_processed_features_series, pd.Series)

    for output, expected in zip(output_processed_features_series, expected_processed_features):
        assert output == expected


@pytest.mark.parametrize("execution_number", range(TEST_REPEAT))
def test_TextFeaturesAnalyser_features_parallel(
    execution_number, parallelised_tfa, input_df, expected_processed_features
):
    """Test that the features for a certain row don't skip to another row.

    This is the control to see if parallelised tfa's always output the same output.
    it passed the test.
    12/40 times it passed the test.

    Note: before refactoring to a fixture we instantiated the tfa in the test itself,
    it passed the test 29/40 times.
    """
    output_processed_features_df = parallelised_tfa.features(input_df)
    output_processed_features_series = output_processed_features_df.iloc[:, 0]

    assert isinstance(output_processed_features_series, pd.Series)

    for output, expected in zip(output_processed_features_series, expected_processed_features):
        assert output == expected
