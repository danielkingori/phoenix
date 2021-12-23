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


TEST_REPEAT_COUNT = 100

ARABIC_STEMMER_PARAMS = {
    "ar": {
        "stemmer": stemmer("arabic"),
        "stop_words": tfa.get_stopwords(),
        "strip_accents": "unicode",
        "encoding": "utf-8",
        "token_pattern": r"#?\b\w\w+\b",
    },
}


@pytest.fixture
def unparallelised_tfa():
    """Unparallelised tfa. Does stemming for Arabic."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ARABIC_STEMMER_PARAMS.keys()),
        default_params=ARABIC_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=False,
    )


@pytest.fixture
def parallelised_tfa():
    """Parallelised tfa. Does stemming for Arabic."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ARABIC_STEMMER_PARAMS.keys()),
        default_params=ARABIC_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=True,
    )


@pytest.fixture
def input_df() -> pd.DataFrame:
    """Input dataframe of Arabic messages."""
    input_df = pd.DataFrame(
        data={
            "message": [
                #  "#الحدث_اليمني",
                #  "المحلل السياسي",
                #  "هيئة الأركان العامة",
                #  "#العربية",
                "وكيل وزارة الشؤون القانونية وحقوق الإنسان",
                "اتفاق_ستوكهولم",
                "رئيس الوزراء اليمني معين عبدالملك",
            ],
            "language": ["ar"] * 3,
        }
    )
    return input_df


@pytest.fixture
def expected_processed_features() -> List[List[str]]:
    """Expected features after processing."""
    features = [
        #  ["#الحدث_اليم"],
        #  ["محلل", "سياس", "محلل سياس"],
        #  ["هي", "ارك", "عام", "هي ارك", "ارك عام", "هي ارك عام"],
        #  ["#العرب"],
        [
            "كيل",
            "زار",
            "الشو",
            "قانون",
            "حقوق",
            "انس",
            "كيل زار",
            "زار الشو",
            "الشو قانون",
            "قانون حقوق",
            "حقوق انس",
            "كيل زار الشو",
            "زار الشو قانون",
            "الشو قانون حقوق",
            "قانون حقوق انس",
        ],
        ["اتفاق_ستوكهولم"],
        [
            "رييس",
            "وزراء",
            "يمن",
            "معين",
            "عبدالمل",
            "رييس وزراء",
            "وزراء يمن",
            "يمن معين",
            "معين عبدالمل",
            "رييس وزراء يمن",
            "وزراء يمن معين",
            "يمن معين عبدالمل",
        ],
    ]
    return features


@pytest.mark.parametrize("execution_number", range(TEST_REPEAT_COUNT))
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


@pytest.mark.parametrize("execution_number", range(TEST_REPEAT_COUNT))
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


def test_TextFeaturesAnalyser_features_skip_rows_parallel_vs_non_parallel_small(
    unparallelised_tfa, parallelised_tfa
):
    """Test that the features for a certain row don't skip to another row.
    This is the test to see if the unparallelised tfa has a different output to the parallelised
    tfa for a small subset. It contains only the row (index 3) that changes the most in the
    large test.
    This one passes, 40/40 times.
    """
    input_df = pd.DataFrame(
        data={
            "message": [
                "وكيل وزارة الشؤون القانونية وحقوق الإنسان",
            ],
            "language": ["ar"] * 1,
        }
    )

    parallelised_df = input_df.copy()
    non_parallelised_df = input_df.copy()

    parallelised_df["processed_features"] = parallelised_tfa.features(parallelised_df)
    non_parallelised_df["processed_features"] = unparallelised_tfa.features(non_parallelised_df)

    pd.testing.assert_frame_equal(parallelised_df, non_parallelised_df)