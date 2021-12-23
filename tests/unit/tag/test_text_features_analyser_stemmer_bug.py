"""Unit tests specifically for tracking down the intermittent bug in `tfa.features`.

The `feature` function when ran using `dask` (i.e. parallelised) intermittently gives inconsistent
results. The bug was tracked down as be caused by a combination of `snowball` stemmer instances not
being thread-safe combined with running using `dask` parallelised.

The solution is to instantiate a stemmer instance _per thread_ and not share an instance across
threads.

One can use the `execution_number` range value to vary the number of time a test is repeated. It
may be required to up the number to 1000 to get failures, depending on the machine you are running
on.
"""
from typing import List

import pandas as pd
import pytest
from snowballstemmer import stemmer

from phoenix.tag import text_features_analyser as tfa


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
                "#الحدث_اليمني",
                "المحلل السياسي",
                "هيئة الأركان العامة",
                "#العربية",
                "وكيل وزارة الشؤون القانونية وحقوق الإنسان",
                "اتفاق_ستوكهولم",
                "رئيس الوزراء اليمني معين عبدالملك",
            ],
            "language": ["ar"] * 7,
        }
    )
    return input_df


@pytest.fixture
def expected_processed_features() -> List[List[str]]:
    """Expected features after processing."""
    features = [
        ["#الحدث_اليم"],
        ["محلل", "سياس", "محلل سياس"],
        ["هي", "ارك", "عام", "هي ارك", "ارك عام", "هي ارك عام"],
        ["#العرب"],
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


@pytest.mark.parametrize("execution_number", range(100))
def test_TextFeaturesAnalyser_features_unparallelised(
    execution_number, unparallelised_tfa, input_df, expected_processed_features
):
    """Test that the features for a certain row don't skip to another row.

    This is the control to see if unparallelised tfa's always output the same output.  40/40 times
    it passed the test.

    NOTE: this also worked with two separate instances of unparallelised_tfa,
    before it was a fixture
    """
    output_processed_features = unparallelised_tfa.features(input_df)

    assert isinstance(output_processed_features, pd.Series)

    for output, expected in zip(output_processed_features, expected_processed_features):
        assert output == expected


@pytest.mark.parametrize("execution_number", range(100))
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
    output_processed_features = parallelised_tfa.features(input_df)

    assert isinstance(output_processed_features, pd.Series)

    for output, expected in zip(output_processed_features, expected_processed_features):
        assert output == expected
