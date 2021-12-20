"""Unit tests for features bug. Is in its own file ONLY because the rest of tfa tests take 5+s.

Has differing outcomes when run multiple times, but only the full sized
parallel_vs_non_parallel. You'll need to run this suite multiple (+- 20) times to reliably get
the differing outcomes.
"""
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
def unparallelised_featuriser():
    """Unparallelised featuriser. Does stemming for Arabic."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ARABIC_STEMMER_PARAMS.keys()),
        default_params=ARABIC_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=False,
    )


@pytest.fixture
def parallelised_featuriser():
    """Parallelised featuriser. Does stemming for Arabic."""
    return tfa.TextFeaturesAnalyser(
        languages=list(ARABIC_STEMMER_PARAMS.keys()),
        default_params=ARABIC_STEMMER_PARAMS,
        ngram_ranges=[(1, 3)],
        use_ngrams=True,
        parallelisable=True,
    )


def test_TextFeaturesAnalyser_features_unparallelised(unparallelised_featuriser):
    """Test that the features for a certain row don't skip to another row.
    This is the control to see if unparallelised tfa's always output the same output.
    40/40 times it passed the test.
    NOTE: this also worked with two separate instances of unparallelised_featuriser, before it
    was a fixture
    """
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

    non_parallelised_df_1 = input_df.copy()
    non_parallelised_df_2 = input_df.copy()

    non_parallelised_df_1["processed_features"] = unparallelised_featuriser.features(
        non_parallelised_df_1
    )
    non_parallelised_df_2["processed_features"] = unparallelised_featuriser.features(
        non_parallelised_df_2
    )

    pd.testing.assert_frame_equal(non_parallelised_df_1, non_parallelised_df_2)


@pytest.mark.skip
def test_TextFeaturesAnalyser_features_skip_rows_parallel_vs_non_parallel(
    unparallelised_featuriser, parallelised_featuriser
):
    """Test that the features for a certain row don't skip to another row.
    This is the test to see if the unparallelised tfa has a different output to the parallelised
    tfa
    12/40 times it passed the test.

    Note: before refactoring to a fixture we instantiated the tfa in the test itself,
    it passed the test 29/40 times.
    """
    input_df = pd.DataFrame(
        data={
            "message": [
                "#الحدث_اليمني",
                "المحلل السياسي",
                "هيئة الأركان العامة",  # this one fails sometimes
                "#العربية",
                "وكيل وزارة الشؤون القانونية وحقوق الإنسان",  # this one fails most
                "اتفاق_ستوكهولم",
                "رئيس الوزراء اليمني معين عبدالملك",
            ],
            "language": ["ar"] * 7,
        }
    )

    parallelised_df = input_df.copy()
    non_parallelised_df = input_df.copy()

    parallelised_df["processed_features"] = parallelised_featuriser.features(parallelised_df)
    non_parallelised_df["processed_features"] = unparallelised_featuriser.features(
        non_parallelised_df
    )

    pd.testing.assert_frame_equal(parallelised_df, non_parallelised_df)


def test_TextFeaturesAnalyser_features_skip_rows_parallel_vs_non_parallel_small(
    unparallelised_featuriser, parallelised_featuriser
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

    parallelised_df["processed_features"] = parallelised_featuriser.features(parallelised_df)
    non_parallelised_df["processed_features"] = unparallelised_featuriser.features(
        non_parallelised_df
    )

    pd.testing.assert_frame_equal(parallelised_df, non_parallelised_df)
