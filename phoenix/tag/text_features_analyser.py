"""Text feature analyser."""
from typing import Dict, List

import dask.dataframe as dd
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


class TextFeaturesAnalyser:
    """TextFeaturesAnalyser.

    Create a dictionary of analysers for each language and each
    ngram_ranges.
    """

    dict_countvectorizers: Dict = {}
    dict_analyser: Dict = {}

    def __init__(self, languages, ngram_ranges, default_params):
        """Init the text features Analyser."""
        for lang in languages:
            lang_default_params = {}
            if lang in default_params:
                lang_default_params = default_params[lang]
            countvectorizers = list(
                map(
                    lambda ngram_range: CountVectorizer(
                        **lang_default_params, ngram_range=ngram_range
                    ),
                    ngram_ranges,
                )
            )
            self.column_return_count = len(countvectorizers)
            self.dict_countvectorizers[lang] = countvectorizers
            self.dict_analyser[lang] = self._create_analyser(countvectorizers)

    def _build_meta_return(self):
        """Build the meta return."""
        return [(i, "object") for i in range(self.column_return_count)]

    def _create_analyser(self, countvectorizers: List):
        # cast to a list is needed otherwise will not
        # be able to analyse more then one row.
        return list(map(lambda obj: obj.build_analyzer(), countvectorizers))

    def features(self, df: pd.DataFrame, message_key: str = "message"):
        """Build feature grams."""
        self.message_key = message_key
        ddf = dd.from_pandas(
            df, npartitions=30
        )  # Should have npartitions configured in envirnment
        return ddf.apply(self.feature_apply, axis=1, meta=self._build_meta_return()).compute()

    def feature_apply(self, row):
        """Get features for row."""
        message = row[self.message_key]
        lang = row["language"]
        if lang in self.dict_analyser:
            analysers = self.dict_analyser[lang]
            return pd.Series(map(lambda analyser_fn: analyser_fn(message), analysers))
        raise ValueError(f"Language {lang} is not supported")


def create():
    """Create the TextFeaturesAnalyser."""
    default_params = {
        "ar": {"strip_accents": "unicode"},
        "ar_izi": {"strip_accents": "unicode"},
        "en": {"strip_accents": "ascii"},
    }

    return TextFeaturesAnalyser(
        languages=list(default_params.keys()),
        default_params=default_params,
        ngram_ranges=[(1, 3)],
    )


def combine_ngrams(df: pd.DataFrame):
    """Combine the ngrams columns."""
    return df.apply(lambda x: np.concatenate(x), axis=1)


def ngram_count(df: pd.DataFrame):
    """Combine the ngrams columns."""
    return df.applymap(_ngram_count_apply).squeeze()


def _ngram_count_apply(element_list):
    return dict(zip(*np.unique(element_list, return_counts=True)))
