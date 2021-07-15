"""Text feature analyser.

For this to work you need to download the stopwords.
```
python -m nltk.downloader stopwords
```

The text feature analyser will do the analysis of text for
arabic, arabizi, and english.
"""
from typing import Callable, Dict, List

import functools
import itertools

import dask.dataframe as dd
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from snowballstemmer import stemmer


class StemmedCountVectorizer(CountVectorizer):
    """CountVectorizer with stemming.

    Extension of the CountVectorizer to include the stemming functionality.
    """

    def __init__(self, stemmer=None, **kwargs):
        super(StemmedCountVectorizer, self).__init__(**kwargs)
        if stemmer:
            self.stemmer = stemmer
        else:
            self.stemmer = None

    def build_analyzer(self):
        """Build the analyzer with the stemmer."""
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        if not self.stemmer:
            return analyzer

        fn = functools.partial(stem_analyzer, self.stemmer, analyzer)
        return fn

    def get_most_common_words(self, count_vector_matrix: pd.arrays.SparseArray) -> Dict[str, int]:
        """Gets a dict of the most common words and their occurrence numbers.

        Args:
            count_vector_matrix (pd.arrays.SparseArray): sparse matrix of (n_samples,
            n_features), returned value of self.fit_transform()
        """
        count_dict = zip(self.get_feature_names(), count_vector_matrix.sum(axis=0).tolist()[0])
        count_dict = sorted(count_dict, key=lambda x: -x[1])  # type: ignore

        return dict(count_dict)


# Cannot be a method on object when used by dash.
def stem_analyzer(stemmer, analyzer, doc):
    """Stem_analyzer."""
    li = []
    for w in filter(None, analyzer(doc)):  # type: ignore
        if w:
            try:
                li.append(stemmer.stemWord(w))
            except IndexError:
                # Index Errors causes problems
                li.append(w)
    return li


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
                    lambda ngram_range: StemmedCountVectorizer(
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
        # When using dask have to create a partial rather then a method on a class
        fn = functools.partial(feature_apply, self.dict_analyser, self.message_key)
        return ddf.apply(fn, axis=1, meta=self._build_meta_return()).compute()


def feature_apply(
    dict_analyser: Dict[str, List[Callable]], text_key: str, row: pd.Series
) -> pd.Series:
    """Get features for row.

    Arguments:
        dict_analyser: a dictionary of analyser for each language.
            eg.
                {"en": fn, "ar": fn, ...}
        text_key: The key of the column for the text.
        row: the row from the dataframe as a Series.

    Returns:
        A list of features:
            pd.Series(["f1", "f2", ...])
    """
    message = row[text_key]
    lang = row["language"]
    if lang in dict_analyser:
        analysers = dict_analyser[lang]
        return pd.Series(map(lambda analyser_fn: analyser_fn(message), analysers))
    keys = list(dict_analyser.keys())
    raise ValueError(f"Language {lang} is not supported. Supported keys: {keys}")


def create():
    """Create the TextFeaturesAnalyser."""
    # Configuration is hard coded this can be changed at some point.
    default_params = {
        "ar": {
            "stemmer": stemmer("arabic"),
            "stop_words": stopwords.words("arabic"),
            "strip_accents": "unicode",
            "encoding": "utf-8",
        },
        "ar_izi": {"strip_accents": "unicode"},
        "en": {
            "stemmer": stemmer("english"),
            "stop_words": stopwords.words("english"),
            "strip_accents": "ascii",
            "encoding": "utf-8",
        },
    }

    return TextFeaturesAnalyser(
        languages=list(default_params.keys()),
        default_params=default_params,
        ngram_ranges=[(1, 3)],
    )


def combine_ngrams(df: pd.DataFrame):
    """Combine the ngrams columns.

    This is a dark pandas magic function and I can't remember what it does.
    """
    return df.apply(lambda x: np.concatenate(x), axis=1)


def ngram_count(df: pd.DataFrame):
    """Combine the ngrams columns.

    This is a dark pandas magic function and I can't remember what it does.
    """
    return df.applymap(_ngram_count_apply).squeeze()


def _ngram_count_apply(element_list):
    return dict(zip(*np.unique(element_list, return_counts=True)))


def features_index(df: pd.DataFrame):
    """Combine the ngrams_count columns into features index.

    Returns a tuple of all the features:
    (ngram_key, feature, count)
    """
    return df.apply(_features_index, axis=1)


def _features_index(row):
    return list(
        itertools.chain.from_iterable(
            [[(key,) + v for v in list(value.items())] for key, value in row.items()]
        )
    )
