"""Text feature analyser.

For this to work you need to download the stopwords.
```
python -m nltk.downloader stopwords
```

The text feature analyser will do the analysis of text for
arabic, arabizi, and english.
"""
from typing import Any, Callable, Dict, List, Tuple

import functools
import itertools
import logging

import arabic_reshaper
import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from bidi.algorithm import get_display
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from snowballstemmer import stemmer

from phoenix.tag import kurdish


logger = logging.getLogger(__name__)


class StemmedCountVectorizer(CountVectorizer):
    """CountVectorizer with stemming.

    Extension of the CountVectorizer to include the stemming functionality.
    """

    def __init__(self, stemmer=None, stemmer_initialiser=None, **kwargs):
        super(StemmedCountVectorizer, self).__init__(**kwargs)
        self.stemmer = stemmer
        self.stemmer_initialiser = stemmer_initialiser

    def build_analyzer(self, use_ngrams=True):
        """Build the analyzer with the stemmer. Optionally can build analyzer without ngrams."""
        if not use_ngrams:
            self._word_ngrams = self._no_ngram_word_ngram_processor
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        # A defined `stemmer_initialiser` means that the stemmer will be initialised inside running
        # the analyzer. As such we are returning an analyzer function without the stemmer as it
        # will be passed directly to function at run time - this will be within a specific thread
        # if used in conjunction with panellisation, thus avoiding race condition bugs from sharing
        # a stemmer instance which is non-threadsafe across multiple threads.
        if self.stemmer_initialiser is not None:
            fn = functools.partial(stem_analyzer, analyzer)
            return fn
        elif self.stemmer is not None:
            fn = functools.partial(stem_analyzer, analyzer, self.stemmer)
            return fn
        else:
            return analyzer

    def _no_ngram_word_ngram_processor(self, tokens, stop_words=None):
        """Override of the CountVectorizer._word_ngrams which doesn't split text into ngrams.

        Args:
            tokens: list of tokens in text
            stop_words: list of str: stopwords

        This override is needed as the original _analyze function of the CountVectorizer only
        removes stopwords if a _word_ngram processor is available. By overriding this, we can get
        the full analysis pipeline of preprocessing, tokenization, and stop word removal follow the
        CountVectorizer method without ngram.
        """
        # handle stop words
        if stop_words is not None:
            tokens = [w for w in tokens if w not in stop_words]

        return tokens

    def get_most_common_words(self, count_vector_matrix: pd.arrays.SparseArray) -> Dict[str, int]:
        """Gets a dict of the most common words and their occurrence numbers.

        Args:
            count_vector_matrix (pd.arrays.SparseArray): sparse matrix of (n_samples,
            n_features), returned value of self.fit_transform()
        """
        count_dict = zip(self.get_feature_names(), count_vector_matrix.sum(axis=0).tolist()[0])
        count_dict = sorted(count_dict, key=lambda x: -x[1])  # type: ignore

        return dict(count_dict)

    def plot_most_common_words(self, count_vector_matrix: pd.arrays.SparseArray, n: int) -> None:
        """Plot common words in the dataset in a bar graph.

        Args:
            count_vector_matrix (pd.arrays.SparseArray): sparse matrix of (n_samples,
            n_features), returned value of self.fit_transform()
            n (int): number of most common words yo be plotted
        """
        wordcount_dict = self.get_most_common_words(count_vector_matrix)
        words = [get_display(arabic_reshaper.reshape(w)) for w in list(wordcount_dict)[0:n]]
        counts = [word_count for word_count in list(wordcount_dict.values())[0:n]]
        x_pos = np.arange(len(words))

        sns.barplot(x_pos, counts)
        plt.xticks(x_pos, words, rotation=80)
        plt.xlabel("words", fontsize=13)
        plt.ylabel("counts", fontsize=13)
        plt.title(f"{n} most common words", fontsize=15)
        plt.show()


# Cannot be a method on object when used by dash.
def stem_analyzer(analyzer, stemmer, doc):
    """Stem_analyzer."""
    li: List[str] = []
    if not doc:
        return li
    if not isinstance(doc, str):
        doc = str(doc)
    for token in filter(None, analyzer(doc)):  # type: ignore
        if token:
            words_list = [stemmer.stemWord(w) for w in token.split(" ") if w]
            li.append(" ".join(words_list))
    return li


class TextFeaturesAnalyser:
    """TextFeaturesAnalyser.

    Create a dictionary of analysers for each language and each
    ngram_ranges.
    """

    dict_countvectorizers: Dict
    dict_analyser: Dict

    def __init__(
        self,
        languages: List[str],
        ngram_ranges: List[Tuple[int, int]],
        use_ngrams: bool,
        default_params: Dict[str, Dict[str, Any]],
        parallelisable: bool,
    ):
        """Init the text features Analyser.

        Args:
            languages (List[str]): List of languages that the TextFeaturesAnalyser will create
                analysers for.
            ngram_ranges (List[Tuple[int,int]]): List of ngram ranges to create when analysing
                text.
            use_ngrams (bool): flag to use ngrams in analysis. If False the entire text will be
                analysed as a sentence.
            default_params (Dict[str, Dict[str, Any]]): default parameters to use when
                analysing. The first key contains language. The sub-key contains processing
                types, such as `stemmer`, `stop_words`, or `preprocessor`.
            parallelisable (bool): flag to use dask. if False, uses pandas.apply() when creating
                features. Needed for when parts of the function can not be pickled and thus
                can't be used in combination with dask.
        """
        self.dict_countvectorizers = {}
        self.dict_analyser = {}
        self.dict_stemmer_initialiser = {
            lang: d["stemmer_initialiser"]
            for lang, d in default_params.items()
            if "stemmer_initialiser" in d
        }

        for lang in languages:
            lang_default_params = {}
            if lang in default_params:
                lang_default_params = default_params[lang]
            countvectorizers = [
                StemmedCountVectorizer(**lang_default_params, ngram_range=ngram_range)
                for ngram_range in ngram_ranges
            ]
            self.dict_countvectorizers[lang] = countvectorizers
            self.dict_analyser[lang] = self._create_analysers(countvectorizers, use_ngrams)
        self.column_return_count = len(ngram_ranges)
        self.parallelisable = parallelisable

    def _build_meta_return(self):
        """Build the meta return."""
        return [(i, "object") for i in range(self.column_return_count)]

    def _create_analysers(self, countvectorizers: List, use_ngrams: bool):
        # cast to a list is needed otherwise will not
        # be able to analyse more then one row.
        return [countvectorizer.build_analyzer(use_ngrams) for countvectorizer in countvectorizers]

    def features(self, df: pd.DataFrame, message_key: str = "message") -> pd.Series:
        """Build feature grams.

        Args:
            df (pd.DataFrame): dataframe with minimum the `message_key` column (default "message")
                , and the "language" column
            message_key (str): name of column that contains the string to be feature-ized.
        """
        self.message_key = message_key
        if self.parallelisable:
            ddf = dd.from_pandas(
                df, npartitions=30
            )  # Should have npartitions configured in envirnment
            # When using dask have to create a partial rather then a method on a class
            for p in range(ddf.npartitions):
                logger.info(f"Partition Index={p}, Number of Rows={len(ddf.get_partition(p))}")

            return (
                ddf.map_partitions(
                    block_feature_apply,
                    self.dict_analyser,
                    self.dict_stemmer_initialiser,
                    self.message_key,
                    meta=self._build_meta_return(),
                )
                .compute()
                .iloc[:, 0]
            )
        else:
            if len(df) == 0:
                # An apply on an empty df returns the df itself, which has two columns. The
                # expected return is a single series.
                return pd.Series()
            return block_feature_apply(
                df, self.dict_analyser, self.dict_stemmer_initialiser, self.message_key
            ).iloc[:, 0]


def block_feature_apply(
    partition_df: pd.DataFrame,
    dict_analyser: Dict[str, List[Callable]],
    dict_stemmer_initialiser: Dict[str, Tuple[Callable, Tuple]],
    text_key: str,
) -> pd.DataFrame:
    """Produce features for df.

    Expected usage of function is for `dask`s `map_partitions`.

    Arguments:
        partition_df: Input dataframe for which to compute features for.
        dict_analyser: a dictionary of analyser for each language.
            eg.
                {"en": fn, "ar": fn, ...}
        dict_stemmer_initialiser: A dictionary, language string as key, and value is tuple of
            callable and args, together which initialise the stemmer for that language.  Reasoning
            being that the stemmer we use is not thread safe, so we need to instantiate a stemmer
            for each thread.
            eg.
               {"en": (stemmer_init_fn, args), "ar": (stemmer_init_fn, args), ...}
        text_key: The key of the column for the text.

    Returns:
        A list of features:
            pd.Series(["f1", "f2", ...])
    """
    # dict_stemmer = {
    #    lang: initialiser[0](*initialiser[1])
    #    for lang, initialiser in dict_stemmer_initialiser.items()
    # }

    dict_stemmer = {}
    for lang, initialiser in dict_stemmer_initialiser.items():
        dict_stemmer[lang] = initialiser[0](*initialiser[1])

    fn = functools.partial(feature_apply, dict_analyser, dict_stemmer, text_key)
    return partition_df.apply(fn, axis=1)


def feature_apply(
    dict_analyser: Dict[str, List[Callable]],
    dict_stemmer: Dict[str, Any],
    text_key: str,
    row: pd.Series,
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
    if lang in dict_analyser and lang in dict_stemmer:
        analysers = dict_analyser[lang]
        return pd.Series([analyser_fn(dict_stemmer[lang], message) for analyser_fn in analysers])
    elif lang in dict_analyser and lang not in dict_stemmer:
        analysers = dict_analyser[lang]
        return pd.Series([analyser_fn(message) for analyser_fn in analysers])
    keys = list(dict_analyser.keys())
    raise ValueError(f"Language {lang} is not supported. Supported keys: {keys}")


def create(
    ngram_ranges: List[Tuple[int, int]] = [(1, 3)], use_ngrams=True, parallelisable: bool = False
):
    """Create the TextFeaturesAnalyser."""
    # Configuration is hard coded this can be changed at some point.
    # token_pattern is the default token pattern with the addition of a optional # before a word
    default_params: Dict[str, Dict[str, Any]] = {
        "ar": {
            "stemmer_initialiser": (stemmer, ("arabic",)),
            "stop_words": get_stopwords(),
            "strip_accents": "unicode",
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "ar_izi": {"strip_accents": "unicode"},
        "en": {
            "stemmer_initialiser": (stemmer, ("english",)),
            "stop_words": get_stopwords(),
            "strip_accents": "ascii",
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "ckb": {
            "stemmer_initialiser": (kurdish.SoraniStemmer, ()),
            "stop_words": kurdish.sorani_stopwords,
            "preprocessor": kurdish.sorani_preprocess,
            "tokenizer": kurdish.sorani_tokenize,
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "ku": {
            "stemmer_initialiser": (kurdish.KurmanjiStemmer, ()),
            "stop_words": kurdish.kurmanji_stopwords,
            "preprocessor": kurdish.kurmanji_preprocess,
            "tokenizer": kurdish.kurmanji_tokenize,
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "fr": {
            "stemmer_initialiser": (stemmer, ("french",)),
            "stop_words": stopwords.words("french"),
            "strip_accents": "ascii",
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "swa": {
            "strip_accents": "ascii",
            "encoding": "utf-8",
            "token_pattern": r"#?\b\w\w+\b",
        },
        "und": {"strip_accents": "unicode"},
    }

    # the processors for kurdish use cyhunspell, which isn't picklable. This means dask cannot
    # parallelize them.
    if parallelisable:
        del default_params["ckb"]
        del default_params["ku"]

    return TextFeaturesAnalyser(
        languages=list(default_params.keys()),
        default_params=default_params,
        ngram_ranges=ngram_ranges,
        use_ngrams=use_ngrams,
        parallelisable=parallelisable,
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


def get_stopwords() -> List[str]:
    """Gets stopwords for both arabic and english as languages can be mixed within objects.

    The stopwords of the languages don't overlap so there is no danger of removing arabic
    stopwords which are non-stopwords in english or vice versa.
    """
    stopwords_list = stopwords.words("arabic")
    stopwords_list.extend(stopwords.words("english"))

    return stopwords_list
