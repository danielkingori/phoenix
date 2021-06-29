"""Module to visualise posts by segmenting them through a Latent Dirichelet Allocation."""
from typing import Dict

import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer


class StemmedCountVectorizer(CountVectorizer):
    """StemmedCountVectorizer vectorizes stemmed words.

    Args:
        stemmer: a stemmer object with the stemWord() function. Implemented with
            snowballstemmer.stemmer
        **kwargs: additional keyword args used in the
            sklearn.feature_extraction.text.CountVectorizer init.
    """

    def __init__(self, stemmer, **kwargs):
        super(StemmedCountVectorizer, self).__init__(**kwargs)
        self.stemmer = stemmer

    def build_analyzer(self):
        """Overwrite of only the stemming of super's build_analyzer to use the added stemmer."""
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        return lambda doc: (self.stemmer.stemWord(w) for w in analyzer(doc))

    def get_most_common_words(self, count_vector_matrix: pd.arrays.SparseArray) -> Dict[str, int]:
        """Gets a dict of the most common words and their occurrence numbers.

        Args:
            count_vector_matrix (pd.arrays.SparseArray): sparse matrix of (n_samples,
            n_features), returned value of self.fit_transform()
        """
        count_dict = zip(self.get_feature_names(), count_vector_matrix.sum(axis=0).tolist()[0])
        count_dict = sorted(count_dict, key=lambda x: -x[1])

        return dict(count_dict)


def remove_links(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Removes links from a column."""
    df[col_name] = df[col_name].replace(to_replace=r"\S*https?:\S*", value="", regex=True)

    return df
