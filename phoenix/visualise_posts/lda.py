"""Module to visualise posts by segmenting them through a Latent Dirichelet Allocation."""
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


def remove_links(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Removes links from a column."""
    df[col_name] = df[col_name].replace(to_replace=r"\S*https?:\S*", value="", regex=True)

    return df
