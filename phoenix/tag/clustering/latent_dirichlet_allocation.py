"""Implements Latent Dirichlet Allocation on data."""


import pandas as pd
from snowballstemmer import stemmer

from phoenix.tag.text_features_analyser import StemmedCountVectorizer, get_stopwords


class LatentDirichletAllocator:
    """LatentDirichletAllocator.

    Train LatentDirichletAllocation model to segment groups of objects' text (posts/tweets) and
    explain which words in the texts were most important in the segmentation.
    """

    def __init__(
        self, df: pd.DataFrame, text_column: str = "clean_text", grouping_column: str = ""
    ):
        self.text_column = text_column
        self.grouping_column = grouping_column
        if not grouping_column:
            self.dfs = {"all": df}
        else:
            self.dfs = {group: df for group, df in df.groupby(grouping_column)}

        self.vectorizers = self._train_vectorizer()

    def _train_vectorizer(self):
        """Train a CountVectorizer per group.

        Default stems words in arabic.
        """
        vectorizer_dict = {}

        for name, df in self.dfs.items():
            count_vectorizer = StemmedCountVectorizer(
                stemmer("arabic"), stop_words=get_stopwords()
            )
            word_matrix = count_vectorizer.fit_transform(df[self.text_column])

            vectorizer_dict[name] = {
                "count_vectorizer": count_vectorizer,
                "word_matrix": word_matrix,
            }

        return vectorizer_dict
