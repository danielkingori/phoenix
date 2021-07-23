"""Implements Latent Dirichlet Allocation on data."""
from typing import List, Optional

import pandas as pd
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.model_selection import GridSearchCV
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

    def train(
        self,
        n_components_list: Optional[List[int]] = None,
        max_iter_list: Optional[List[int]] = None,
    ):
        """Train the Latent Dirichlet Allocation model.

        Args:
            n_components_list(List[int]): list of number of components to try when searching for
            the best model.
            max_iter_list(List[int]): list of maximum iterations to try when searching for the
            best model.
        """
        n_components = n_components_list if n_components_list else [10, 20, 30, 40]
        max_iter = max_iter_list if max_iter_list else [10, 20, 40]
        search_params = {"n_components": n_components, "max_iter": max_iter}

        for vectorizer_name in self.vectorizers:
            model = GridSearchCV(LatentDirichletAllocation(), cv=None, param_grid=search_params)
            model.fit(self.vectorizers[vectorizer_name]["word_matrix"])
            self.vectorizers[vectorizer_name]["grid_search_model"] = model
