"""Implements Latent Dirichlet Allocation on data."""
from typing import List, Optional

import pickle

import arabic_reshaper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tentaclio
from bidi.algorithm import get_display
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.model_selection import GridSearchCV
from snowballstemmer import stemmer

from phoenix.common.artifacts import dataframes
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

    def save_plot(self, base_url: str, n_top_words: int = 15, title: str = "Wordcloud Groupings"):
        """Plot the LatentDirichletAllocation with the top defining words in the group.

        Args:
            base_url(str): base_url for saving the plot
            n_top_words(int): Number of words per group to show
            title(str): Title for the plots. Will be prepended with the groupings if any.
        """
        for vectorizer_name in self.vectorizers:
            if not self.vectorizers[vectorizer_name]["grid_search_model"]:
                raise KeyError("model not found, please train the model first.")

            if vectorizer_name == "all":
                DATASET_NAME = ""
            else:
                DATASET_NAME = vectorizer_name
            model = self.vectorizers[vectorizer_name]["grid_search_model"].best_estimator_
            feature_names = self.vectorizers[vectorizer_name][
                "count_vectorizer"
            ].get_feature_names()
            fig, axes = plt.subplots(2, 5, figsize=(30, 15), sharex="all")
            axes = axes.flatten()
            for topic_idx, topic in enumerate(model.components_):
                top_features_ind = topic.argsort()[: -n_top_words - 1 : -1]
                top_features = [
                    get_display(arabic_reshaper.reshape(feature_names[i]))
                    for i in top_features_ind
                ]
                weights = topic[top_features_ind]

                ax = axes[topic_idx]
                ax.barh(top_features, weights, height=0.7)
                ax.set_title(f"Cloud {topic_idx + 1}", fontdict={"fontsize": 30})
                ax.invert_yaxis()
                ax.tick_params(axis="both", which="major", labelsize=20)
                for i in "top right left".split():
                    ax.spines[i].set_visible(False)
                fig.suptitle(f"{DATASET_NAME} {title}", fontsize=40)

            plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
            with tentaclio.open(f"{base_url}{DATASET_NAME}_lda_wordcloud.png", mode="wb") as w:
                plt.savefig(w)
            plt.show()

    def tag_dataframe(self):
        """Write back LDA results to dataframes."""
        for vectorizer_name in self.vectorizers:
            if not self.vectorizers[vectorizer_name]["grid_search_model"]:
                raise KeyError("model not found, please train the model first.")
            model = self.vectorizers[vectorizer_name]["grid_search_model"].best_estimator_
            word_matrix = self.vectorizers[vectorizer_name]["word_matrix"]
            cloud_model_groups = model.transform(word_matrix)
            # Get index of most confident grouping (plus 1 as humans start from 1, not 0)
            self.dfs[vectorizer_name]["lda_name"] = vectorizer_name
            self.dfs[vectorizer_name]["lda_cloud"] = np.argmax(cloud_model_groups, axis=1) + 1

            self.dfs[vectorizer_name]["lda_cloud_confidence"] = np.amax(cloud_model_groups, axis=1)

    def persist(self, output_dir_url: str):
        """Persist dataframe tagged with LDA groupings."""
        for vectorizer_name in self.vectorizers:
            if "lda_name" not in self.dfs[vectorizer_name].columns:
                raise KeyError(
                    "Dataframe not tagged with LDA groupings, please run tag_dataframe."
                )

            url = dataframes.url(output_dir_url, f"{vectorizer_name}_latent_dirichlet_allocation")
            dataframes.persist(url, self.dfs[vectorizer_name])

    def persist_model(self, output_dir_url):
        """Persist model."""
        with tentaclio.open(f"{output_dir_url}latent_dirichlet_allocator_model.sav", "wb") as f:
            pickle.dump(self, f)
