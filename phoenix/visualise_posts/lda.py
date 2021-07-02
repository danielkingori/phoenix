"""Module to visualise posts by segmenting them through a Latent Dirichelet Allocation."""
from typing import Dict, List

import arabic_reshaper
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from bidi.algorithm import get_display
from nltk.corpus import stopwords
from scipy.sparse.csr import csr_matrix
from sklearn.decomposition._lda import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer

from phoenix.common.artifacts import dataframes


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
        count_dict = sorted(count_dict, key=lambda x: -x[1])  # type: ignore

        return dict(count_dict)

    def plot_most_common_words(self, count_vector_matrix: pd.arrays.SparseArray, n: int) -> None:
        """Plot common words in the dataset in a bar graph."""
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


def remove_links(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """Removes links from a column."""
    df[col_name] = df[col_name].replace(to_replace=r"\S*https?:\S*", value="", regex=True)

    return df


def get_stopwords() -> List[str]:
    """Gets stopwords for both arabic and english."""
    stopwords_list = stopwords.words("arabic")
    stopwords_list.extend(stopwords.words("english"))

    return stopwords_list


def save_plot_top_lda_words(
    model: LatentDirichletAllocation, feature_names: List, n_top_words: int, title: str, f
) -> None:
    """Plots the top words in each topic in the LDA model."""
    fig, axes = plt.subplots(2, 5, figsize=(30, 15), sharex=True)
    axes = axes.flatten()
    for topic_idx, topic in enumerate(model.components_):
        top_features_ind = topic.argsort()[: -n_top_words - 1 : -1]
        top_features = [
            get_display(arabic_reshaper.reshape(feature_names[i])) for i in top_features_ind
        ]
        weights = topic[top_features_ind]

        ax = axes[topic_idx]
        ax.barh(top_features, weights, height=0.7)
        ax.set_title(f"Cloud {topic_idx +1}", fontdict={"fontsize": 30})
        ax.invert_yaxis()
        ax.tick_params(axis="both", which="major", labelsize=20)
        for i in "top right left".split():
            ax.spines[i].set_visible(False)
        fig.suptitle(title, fontsize=40)

    plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
    plt.savefig(f)
    plt.show()


def write_cloud_results(
    df: pd.DataFrame, word_matrix: csr_matrix, model: LatentDirichletAllocation
) -> pd.DataFrame:
    """Write results of the LDA models back to the dataframe.

    Args:
        df: original dataframe used for fitting the CountVectorizer, must have same size and
            index as word_matrix
        word_matrix: matrix result from fitting the CountVectorizer, must have same size and index
            as df
        model: Trained LDA model

    Returns:
        pd.DataFrame: dataframe with additional columns lda_cloud, and lda_cloud_confidence
    """
    cloud_model_groups = model.transform(word_matrix)
    # Get index of most confident grouping (plus 1 as humans start from 1, not 0)
    df["lda_cloud"] = np.argmax(cloud_model_groups, axis=1) + 1

    df["lda_cloud_confidence"] = np.amax(cloud_model_groups, axis=1)

    return df


def persist(output_dir_url: str, tagged_df: pd.DataFrame):
    """Persist artifacts."""
    name = "lda"
    url = dataframes.url(output_dir_url, name)
    dataframes.persist(url, tagged_df)
