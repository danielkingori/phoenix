"""Tension classifier."""
from typing import List

import pickle

import tentaclio
from sklearn.base import ClassifierMixin

from phoenix.tag.text_features_analyser import StemmedCountVectorizer


class TensionClassifier:
    """The TensionClassifier is a class which houses a classifier for tensions.

    Args:
        classifier: The sklearn classifier used to classify tensions
    """

    def __init__(self, classifier: ClassifierMixin):
        self.classifier = classifier

    def persist_model(self, output_dir_url):
        """Persist model."""
        with tentaclio.open(f"{output_dir_url}tension_classifier_model.sav", "wb") as f:
            pickle.dump(self, f)


class CountVectorizerTensionClassifier(TensionClassifier):
    """Uses a count vectorizer to classify tensions.

    Args:
        count_vectorizer: (StemmedCountVectorizer) count_vectorizer which has been fit
        classifier: The sklearn classifier used to classify tensions
        class_labels: List[str]: list of tension names for the classifier's classes
    """

    def __init__(
        self,
        count_vectorizer: StemmedCountVectorizer,
        classifier: ClassifierMixin,
        class_labels: List[str],
    ):

        super().__init__(classifier)
        self.count_vectorizer = count_vectorizer
        self.class_labels = class_labels

    def persist_model(self, output_dir_url):
        """Persist model."""
        with tentaclio.open(
                f"{output_dir_url}count_vectorizer_tension_classifier_model.sav", "wb"
        ) as f:
            pickle.dump(self, f)
