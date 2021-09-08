"""Tension classifier."""
from typing import List, Optional

import logging
import pickle

import pandas as pd
import tentaclio
from sklearn.base import ClassifierMixin

from phoenix.tag.text_features_analyser import StemmedCountVectorizer


logger = logging.getLogger()


class TensionClassifier:
    """The TensionClassifier is a class which houses a classifier for tensions.

    Args:
        classifier: The sklearn classifier used to classify tensions
    """

    def __init__(self, classifier: ClassifierMixin):
        self.classifier = classifier

    @staticmethod
    def get_model_name(suffix: Optional[str] = None) -> str:
        """Get the model's name. Used for saving models."""
        model_name = "tension_classifier_model"
        if suffix:
            model_name = f"{model_name}_{suffix}"
        return model_name

    def persist_model(self, output_dir_url):
        """Persist model."""
        with tentaclio.open(f"{output_dir_url}{self.get_model_name()}.pickle", "wb") as f:
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

    @staticmethod
    def get_model_name(suffix: Optional[str] = None) -> str:
        """Get the model's name. Used for saving models."""
        model_name = "count_vectorizer_tension_classifier_model"
        if suffix:
            model_name = f"{model_name}_{suffix}"
        return model_name

    def predict(self, df: pd.DataFrame, clean_text_col: str) -> pd.DataFrame:
        """Predict and tag a dataframe based on its text column."""
        logger.info("Starting word vectorization")
        word_vectors = self.count_vectorizer.transform(df[clean_text_col])

        logger.info("Starting classification")
        classifications = self.classifier.predict(word_vectors)
        logger.info(f"Writing the following classifications back: {self.class_labels}")
        df[self.class_labels] = classifications
        return df
