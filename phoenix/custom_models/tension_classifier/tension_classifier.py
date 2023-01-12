"""Tension classifier."""
from typing import Optional

import logging
import pickle

import tentaclio
from sklearn.base import ClassifierMixin

from phoenix.common import artifacts


logger = logging.getLogger()


class TensionClassifier:
    """The TensionClassifier is a class which houses a classifier for tensions.

    Args:
        classifier: The sklearn classifier used to classify tensions
    """

    def __init__(self, classifier: Optional[ClassifierMixin] = None):
        self.classifier: ClassifierMixin
        if classifier:
            self.classifier = classifier

    @staticmethod
    def get_model_name(suffix: Optional[str] = None) -> str:
        """Get the model's name. Used for saving models."""
        model_name = "tension_classifier_model"
        if suffix:
            model_name = f"{model_name}_{suffix}"
        return model_name

    @staticmethod
    def get_model(model_url):
        """Get the persisted model."""
        with tentaclio.open(model_url, "rb") as f:
            loaded_model = pickle.load(f)
        return loaded_model

    @classmethod
    def get_model_url(cls, output_dir_url, suffix: Optional[str] = None) -> str:
        """Get the model URL."""
        model_name = cls.get_model_name(suffix)
        return f"{output_dir_url}{model_name}.pickle"

    def persist_model(self, output_dir_url, suffix: Optional[str] = None) -> str:
        """Persist model."""
        url = self.get_model_url(output_dir_url, suffix)
        artifacts.utils.create_folders_if_needed(url)

        with tentaclio.open(url, "wb") as f:
            pickle.dump(self, f)

        return url
