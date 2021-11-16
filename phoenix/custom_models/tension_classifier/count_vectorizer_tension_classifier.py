"""Count Vectorizer Tension classifier classifies tensions based on word vectors from text."""
from typing import List, Optional

import numpy as np
import pandas as pd
from sklearn.base import ClassifierMixin
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.multioutput import MultiOutputClassifier
from snowballstemmer import stemmer

from phoenix.custom_models.tension_classifier.tension_classifier import TensionClassifier, logger
from phoenix.tag.text_features_analyser import StemmedCountVectorizer, get_stopwords


class CountVectorizerTensionClassifier(TensionClassifier):
    """Uses a count vectorizer to classify tensions.

    Args:
        count_vectorizer: (StemmedCountVectorizer) count_vectorizer which has been fit
        classifier: The sklearn classifier used to classify tensions
        class_labels: List[str]: list of tension names for the classifier's classes
        X_test: Optional[np.ndarray]: word matrix of the test set. Obtained by running
            count_vectorizer.transform(test_dataset["text"])
        Y_test: Optional[np.ndarray]: true labels of the test set. Is a matrix of shape
            [n, m] where n is number of rows, and m is the len(class_labels)
    """

    def __init__(
        self,
        class_labels: List[str],
        count_vectorizer: Optional[StemmedCountVectorizer] = None,
        classifier: Optional[ClassifierMixin] = None,
        X_test: Optional[np.ndarray] = None,
        Y_test: Optional[np.ndarray] = None,
    ):
        super().__init__(classifier)
        self.count_vectorizer: StemmedCountVectorizer
        if count_vectorizer:
            self.count_vectorizer = count_vectorizer
        self.X_test: np.ndarray
        if X_test:
            self.X_test = X_test
        self.Y_test: np.ndarray
        if Y_test:
            self.Y_test = Y_test
        self.class_labels: List[str] = class_labels

    @staticmethod
    def get_model_name(suffix: Optional[str] = None) -> str:
        """Get the model's name. Used for saving models."""
        model_name = "count_vectorizer_tension_classifier_model"
        if suffix:
            model_name = f"{model_name}_{suffix}"
        return model_name

    def train(
        self,
        training_df: pd.DataFrame,
        test_df: pd.DataFrame,
        random_state_int: int = 2021,
    ) -> None:
        """Train a count_vectorizer based tension classifier model."""
        logger.info("Training StemmedCountVectorizer training dataset.")
        count_vectorizer = StemmedCountVectorizer(stemmer("arabic"), stop_words=get_stopwords())

        X_train = count_vectorizer.fit_transform(training_df["text"])
        self.count_vectorizer = count_vectorizer
        Y_train = training_df[self.class_labels].to_numpy()

        logger.info("Preparing test dataset.")

        self.X_test = count_vectorizer.transform(test_df["text"])
        self.Y_test = test_df[self.class_labels].to_numpy()

        # We're assuming that there are no weird interactions from using the same random_state
        # for train_test_split as the random_state for the classifier.
        forest = RandomForestClassifier(random_state=random_state_int, class_weight="balanced")
        multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

        logger.info("Fitting Multi Output Random Forest Classifier.")
        multi_target_forest.fit(X_train, Y_train)

        logger.info(f"Mean accuracy: {multi_target_forest.score(self.X_test, self.Y_test)}")
        self.classifier = multi_target_forest

    def analyse(self):
        """Analyse the trained model performance."""
        if not hasattr(self, "classifier"):
            raise ValueError("There is no classifier, please run train() first.")

        if not hasattr(self, "X_test") or not hasattr(self, "Y_test"):
            raise ValueError("There is no dataset, please add the dataset or run train() first.")

        Y_hat = self.classifier.predict(self.X_test)
        for i in range(self.Y_test.shape[1]):
            logger.info(self.class_labels[i])
            logger.info(f"\nROC AUC: {roc_auc_score(self.Y_test[:,i],Y_hat[:,i])}\n")
            confusion_matrix = pd.crosstab(
                self.Y_test[:, i],
                Y_hat[:, i],
                rownames=["True"],
                colnames=["Predicted"],
                margins=True,
            )
            logger.info(f"\nconfusion_matrix:\n{confusion_matrix}")
            logger.info(classification_report(self.Y_test[:, i], Y_hat[:, i]))

    def predict(self, df: pd.DataFrame, clean_text_col: str = "text") -> pd.DataFrame:
        """Predict and tag a dataframe based on its text column."""
        logger.info("Starting word vectorization")
        word_vectors = self.count_vectorizer.transform(df[clean_text_col])

        logger.info("Starting classification")
        classifications = self.classifier.predict(word_vectors)
        logger.info(f"Writing the following classifications back: {self.class_labels}")
        df[self.class_labels] = classifications
        return df
