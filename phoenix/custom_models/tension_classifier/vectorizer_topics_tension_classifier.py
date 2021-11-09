"""Vectorizer Topics Tension classifier classifies tensions based on topics and words in text."""
from typing import List, Optional

import random

import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.multioutput import MultiOutputClassifier
from sklearn.pipeline import FeatureUnion, Pipeline

from phoenix.custom_models.tension_classifier.tension_classifier import TensionClassifier, logger
from phoenix.tag.text_features_analyser import StemmedCountVectorizer, get_stopwords


class CategoricalColumns(BaseEstimator, TransformerMixin):
    """CategoricalColumns is a One Hot Encoder transformer for the topics column."""

    def fit(self, df, y=None):
        """Fit model to the data."""
        topics = self._to_array(df["topics"])
        columns = topics.explode().unique()
        self.columns = [c.strip() for c in columns]
        return self

    def transform(self, df):
        """Transform data to trained model."""
        out = {}
        for col in self.columns:
            out[col] = df["topics"].map(str).str.contains(col).astype(int)
        return pd.DataFrame(out)

    def _to_array(self, series):
        """Clean up a series with a str(list) to get an array."""
        remove = ["[", "]", "'"]
        for char in remove:
            series = series.str.replace(char, "", regex=False)
        return series.str.split(",")


class VectorizerTopicsTensionClassifier(TensionClassifier):
    """VectorizerTopicsTensionClassifier uses word vectors and topics to classify tensions."""

    def __init__(
        self,
        class_labels: List[str],
        classifier: Optional[ClassifierMixin] = None,
    ):
        super().__init__(classifier)
        self.class_labels: List[str] = class_labels
        self.expected_input_cols: List[str] = ["text", "topics"]
        self.expected_cols = self.expected_input_cols + self.class_labels

    @staticmethod
    def get_model_name(suffix: Optional[str] = None) -> str:
        """Get the model's name. Used for saving models."""
        model_name = "count_vectorizer_topics_tension_classifier_model"
        if suffix:
            model_name = f"{model_name}_{suffix}"
        return model_name

    def check_expected_train_cols(self, df: pd.DataFrame) -> bool:
        """Checks that the train df has the correct columns."""
        for col in self.expected_cols:
            if col not in df.columns:
                raise ValueError(f"Expected column {col} in the dataframe, but not found.")
        return True

    def check_expected_prediction_cols(self, df: pd.DataFrame) -> bool:
        """Checks that the predict df has the correct columns."""
        for col in self.expected_input_cols:
            if col not in df.columns:
                raise ValueError(f"Expected column {col} in the dataframe, but not found.")
        return True

    def train(
        self,
        df: pd.DataFrame,
        random_state_int: int = random.randint(0, 999999),
    ) -> None:
        """Train a model that uses word vectors and topics to classify tensions."""
        self.check_expected_train_cols(df)
        df = df[self.expected_cols]
        logger.info("Training VectorizerTopics Classifier")
        count_vectorizer = StemmedCountVectorizer(stop_words=get_stopwords())
        forest = RandomForestClassifier(random_state=random_state_int, class_weight="balanced")
        multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

        model = Pipeline(
            [
                (
                    "combine_word_vec_and_topics",
                    FeatureUnion(
                        [
                            (
                                "column_transformer_text",
                                ColumnTransformer(
                                    [("word_vectorizer", count_vectorizer, "text")],
                                ),
                            ),
                            ("categories", CategoricalColumns()),
                        ]
                    ),
                ),
                ("random_forest", multi_target_forest),
            ]
        )
        model.fit(df.drop(self.class_labels, axis=1), df[self.class_labels].values)
        logger.info("Finished training VectorizerTopics Tension Classifier")
        self.classifier = model

    def analyse(self, test_df: pd.DataFrame):
        """Analyse the trained model performance."""
        if not hasattr(self, "classifier"):
            raise ValueError("There is no classifier, please run train() first.")
        self.check_expected_train_cols(test_df)
        test_df = test_df[self.expected_cols]
        Y_hat = self.classifier.predict(test_df.drop(self.class_labels, axis=1))
        Y_test = test_df[self.class_labels].values
        for i in range(Y_test.shape[1]):
            logger.info(self.class_labels[i])
            logger.info(f"\nROC AUC: {roc_auc_score(Y_test[:,i],Y_hat[:,i])}\n")
            confusion_matrix = pd.crosstab(
                Y_test[:, i],
                Y_hat[:, i],
                rownames=["True"],
                colnames=["Predicted"],
                margins=True,
            )
            logger.info(f"\nconfusion_matrix:\n{confusion_matrix}")
            logger.info(classification_report(Y_test[:, i], Y_hat[:, i]))

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Predict and tag a dataframe based on its text column."""
        self.check_expected_prediction_cols(df)
        logger.info("Starting classification")
        classifications = self.classifier.predict(df[self.expected_input_cols])
        logger.info(f"Writing the following classifications back: {self.class_labels}")
        df[self.class_labels] = classifications
        return df
