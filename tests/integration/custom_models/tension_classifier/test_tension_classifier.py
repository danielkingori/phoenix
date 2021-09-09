"""Integration tests for tension_classifier."""

import pickle

import mock
import pandas as pd
import tentaclio
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier

from phoenix.custom_models.tension_classifier import tension_classifier
from phoenix.tag.text_features_analyser import StemmedCountVectorizer


def test_persist_model(tmpdir):
    class_labels = ["is_economic_labour_tension"]
    vectorizer = StemmedCountVectorizer()
    forest = RandomForestClassifier(random_state=1)
    multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

    classifier = tension_classifier.CountVectorizerTensionClassifier(
        class_labels, vectorizer, multi_target_forest
    )

    dir_url = "file:" + str(tmpdir) + "/"
    classifier.persist_model(dir_url)
    with tentaclio.open(f"{dir_url}{classifier.get_model_name()}.pickle", "rb") as f:
        loaded_model = pickle.load(f)

    assert loaded_model.class_labels == class_labels
    # incomplete check as the objects are different and simple equality checks `==` fail
    assert loaded_model.count_vectorizer.__class__ == vectorizer.__class__
    assert loaded_model.classifier.__class__ == multi_target_forest.__class__


def test_predict():
    class_labels = ["is_economic_labour_tension", "is_sectarian_tension"]
    mock_vectorizer = mock.MagicMock()
    mock_vectorizer.transform.return_value = "returned_matrix"

    mock_classifier = mock.MagicMock()
    mock_classifier.predict.return_value = [[0, 1]]

    cv_tension_classifier = tension_classifier.CountVectorizerTensionClassifier(
        class_labels, mock_vectorizer, mock_classifier
    )
    # This tests that the columns in `class_labels` - the ones that we have a prediction for are
    # being overwritten by the predicted classifications. Any other tensions should not be
    # overwritten
    expected_df = pd.DataFrame(
        [("input_text", 0, 1, 0, 1)],
        columns=[
            "text",
            "is_economic_labour_tension",
            "is_sectarian_tension",
            "another_tension",
            "yet_another_tension",
        ],
    )

    input_df = pd.DataFrame(
        [("input_text", 1, 0, 0, 1)],
        columns=[
            "text",
            "is_economic_labour_tension",
            "is_sectarian_tension",
            "another_tension",
            "yet_another_tension",
        ],
    )

    output_df = cv_tension_classifier.predict(input_df, "text")

    pd.testing.assert_frame_equal(output_df, expected_df)
    mock_vectorizer.transform.assert_called()
    mock_classifier.predict.assert_called_with("returned_matrix")
