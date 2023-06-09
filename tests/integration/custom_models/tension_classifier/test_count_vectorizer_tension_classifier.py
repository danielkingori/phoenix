"""Integration tests for tension_classifier."""

import mock
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier

from phoenix.custom_models.tension_classifier import count_vectorizer_tension_classifier
from phoenix.tag.text_features_analyser import StemmedCountVectorizer


def test_persist_model(tmpdir):
    class_labels = ["is_economic_labour_tension"]
    vectorizer = StemmedCountVectorizer()
    forest = RandomForestClassifier(random_state=1)
    multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

    classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(
        class_labels, vectorizer, multi_target_forest
    )

    dir_url = "file:" + str(tmpdir) + "/"
    url = classifier.persist_model(dir_url)
    assert classifier.get_model_url(dir_url) == url
    loaded_model = classifier.get_model(url)

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

    cv_tension_classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(
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


def test_CountVectorizerTensionClassifier_train():
    """Simple integration test that model gets trained and assigns the expected class variables."""
    input_training_df = pd.DataFrame(
        {
            "text": ["hello", "world", "foo", "bar", "baz", "xyz"] * 10,
            "is_economic_labour_tension": [1, 0, 1, 0, 1, 0] * 10,
            "is_sectarian_tension": [1, 1, 1, 1, 1, 1] * 10,
            "is_environmental_tension": [1, 1, 1, 1, 1, 1] * 10,
            "is_political_tension": [0, 0, 0, 0, 0, 0] * 10,
        }
    )

    tensions_with_enough_labels = [
        "is_economic_labour_tension",
        "is_sectarian_tension",
        "is_environmental_tension",
    ]

    input_test_df = pd.DataFrame(
        {
            "text": ["hello", "world", "foo", "bar", "baz", "xyz"] * 3,
            "is_economic_labour_tension": [1, 0, 1, 0, 1, 0] * 3,
            "is_sectarian_tension": [1, 1, 1, 1, 1, 1] * 3,
            "is_environmental_tension": [1, 1, 1, 1, 1, 1] * 3,
            "is_political_tension": [0, 0, 0, 0, 0, 0] * 3,
        }
    )

    count_vectorizer_classifier = (
        count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(
            class_labels=tensions_with_enough_labels
        )
    )

    count_vectorizer_classifier.train(input_training_df, input_test_df)

    # Test that there are 18 rows with 6 unique words in the test set matrix
    assert count_vectorizer_classifier.X_test.shape == (18, 6)
    # Test that there are exactly equal labels compared to the class_labels inputted
    assert count_vectorizer_classifier.Y_test.shape == (18, len(tensions_with_enough_labels))
    assert count_vectorizer_classifier.count_vectorizer is not None
    assert count_vectorizer_classifier.classifier is not None
