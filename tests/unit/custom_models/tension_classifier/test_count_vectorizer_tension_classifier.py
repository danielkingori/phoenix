import pytest
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier

from phoenix.custom_models.tension_classifier import count_vectorizer_tension_classifier
from phoenix.tag.text_features_analyser import StemmedCountVectorizer


def test_get_model_name_count_vectorizer():
    assert (
        count_vectorizer_tension_classifier.CountVectorizerTensionClassifier.get_model_name()
        == "count_vectorizer_tension_classifier_model"
    )


def test_get_model_url_count_vectorizer():
    assert (
        count_vectorizer_tension_classifier.CountVectorizerTensionClassifier.get_model_url(
            "file:///"
        )
        == "file:///count_vectorizer_tension_classifier_model.pickle"
    )


def test_get_model_url_count_vectorizer_suffix():
    assert (
        count_vectorizer_tension_classifier.CountVectorizerTensionClassifier.get_model_url(
            "file:///", "suf"
        )
        == "file:///count_vectorizer_tension_classifier_model_suf.pickle"
    )


def test_get_model_name_count_vectorizer_with_suffix():
    assert (
        count_vectorizer_tension_classifier.CountVectorizerTensionClassifier.get_model_name(
            "SEP_21"
        )
        == "count_vectorizer_tension_classifier_model_SEP_21"
    )


def test_count_vectorizer_tension_classifier_analyse_fail():
    class_labels = ["is_economic_labour_tension"]
    classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(class_labels)
    with pytest.raises(ValueError) as exc_info:
        classifier.analyse()
    assert "classifier" in str(exc_info.value)


def test_count_vectorizer_tension_classifier_analyse_data_fail():
    class_labels = ["is_economic_labour_tension"]
    vectorizer = StemmedCountVectorizer()
    forest = RandomForestClassifier(random_state=1)
    multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

    classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(
        class_labels, vectorizer, multi_target_forest
    )

    with pytest.raises(ValueError) as exc_info:
        classifier.analyse()
    assert "dataset" in str(exc_info.value)
