"""Unit tests for tension classifier."""
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier

from phoenix.custom_models.tension_classifier import (
    count_vectorizer_tension_classifier,
    tension_classifier,
)
from phoenix.tag.text_features_analyser import StemmedCountVectorizer


def test_count_vectorizer_tension_classifier_init():
    class_labels = ["is_economic_labour_tension"]
    vectorizer = StemmedCountVectorizer()
    forest = RandomForestClassifier(random_state=1)
    multi_target_forest = MultiOutputClassifier(forest, n_jobs=-1)

    classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(
        class_labels, vectorizer, multi_target_forest
    )

    assert classifier.class_labels == class_labels
    assert classifier.count_vectorizer == vectorizer
    assert classifier.classifier == multi_target_forest


def test_count_vectorizer_tension_classifier_init_class_label_only():
    class_labels = ["is_economic_labour_tension"]
    classifier = count_vectorizer_tension_classifier.CountVectorizerTensionClassifier(class_labels)
    assert classifier.class_labels == class_labels


def test_get_model_name():
    assert tension_classifier.TensionClassifier.get_model_name() == "tension_classifier_model"


def test_get_model_url():
    assert (
        tension_classifier.TensionClassifier.get_model_url("file:///")
        == "file:///tension_classifier_model.pickle"
    )


def test_get_model_url_suffix():
    assert (
        tension_classifier.TensionClassifier.get_model_url("file:///", "suf")
        == "file:///tension_classifier_model_suf.pickle"
    )


def test_get_model_name_with_suffix():
    assert (
        tension_classifier.TensionClassifier.get_model_name("SEP_21")
        == "tension_classifier_model_SEP_21"
    )
