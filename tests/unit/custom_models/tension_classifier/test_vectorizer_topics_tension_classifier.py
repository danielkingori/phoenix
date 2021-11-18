import pandas as pd

from phoenix.custom_models.tension_classifier import vectorizer_topics_tension_classifier


def test_CategoricalColumns_fit():
    df = pd.DataFrame({"topics": ["['foo', 'bar']", "['baz']", "[none]"], "id": [1, 2, 3]})

    actual_categorical_columns = vectorizer_topics_tension_classifier.CategoricalColumns()
    actual_categorical_columns.fit_transform(df)

    assert actual_categorical_columns.columns == ["foo", "bar", "baz", "none"]


def test_CategoricalColumns_transform():
    df_train = pd.DataFrame({"topics": ["['foo', 'bar']", "['baz']", "[none]"], "id": [1, 2, 3]})
    df = pd.DataFrame({"topics": ["['foo']", "['none']"], "id": [1, 2]})
    actual_categorical_columns = vectorizer_topics_tension_classifier.CategoricalColumns()
    actual_categorical_columns.fit(df_train)

    expected_df = pd.DataFrame(
        data=[(1, 0, 0, 0), (0, 0, 0, 1)], columns=["foo", "bar", "baz", "none"]
    )

    output_df = actual_categorical_columns.transform(df)

    pd.testing.assert_frame_equal(expected_df, output_df)
