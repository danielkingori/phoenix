"""Utils for custom models."""

import numpy as np
import pandas as pd


def explode_str(df: pd.DataFrame, col: str, sep: str):
    """Explodes columns that have strings separated by a `sep` into new rows.

    Args:
        df: (pd.DataFrame) dataframe in which you want to explode a column
        col: (str) column name that you want to explode
        sep: (str) separator that signifies it needs to be exploded
    """
    s = df[col]
    i = np.arange(len(s)).repeat(s.str.count(sep) + 1)
    df = df.iloc[i].assign(**{col: sep.join(s).split(sep)})
    df[col] = df[col].str.lstrip()
    return df


def get_incorrectly_tagged_classes(classifier, test_df):
    """Get incorrectly tagged classes from a trained classifier.

    Args:
        classifier: (TensionClassifier): also expects the test classes to be packaged with it
        test_df: pd.DataFrame: test dataframe

    Still WIP, expects the classifier object to have data in there as well.
    """
    cv_tension_classifier = classifier
    Y_hat = cv_tension_classifier.classifier.predict(cv_tension_classifier.X_test)
    Y_hat_proba = cv_tension_classifier.classifier.predict_proba(cv_tension_classifier.X_test)

    tensions_tf = []
    for i in range(Y_hat.shape[1]):
        tensions_tf.append(cv_tension_classifier.class_labels[i] + "_true")
        tensions_tf.append(cv_tension_classifier.class_labels[i] + "_false")

    confusion_df = pd.DataFrame(columns=["text"] + tensions_tf)

    k = 0
    for i in range(Y_hat.shape[1]):
        print(cv_tension_classifier.class_labels[i])
        for j in range(Y_hat.shape[0]):
            if Y_hat[j, i] != cv_tension_classifier.Y_test[j, i]:
                print(Y_hat_proba[i][j, :])
                text = test_df.iloc[j]["text"]
                label_true = f"{cv_tension_classifier.class_labels[i]}_true"
                label_false = f"{cv_tension_classifier.class_labels[i]}_false"
                d = {
                    "text": text,
                    label_true: Y_hat_proba[i][j, 0],
                    label_false: Y_hat_proba[i][j, 1],
                }
                confusion_df.loc[k] = pd.Series(d)
                k += 1

    confusion_df.to_csv("confusion_df.csv")
