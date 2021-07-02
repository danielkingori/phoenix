"""Test lda."""
import numpy as np
import pandas as pd
import pytest
from mock import MagicMock
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from snowballstemmer import stemmer

from phoenix.common.artifacts import dataframes
from phoenix.visualise_posts import lda


@pytest.fixture()
def expected_df():
    df = pd.DataFrame(
        [("message1", 3, 0.3), ("message2", 2, 0.5)],
        columns=["message", "lda_cloud", "lda_cloud_confidence"],
    )
    return df


def test_remove_links():
    """Test remove_links removes links."""
    input_df = pd.DataFrame(
        [
            ("The website is https://datavaluepeople.com/ It's pretty good"),
            ("The website is https://datavaluepeople.com It's pretty good"),
            (
                "تقرير عن الأحداث التي ترافقت مع الانتخابات السورية ومقابلات مع سوريين شاهدوا الحلقة الكاملة من برنامج طوني خليفة عبر هذا الرابط : https://www.facebook.com/RadioSawtBeirut/videos/1067605747100290/?__cft__[0]=AZXyIV8Fviw995IwAzV8LcdwrmFmsCGcJpRU-bHjoTfWonU-5A2B6UZse6IO4t-OwQNoAxgfa6DOYr6_Fnb9rYBWYziPWXw3mxHXg_15bMVYCStGfh2_a8ogbKqYJOGI9hQSWGiUro1LxFZo-1HG3W-NuPeuYyPAw75bwUMNaaEEBg&__tn__=%2CO%2CP-R"  # noqa
            ),
        ],
        columns=["message"],
    )
    output_df = lda.remove_links(input_df, "message")

    expected_df = pd.DataFrame(
        [
            ("The website is  It's pretty good"),
            ("The website is  It's pretty good"),
            (
                "تقرير عن الأحداث التي ترافقت مع الانتخابات السورية ومقابلات مع سوريين شاهدوا الحلقة الكاملة من برنامج طوني خليفة عبر هذا الرابط : "  # noqa
            ),
        ],
        columns=["message"],
    )

    pd.testing.assert_frame_equal(expected_df, output_df)


def test_StemmedCountVectorizer_stemming():
    """Test if StemmedCountVectorizer is initialized with the right stemmer."""
    en_stemmer = stemmer("english")
    ar_stemmer = stemmer("arabic")

    en_corpus = ["succeeding in stemming removes the ends of words"]

    en_vectorizer = lda.StemmedCountVectorizer(en_stemmer, stop_words=stopwords.words("english"))
    en_vectorizer.fit_transform(en_corpus)
    expected_feature_names = ["succeed", "stem", "remov", "end", "word"]
    assert set(en_vectorizer.get_feature_names()) == set(expected_feature_names)
    assert isinstance(en_vectorizer, CountVectorizer)

    ar_vectorizer = lda.StemmedCountVectorizer(ar_stemmer, stop_words=stopwords.words("arabic"))
    ar_corpus = [
        "تقرير عن الأحداث التي ترافقت مع الانتخابات السورية ومقابلات مع سوريين شاهدوا الحلقة الكاملة من برنامج طوني خليفة عبر هذا الرابط"  # noqa
    ]

    ar_vectorizer.fit_transform(ar_corpus)
    expected_ar_feature_names = [
        "احداث",
        "انتخاب",
        "ترافق",
        "تقرير",
        "حلق",
        "خليف",
        "رابط",
        "رنامج",
        "سور",
        "سوري",
        "شاهد",
        "طون",
        "عبر",
        "كامل",
        "مقابلا",
    ]

    assert set(ar_vectorizer.get_feature_names()) == set(expected_ar_feature_names)
    assert isinstance(ar_vectorizer, CountVectorizer)


def test_StemmedCountVectorizer_common_words():
    en_stemmer = stemmer("english")
    en_corpus = ["succeeding in stemming removes the ends of words", "words words words"]
    en_vectorizer = lda.StemmedCountVectorizer(en_stemmer, stop_words=stopwords.words("english"))
    en_matrix = en_vectorizer.fit_transform(en_corpus)
    actual_word_dict = en_vectorizer.get_most_common_words(en_matrix)
    expected_word_dict = {
        "succeed": 1,
        "stem": 1,
        "remov": 1,
        "end": 1,
        "word": 4,
    }

    assert actual_word_dict == expected_word_dict


def test_get_stopwords():
    """Test get_stopwords list and that it contains at least the english and arabic stopwords."""
    stopwords_list = stopwords.words("arabic")
    stopwords_list.extend(stopwords.words("english"))

    actual_stopwords_list = lda.get_stopwords()
    assert set(stopwords_list).issubset(actual_stopwords_list)


def test_write_cloud_results(expected_df):
    model = MagicMock()
    model.transform.return_value = np.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.3]])

    input_matrix = ["hi"]

    input_df = pd.DataFrame(["message1", "message2"], columns=["message"])
    expected_df = pd.DataFrame(
        [("message1", 3, 0.3), ("message2", 2, 0.5)],
        columns=["message", "lda_cloud", "lda_cloud_confidence"],
    )

    actual_df = lda.write_cloud_results(input_df, input_matrix, model)

    pd.testing.assert_frame_equal(actual_df, expected_df)
    model.transform.assert_called_with(input_matrix)


def test_persist(tmpdir, expected_df):
    dir_url = "file:" + str(tmpdir) + "/"
    lda.persist(dir_url, expected_df)
    pd.testing.assert_frame_equal(
        dataframes.get(dir_url + "lda.parquet").dataframe,
        expected_df,
    )
