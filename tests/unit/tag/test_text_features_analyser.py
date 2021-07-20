"""Test text_features_analyser."""
import pandas as pd
import pytest
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from snowballstemmer import stemmer

from phoenix.tag import text_features_analyser as tfa


def test_StemmedCountVectorizer_en():
    """Test if StemmedCountVectorizer is initialized with the right english stemmer."""
    en_stemmer = stemmer("english")

    en_corpus = ["succeeding in stemming removes the ends of words"]

    en_vectorizer = tfa.StemmedCountVectorizer(en_stemmer, stop_words=stopwords.words("english"))
    en_vectorizer.fit_transform(en_corpus)
    expected_feature_names = ["succeed", "stem", "remov", "end", "word"]
    assert set(en_vectorizer.get_feature_names()) == set(expected_feature_names)
    assert isinstance(en_vectorizer, CountVectorizer)


def test_StemmedCountVectorizer_ar():
    """Test if StemmedCountVectorizer is initialized with the right stemmer."""
    ar_stemmer = stemmer("arabic")

    ar_vectorizer = tfa.StemmedCountVectorizer(ar_stemmer, stop_words=stopwords.words("arabic"))
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
    en_vectorizer = tfa.StemmedCountVectorizer(en_stemmer, stop_words=stopwords.words("english"))
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


@pytest.mark.skip(
    "bug to be fixed: doesn't stem the first word of a bigram and the first 2 words of "
    "a trigram"
)
def test_TextFeaturesAnalyser_features():
    df_test = pd.DataFrame(
        [("1", "succeeding in stemming removes the ends of words", "en")],
        columns=["id", "clean_text", "language"],
    )
    text_analyser = tfa.create()
    df_test["features"] = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")

    expected_3gram_feature_list = [
        "succeed",
        "stem",
        "remov",
        "end",
        "word",
        "succeed stem",
        "stem remov",
        "remov end",
        "end word",
        "succeed stem remov",
        "stem remov end",
        "remov end word",
    ]

    assert df_test["features"][0] == expected_3gram_feature_list
