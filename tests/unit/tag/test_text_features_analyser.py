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

    en_vectorizer = tfa.StemmedCountVectorizer(
        stemmer=en_stemmer, stop_words=stopwords.words("english")
    )
    en_vectorizer.fit_transform(en_corpus)
    expected_feature_names = ["succeed", "stem", "remov", "end", "word"]
    assert set(en_vectorizer.get_feature_names()) == set(expected_feature_names)
    assert isinstance(en_vectorizer, CountVectorizer)


def test_StemmedCountVectorizer_numbers():
    """Test if StemmedCountVectorizer can handle only numbers."""
    en_stemmer = stemmer("english")

    en_corpus = [15.0, 12, 13]

    en_vectorizer = tfa.StemmedCountVectorizer(
        stemmer=en_stemmer, stop_words=stopwords.words("english")
    )
    en_vectorizer.fit_transform(en_corpus)
    expected_feature_names = ["12", "13", "15"]
    assert set(en_vectorizer.get_feature_names()) == set(expected_feature_names)
    assert isinstance(en_vectorizer, CountVectorizer)


def test_StemmedCountVectorizer_ar():
    """Test if StemmedCountVectorizer is initialized with the right stemmer."""
    ar_stemmer = stemmer("arabic")

    ar_vectorizer = tfa.StemmedCountVectorizer(
        stemmer=ar_stemmer, stop_words=stopwords.words("arabic")
    )
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
    en_vectorizer = tfa.StemmedCountVectorizer(
        stemmer=en_stemmer, stop_words=stopwords.words("english")
    )
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


def test_get_stopwords_has_english():
    """Test get_stopwords list and that it contains at least the english stopwords."""
    stopwords_list = stopwords.words("english")

    actual_stopwords_list = tfa.get_stopwords()
    assert set(stopwords_list).issubset(actual_stopwords_list)


def test_get_stopwords_has_arabic():
    """Test get_stopwords list and that it contains at least the arabic stopwords."""
    stopwords_list = stopwords.words("arabic")

    actual_stopwords_list = tfa.get_stopwords()
    assert set(stopwords_list).issubset(actual_stopwords_list)


def test_TextFeaturesAnalyser_default_accepted_languages():
    """Test the whitelisted languages from tag.language are accepted in the default analyser.

    This will raise an error if the TextFeaturesAnalyser doesn't have the languages in its
    default language list.
    """
    df_test = pd.DataFrame(
        [
            ("1", "this is a sentance", "en"),
            ("1", "some sentance", "ku"),
            ("1", "another sentance", "und"),
            ("1", "yas", "ckb"),
            ("1", "yas sentance", "ar"),
            ("1", "yas sentances", "ar_izi"),
            ("1", "on peut aussi analyser le français", "fr"),
            ("1", "ninaweza kuchambua sentensi", "swa"),
        ],
        columns=["id", "clean_text", "language"],
    )

    text_analyser = tfa.create(parallelisable=False)
    df_test["features"] = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")


@pytest.mark.parametrize("parallelisable", [True, False])
def test_TextFeaturesAnalyser_features(parallelisable):
    df_test = pd.DataFrame(
        [("1", "succeeding in stemming removes the ends of words", "en")],
        columns=["id", "clean_text", "language"],
    )

    text_analyser = tfa.create(parallelisable=parallelisable)
    output_features = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")
    assert isinstance(output_features, pd.Series)
    df_test["features"] = output_features

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

    assert df_test.loc[0, "features"] == expected_3gram_feature_list


def test_TextFeaturesAnalyser_kurdish():
    """Test analysing Kurdish."""
    df_test = pd.DataFrame(
        [
            ("1", "Min nizanibû a ku bin min", "ku"),
            ("1", "لە ســـاڵەکانی ١٩٥٠دا یان", "ckb"),
        ],
        columns=["id", "clean_text", "language"],
    )
    text_analyser = tfa.create(ngram_ranges=[(1, 2)], parallelisable=False)
    df_test["features"] = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")

    kurmanji_feats = ["Min", "nizanibû", "Min nizanibû"]

    sorani_feats = ["ساڵ", "1950", "ساڵ 1950"]

    assert df_test["features"][0] == kurmanji_feats
    assert df_test["features"][1] == sorani_feats


def test_TextFeaturesAnalyser_kiswahili():
    """Test analysing Kiswahili.

    Currently expects no stemming, just tokenization
    """
    df_test = pd.DataFrame(
        [
            ("1", "Ninaweza kuchambua sentensi", "swa"),
        ],
        columns=["id", "clean_text", "language"],
    )
    text_analyser = tfa.create(ngram_ranges=[(1, 2)], parallelisable=True)
    df_test["features"] = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")

    kiswahili_feats = [
        "ninaweza",
        "kuchambua",
        "sentensi",
        "ninaweza kuchambua",
        "kuchambua sentensi",
    ]

    assert df_test["features"][0] == kiswahili_feats


def test_TextFeaturesAnalyser_features_no_ngrams():
    df_test = pd.DataFrame(
        [("1", "succeeding in stemming removes the ends of words", "en")],
        columns=["id", "clean_text", "language"],
    )
    text_analyser = tfa.create(use_ngrams=False)
    text_analyser_non_parallelizable = tfa.create(use_ngrams=False, parallelisable=False)
    df_test["features"] = text_analyser.features(df_test[["clean_text", "language"]], "clean_text")
    df_test["features_no_dask"] = text_analyser_non_parallelizable.features(
        df_test[["clean_text", "language"]], "clean_text"
    )

    expected_feature_list = [
        "succeed",
        "stem",
        "remov",
        "end",
        "word",
    ]

    assert df_test["features"][0] == expected_feature_list
    assert df_test["features_no_dask"][0] == expected_feature_list


def test_TextFeaturesAnalyser_hashtags_arabic():
    """Test that the analyser handles hashtags as part of a word."""
    featurizer = tfa.create()
    arabic_text = " مبادرة #البابا_فرنسيس في يوم #لبنان "
    expected_tokens = ["مبادرة", "#البابا_فرنسيس", "في", "يوم", "#لبنان"]
    actual_tokens = featurizer.dict_countvectorizers["ar"][0].build_tokenizer()(arabic_text)

    assert expected_tokens == actual_tokens


def test_TextFeaturesAnalyser_hashtags_english():
    """Test that the analyser handles hashtags as part of a word."""
    featurizer = tfa.create()
    english_text = "world# the #hello is here"
    expected_tokens = ["world", "the", "#hello", "is", "here"]
    actual_tokens = featurizer.dict_countvectorizers["ar"][0].build_tokenizer()(english_text)

    assert expected_tokens == actual_tokens
