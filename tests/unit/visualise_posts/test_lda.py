"""Test lda."""

import pandas as pd
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
from snowballstemmer import stemmer

from phoenix.visualise_posts import lda


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


def test_StemmedCountVectorizer():
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