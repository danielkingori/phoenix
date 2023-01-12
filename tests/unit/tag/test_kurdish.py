"""Test Kurdish NLP."""

from phoenix.tag import kurdish


def test_sorani_stemmer():
    """Test stemming in Sorani."""
    stemmer = kurdish.SoraniStemmer()
    assert stemmer.stemWord("دەچینەوە") == "چ"
    assert stemmer.stemWord("سهگ") == "سهگ"


def test_kurmanji_stemmer():
    """Test stemming in kurmanji."""
    stemmer = kurdish.KurmanjiStemmer()
    assert stemmer.stemWord("pisîk") == "pisîk"
    assert stemmer.stemWord("xanî") == "xanî"


def test_sorani_preprocess():
    """Test Sorani preprocessing."""
    input_doc = "لە ســـاڵەکانی ١٩٥٠دا"
    output_doc = kurdish.sorani_preprocess(input_doc)
    assert "لە ساڵەکانی 1950دا" == output_doc


def test_kurmanji_preprocess():
    """Test Kurmanji preprocessing."""
    input_doc = "Min nizanibû ku min dizanibû ku min ew nizanibû."
    output_doc = kurdish.kurmanji_preprocess(input_doc)
    assert "Min nizanibû ku min dizanibû ku min ew nizanibû." == output_doc


def test_sorani_tokenize():
    """Test Sorani tokenization."""
    input_doc = "لە ساڵەکانی 1950دا"
    output_tokens = kurdish.sorani_tokenize(input_doc)
    assert ["لە", "ساڵەکانی", "1950دا"] == output_tokens


def test_kurmanji_tokenize():
    """Test Kurmanji tokenization."""
    input_doc = "Min nizanibû ku min dizanibû ku min ew nizanibû."
    output_tokens = kurdish.kurmanji_tokenize(input_doc)
    assert [
        "Min",
        "nizanibû",
        "ku",
        "min",
        "dizanibû",
        "ku",
        "min",
        "ew",
        "nizanibû",
        ".",
    ] == output_tokens
