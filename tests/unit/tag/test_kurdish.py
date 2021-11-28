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
