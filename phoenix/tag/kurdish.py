"""Kurdish NLP."""
from typing import List

from klpt.preprocess import Preprocess
from klpt.stem import Stem
from klpt.tokenize import Tokenize


KURDISH_CODE = "ku"

SORANI_CODE = "ckb"
SORANI = "Sorani"
ARABIC = "Arabic"

KURMANJI_CODE = "kmr"
KURMANJI = "Kurmanji"
LATIN = "Latin"


class SoraniStemmer:
    """Sorani stemmer.

    KLPT currently (Nov 2021) only stems Sorani verbs, but better than nothing.
    """

    def __init__(self):
        self.stemmer = Stem(SORANI, ARABIC)

    def stemWord(self, word: str) -> str:
        """Stem word.

        KLPT `stem` returns a list of stem(s). Let's just pick the first.

        If empty list returned, then no stems found, so just use given word in full.
        """
        stems = self.stemmer.stem(word)
        if stems:
            return stems[0]
        else:
            return word


class KurmanjiStemmer:
    """Kurmanji stemmer.

    KLPT currently (Nov 2021) only stems Sorani verbs, so this is just pass through functionality.
    """

    def __init__(self):
        pass

    def stemWord(self, word: str) -> str:
        """Pass through."""
        return word


sorani_preprocessor = Preprocess(SORANI, ARABIC, numeral=LATIN)

sorani_stopwords = sorani_preprocessor.stopwords


def sorani_preprocess(doc: str) -> str:
    """Preprocess Sorani text."""
    return sorani_preprocessor.preprocess(doc)


kurmanji_preprocessor = Preprocess(KURMANJI, LATIN, numeral=LATIN)

kurmanji_stopwords = kurmanji_preprocessor.stopwords


def kurmanji_preprocess(doc: str) -> str:
    """Preprocess Kurmanji text."""
    return kurmanji_preprocessor.preprocess(doc)


sorani_tokenizer = Tokenize(SORANI, ARABIC)


def sorani_tokenize(doc: str) -> List[str]:
    """Tokenize Sorani text.

    Should be given document which has been preprocessed.
    """
    return sorani_tokenizer.word_tokenize(doc, separator="", mwe_separator=" ")


kurmanji_tokenizer = Tokenize(KURMANJI, LATIN)


def kurmanji_tokenize(doc: str) -> List[str]:
    """Tokenize Kurmanji text.

    Should be given document which has been preprocessed.
    """
    return kurmanji_tokenizer.word_tokenize(doc, separator="", mwe_separator=" ")
