"""Kurdish NLP."""
from klpt.stem import Stem


SORANI_CODE = "ckb"
SORANI = "Sorani"
ARABIC = "Arabic"

KURMANJI_CODE = "ku"
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
