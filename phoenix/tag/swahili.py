"""Swahili NLP."""

from phoenix.tag.swahili_stemmer import Stemmer

SWAHILI_CODE = "sw"

class SwahiliStemmer:
    """Swahili stemmer."""

    def __init__(self):
        self.stemmer = Stemmer()

    def stemWord(self, word: str) -> str:
        """Stem word."""
        stem = self.stemmer.stem(word)
        return stem
