"""Pandas Utils."""
import re


def words_to_snake(name: str):
    """Map Words with spaces to snake case."""
    return re.sub(r"\W+", "_", name.lower())


def camel_to_snake(name):
    """Camel case string to snake case.

    Taken from:
    https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    """
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
