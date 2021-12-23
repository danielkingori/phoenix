"""RunParams utils."""
from typing import Union


TRUTHY_STR_VALUES = ["t", "true"]
FALSEY_STR_VALUES = ["f", "false"]


def normalise_bool(to_normalise: Union[bool, str, None]) -> bool:
    """Normalise the parameters from a notebook into a bool."""
    if type(to_normalise) is str:
        return string_to_bool(to_normalise)
    return bool(to_normalise)


def string_to_bool(string_var: str) -> bool:
    """Normalise a string to a bool.

    Checks if the string has a bool like value.
    """
    if string_var.lower() in TRUTHY_STR_VALUES:
        return True

    if string_var.lower() in FALSEY_STR_VALUES:
        return False

    return bool(string_var)


def normalise_int(to_normalise: Union[int, str, None]) -> int:
    """Normalise the parameters from a notebook into a int."""
    if type(to_normalise) is int:
        return to_normalise

    if type(to_normalise) is str and to_normalise.isdigit():
        return int(to_normalise)

    return 0
