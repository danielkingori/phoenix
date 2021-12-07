"""Module to process the Single Feature to Label Mapping."""
from typing import Union


def convert_to_bool(input_val: Union[str, bool]) -> bool:
    """Converts string representations of booleans into bools. Any non-false string is True."""
    if type(input_val) == bool:
        return input_val

    if input_val.lower() == "false":
        return False
    else:
        return True
