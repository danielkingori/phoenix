"""Utils for dataclasses."""
import dataclasses
import json


def are_equal(dc_1, dc_2):
    """Are dataclasses equal for nested dataclasses."""
    dc_1_dict = dataclasses.asdict(dc_1)
    dc_2_dict = dataclasses.asdict(dc_2)
    return json.dumps(dc_1_dict, sort_keys=True) == json.dumps(dc_2_dict, sort_keys=True)
