"""Test run_params utils."""
import pytest

from phoenix.common.run_params import utils


@pytest.mark.parametrize(
    "input_param, expected_output",
    [
        (2021, 2021),
        ("2021", 2021),
        ("2021.432", None),
        ("", None),
        (None, None),
    ],
)
def test_normalise_int(input_param, expected_output):
    """Test normalise_int."""
    assert expected_output == utils.normalise_int(input_param)
