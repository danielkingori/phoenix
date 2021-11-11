"""Test of the dataclasses utils."""
import dataclasses

import pytest

from phoenix.common import dataclasses_utils


@dataclasses.dataclass
class DataclassOne:
    """DataclassOne."""

    id: str
    param_1: int


@dataclasses.dataclass
class DataclassTwo:
    """DataclassTwo."""

    id: str
    param_2: int


@pytest.mark.parametrize(
    "d1,d2",
    [
        (
            DataclassOne("id_1", 1),
            DataclassOne("id_1", 1),
        ),
        (
            DataclassOne(id="id_2", param_1=1),
            DataclassOne(id="id_2", param_1=1),
        ),
        (
            DataclassTwo("id_1", 1),
            DataclassTwo("id_1", 1),
        ),
        (
            DataclassTwo(id="id_2", param_2=1),
            DataclassTwo(id="id_2", param_2=1),
        ),
    ],
)
def test_equal(d1, d2):
    """Test that the dataclasses utils works for equal dataclasses."""
    assert dataclasses_utils.are_equal(d1, d2)


@pytest.mark.parametrize(
    "d1,d2",
    [
        (
            DataclassOne("id_1", 1),
            DataclassOne("id_2", 1),
        ),
        (
            DataclassOne("id_1", 1),
            DataclassOne("id_1", 2),
        ),
        (
            DataclassOne(id="id_1", param_1=1),
            DataclassOne(id="id_2", param_1=1),
        ),
        (
            DataclassOne(id="id_2", param_1=1),
            DataclassOne(id="id_2", param_1=2),
        ),
        (
            DataclassTwo("id_1", 1),
            DataclassTwo("id_1", 2),
        ),
        (
            DataclassTwo("id_2", 1),
            DataclassTwo("id_1", 1),
        ),
        (
            DataclassTwo(id="id_2", param_2=2),
            DataclassTwo(id="id_2", param_2=1),
        ),
        (
            DataclassTwo(id="id_2", param_2=1),
            DataclassTwo(id="id_1", param_2=1),
        ),
    ],
)
def test_n_equal(d1, d2):
    """Test that the dataclasses utils works for non equal dataclasses."""
    assert not dataclasses_utils.are_equal(d1, d2)
