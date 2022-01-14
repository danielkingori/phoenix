"""Test for_object_type and topics_for_object_type."""
import pandas as pd
import pytest

from phoenix.tag import finalise


def test_for_objcet_type_not_supported():
    """Test that error when unsupported object type."""
    object_type = "not_supported_type"
    empty_df = pd.DataFrame({})
    with pytest.raises(RuntimeError) as e:
        finalise.for_object_type(object_type, empty_df)
        assert object_type in str(e.value)


def test_topics_for_objcet_type_not_supported():
    """Test that error when unsupported object type."""
    object_type = "not_supported_type"
    empty_df = pd.DataFrame({})
    with pytest.raises(RuntimeError) as e:
        finalise.topics_for_object_type(object_type, empty_df, empty_df)
        assert object_type in str(e.value)
