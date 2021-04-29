"""Test tag."""
import mock
import pandas as pd

from phoenix.tag import tag


@mock.patch("phoenix.tag.language.execute")
def test_tag(m_language_execute):
    """Test tag calls correct sub modules."""
    message_key = "m"
    messages = pd.DataFrame({message_key: ["m1", "m2"]})
    language_ser = pd.Series([["l1", 1.0], ["l2", 1.0]])
    m_language_execute.return_value = language_ser
    r_value = tag.tag_dataframe(messages, message_key)
    m_args, kwargs = m_language_execute.call_args
    assert len(m_args) == 1
    pd.testing.assert_series_equal(m_args[0], messages[message_key])
    e_value = messages.copy()
    e_value[["language", "confidence"]] = language_ser
    pd.testing.assert_frame_equal(
        r_value,
        e_value,
    )
