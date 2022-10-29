"""Unit tests for prodigy_utils."""
import pytest

from phoenix.tag.labelling import prodigy_utils


def test_span_to_text_snippets():
    """Test annotation extraction from span_json."""
    test_json = {
        "text": "This is a keyword for a label. These key words are another.",
        "spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    expected_snippets = [
        {"label": "test_label", "text": "keyword"},
        {"label": "test_label", "text": "key words"},
    ]

    actual_snippets = prodigy_utils.span_to_text_snippets(test_json)
    assert [i for i in expected_snippets if i not in actual_snippets] == []


def test_span_to_text_snippets_except_spans():
    """Test that not having a spans key raises an exception."""
    test_json = {
        "text": "This is a keyword for a label. These key words are another.",
        "not_a_spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    with pytest.raises(Exception):
        _ = prodigy_utils.span_to_text_snippets(test_json)


def test_span_to_text_snippets_except_text():
    """Test that not having a text key raises an exception."""
    test_json = {
        "not_a_text": "This is a keyword for a label. These key words are another.",
        "spans": [
            {"start": 10, "end": 17, "label": "test_label"},
            {"start": 37, "end": 46, "label": "test_label"},
        ],
        "extra_key_1": "extra_data_1",
    }

    with pytest.raises(Exception):
        _ = prodigy_utils.span_to_text_snippets(test_json)
